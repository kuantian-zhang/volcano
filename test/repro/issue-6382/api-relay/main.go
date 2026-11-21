/*
Copyright 2026 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// api-relay transparently forwards a local scheduler's Kubernetes requests.
// Its sole fault injection delays one target Pod GET response after the real
// API server has returned it. Watches and all other requests remain live.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type podView struct {
	Metadata struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		UID             string `json:"uid"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Spec struct {
		SchedulingGates []struct {
			Name string `json:"name"`
		} `json:"schedulingGates"`
		NodeName string `json:"nodeName"`
	} `json:"spec"`
}

type relay struct {
	mu             sync.Mutex
	logMu          sync.Mutex
	namespace, pod string
	armed, held    bool
	release        chan struct{}
	last           map[string]any
	trace          *json.Encoder
}

func (r *relay) record(event string, pod *podView) {
	row := map[string]any{"event": event, "time": time.Now().UTC().Format(time.RFC3339Nano)}
	if pod != nil {
		row["uid"] = pod.Metadata.UID
		row["resourceVersion"] = pod.Metadata.ResourceVersion
		row["gates"] = pod.Spec.SchedulingGates
		row["nodeName"] = pod.Spec.NodeName
	}
	r.logMu.Lock()
	defer r.logMu.Unlock()
	if err := r.trace.Encode(row); err != nil {
		log.Printf("write trace: %v", err)
	}
}

func (r *relay) modify(response *http.Response) error {
	wantPath := "/api/v1/namespaces/" + r.namespace + "/pods/" + r.pod
	if response.Request.Method != http.MethodGet || response.Request.URL.Path != wantPath || response.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	var pod podView
	if err := json.Unmarshal(body, &pod); err != nil {
		return err
	}
	r.record("get_response", &pod)
	r.mu.Lock()
	if !r.armed || r.held || len(pod.Spec.SchedulingGates) == 0 {
		r.mu.Unlock()
		return nil
	}
	r.armed = false
	r.held = true
	r.last = map[string]any{"uid": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion, "gates": pod.Spec.SchedulingGates}
	release := r.release
	r.mu.Unlock()
	r.record("get_captured", &pod)
	select {
	case <-release:
		r.record("get_released", &pod)
		return nil
	case <-response.Request.Context().Done():
		r.record("get_cancelled", &pod)
		return response.Request.Context().Err()
	}
}

func (r *relay) control(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.mu.Lock()
	defer r.mu.Unlock()
	switch request.URL.Path {
	case "/arm":
		if request.Method != http.MethodPost || r.held {
			http.Error(w, "arm requires POST and no held GET", http.StatusConflict)
			return
		}
		r.armed = true
	case "/release":
		if request.Method != http.MethodPost || !r.held {
			http.Error(w, "release requires POST and a held GET", http.StatusConflict)
			return
		}
		select {
		case <-r.release:
		default:
			close(r.release)
		}
	case "/status":
	default:
		http.NotFound(w, request)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"armed": r.armed, "held": r.held, "pod": r.last})
}

func loopbackListener(address string) net.Listener {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		log.Fatal("listen addresses must use an explicit loopback IP")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	return listener
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "admin kubeconfig for the disposable kind cluster")
	contextName := flag.String("context", "", "explicit kind context")
	listen := flag.String("listen", "127.0.0.1:0", "scheduler API listener")
	control := flag.String("control", "127.0.0.1:0", "local diagnostic control listener")
	namespace := flag.String("namespace", "", "only namespace eligible for a delayed GET")
	pod := flag.String("pod", "worker", "only Pod eligible for a delayed GET")
	tracePath := flag.String("trace", "", "JSONL diagnostic trace")
	flag.Parse()
	if !strings.HasPrefix(*contextName, "kind-") || *kubeconfig == "" || *namespace == "" || *tracePath == "" {
		log.Fatal("kubeconfig, explicit kind context, namespace and trace are required")
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: *contextName}).ClientConfig()
	if err != nil {
		log.Fatal(err)
	}
	transport, err := rest.TransportFor(config)
	if err != nil {
		log.Fatal(err)
	}
	upstream, err := url.Parse(config.Host)
	if err != nil {
		log.Fatal(err)
	}
	trace, err := os.OpenFile(*tracePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer trace.Close()
	r := &relay{namespace: *namespace, pod: *pod, release: make(chan struct{}), trace: json.NewEncoder(trace)}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	director := proxy.Director
	proxy.Director = func(request *http.Request) {
		director(request)
		// client-go accepts JSON as well as protobuf. Keep the diagnostic
		// target GET readable without changing the Kubernetes object.
		request.Header.Set("Accept", "application/json")
	}
	proxy.Transport = transport
	proxy.ModifyResponse = r.modify
	proxy.FlushInterval = -1
	apiListener, controlListener := loopbackListener(*listen), loopbackListener(*control)
	ready := map[string]string{"api": "http://" + apiListener.Addr().String(), "control": "http://" + controlListener.Addr().String()}
	if err := json.NewEncoder(os.Stdout).Encode(ready); err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(http.Serve(controlListener, http.HandlerFunc(r.control))) }()
	if err := http.Serve(apiListener, proxy); err != nil {
		log.Fatal(fmt.Errorf("API relay: %w", err))
	}
}
