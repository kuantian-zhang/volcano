#!/usr/bin/env python3
# Copyright 2026 The Volcano Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise syncTask against a real disposable kind API, without scheduler hooks."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request


def wait_for(description, check, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {description}")


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout


class Run:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.kubectl = ["kubectl", "--kubeconfig", str(Path(args.kubeconfig).resolve()), "--context", args.context]
        self.trace = (self.output / "operations.jsonl").open("x")
        self.children = []
        self.snapshot_count = 0

    def event(self, event, **data):
        self.trace.write(json.dumps({"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "event": event, **data}) + "\n")
        self.trace.flush()

    def kub(self, *args, **kwargs):
        return command(self.kubectl + list(args), **kwargs)

    def get(self, *args):
        return json.loads(self.kub("get", *args, "-o", "json"))

    def apply(self, obj, destination):
        destination.write_text(json.dumps(obj, indent=2) + "\n")
        self.kub("create", "-f", str(destination))
        self.event("created", kind=obj["kind"], name=obj["metadata"]["name"])

    def control(self, base, path, method="GET"):
        request = urllib.request.Request(base + path, data=b"" if method == "POST" else None, method=method)
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)

    def start(self, argv, stdout_path, stderr_path, env=None):
        stdout, stderr = stdout_path.open("x"), stderr_path.open("x")
        proc = subprocess.Popen(argv, stdout=stdout, stderr=stderr, env=env)
        self.children.append((proc, stdout, stderr))
        return proc

    def snapshot(self, proc, folder, uid):
        # Snapshot names have second granularity. Wait for a changed mtime and
        # retry incomplete writes instead of reusing an earlier observation.
        prior = {p: p.stat().st_mtime_ns for p in folder.glob("snapshot-*.json")}
        proc.send_signal(signal.SIGUSR1)

        def read():
            if proc.poll() is not None:
                raise RuntimeError(f"scheduler exited: {proc.returncode}")
            for path in sorted(folder.glob("snapshot-*.json"), key=lambda p: p.stat().st_mtime_ns, reverse=True):
                if path.stat().st_mtime_ns == prior.get(path):
                    continue
                try:
                    documents = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
                except (json.JSONDecodeError, OSError):
                    continue
                for document in documents:
                    if not isinstance(document, dict):
                        continue
                    for job in document.values():
                        if isinstance(job, dict) and "Tasks" in job and uid in (job.get("Tasks") or {}):
                            task = job["Tasks"][uid]
                            pod = task["Pod"]
                            observations = folder.parent / "observations"
                            observations.mkdir(exist_ok=True)
                            self.snapshot_count += 1
                            (observations / f"snapshot-{self.snapshot_count:04d}.json").write_bytes(path.read_bytes())
                            return {"uid": pod["metadata"]["uid"], "resourceVersion": pod["metadata"]["resourceVersion"],
                                    "gates": pod["spec"].get("schedulingGates", []), "SchGated": task["SchGated"],
                                    "taskStatus": task["Status"], "nodeName": task.get("NodeName", "")}
            return None

        return wait_for("target task in a new scheduler cache snapshot", read, 10)

    def run_case(self, name, binary):
        case = self.output / name
        case.mkdir()
        cache_dir = case / "cache"
        cache_dir.mkdir()
        ns = f"volcano-6382-{name}-{self.args.run_id}"
        scheduler_name = f"volcano-6382-{name}-{self.args.run_id}"
        resource = ns
        label = "repro.volcano.sh/" + ns
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}}, case / "namespace.json")
        self.apply({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": resource},
                    "provisioner": "kubernetes.io/no-provisioner", "volumeBindingMode": "WaitForFirstConsumer"}, case / "storageclass.json")
        self.apply({"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": resource},
                    "spec": {"capacity": {"storage": "64Mi"}, "accessModes": ["ReadWriteOnce"],
                             "storageClassName": resource, "persistentVolumeReclaimPolicy": "Retain",
                             "hostPath": {"path": "/tmp/" + resource, "type": "DirectoryOrCreate"}}}, case / "pv.json")
        self.apply({"apiVersion": "scheduling.volcano.sh/v1beta1", "kind": "PodGroup", "metadata": {"name": "workers", "namespace": ns},
                    "spec": {"queue": "default", "minMember": 1, "minResources": {"cpu": "100m", "memory": "32Mi"}}}, case / "podgroup.json")

        ready_file = case / "relay-ready.json"
        relay = self.start([self.args.relay, "--kubeconfig", self.args.kubeconfig, "--context", self.args.context,
                            "--namespace", ns, "--pod", "worker", "--trace", str(case / "relay.jsonl")], ready_file, case / "relay.log")

        def relay_ready():
            if relay.poll() is not None:
                raise RuntimeError(f"relay exited: {relay.returncode}")
            try:
                return json.loads(ready_file.read_text())
            except json.JSONDecodeError:
                return None

        addresses = wait_for("loopback API relay", relay_ready, 10)
        proxy_config = {"apiVersion": "v1", "kind": "Config", "clusters": [{"name": "relay", "cluster": {"server": addresses["api"]}}],
                        "users": [{"name": "relay", "user": {}}], "contexts": [{"name": "relay", "context": {"cluster": "relay", "user": "relay"}}],
                        "current-context": "relay"}
        proxy_path = case / "scheduler-kubeconfig.json"
        proxy_path.write_text(json.dumps(proxy_config))
        # The existing config watcher watches its parent directory. Keep logs
        # and snapshots elsewhere so their writes do not trigger reloads.
        config_dir = case / "config"
        config_dir.mkdir()
        config = config_dir / "scheduler.conf"
        config.write_text('actions: "enqueue, allocate, backfill"\ntiers:\n- plugins:\n  - name: priority\n  - name: gang\n  - name: conformance\n- plugins:\n  - name: overcommit\n  - name: drf\n  - name: predicates\n  - name: proportion\n  - name: nodeorder\n')
        scheduler_env = dict(os.environ, DEBUG_SOCKET_DIR=str(case / "klog-socks"))
        scheduler = self.start([binary, "--kubeconfig", str(proxy_path), "--scheduler-name", scheduler_name,
                                "--scheduler-conf", str(config), "--leader-elect=false", "--schedule-period=200ms",
                                "--listen-address=127.0.0.1:0",
                                "--ignored-provisioners=kubernetes.io/no-provisioner", "--cache-dump-dir", str(cache_dir),
                                "--cache-dumper=true", "--logtostderr", "-v=4"], case / "scheduler.stdout", case / "scheduler.log", env=scheduler_env)
        self.control(addresses["control"], "/arm", "POST")
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "worker", "namespace": ns,
                "annotations": {"scheduling.k8s.io/group-name": "workers"}}, "spec": {"schedulerName": scheduler_name, "restartPolicy": "Never",
                "nodeSelector": {label: "ready"}, "tolerations": [{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists", "effect": "NoSchedule"}],
                "schedulingGates": [{"name": "repro.volcano.sh/admission"}],
                "containers": [{"name": "main", "image": self.args.image, "resources": {"requests": {"cpu": "100m", "memory": "32Mi"}},
                                "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]}],
                "volumes": [{"name": "scratch", "persistentVolumeClaim": {"claimName": "scratch"}}]}}
        self.apply(pod, case / "pod.json")
        wait_for("old gated Pod GET to be captured", lambda: self.control(addresses["control"], "/status").get("held"))
        self.event("old_get_held", case=name, status=self.control(addresses["control"], "/status"))
        actual = self.get("pod", "worker", "-n", ns)
        uid = actual["metadata"]["uid"]
        self.apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "scratch", "namespace": ns},
                    "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": resource, "resources": {"requests": {"storage": "64Mi"}}}}, case / "pvc.json")
        self.kub("patch", "pod", "worker", "-n", ns, "--type=json", "-p", '[{"op":"remove","path":"/spec/schedulingGates"}]')
        api = self.get("pod", "worker", "-n", ns)
        (case / "api-after-gate-removal.json").write_text(json.dumps(api, indent=2) + "\n")

        def ungated_snapshot():
            snapshot = self.snapshot(scheduler, cache_dir, uid)
            return snapshot if not snapshot["SchGated"] and not snapshot["gates"] else None

        before = wait_for("informer update present in scheduler cache", ungated_snapshot)
        # Let the normal unschedulable status update settle while the selector
        # still excludes the node. Otherwise a status watch already in flight
        # could incidentally repair the baseline immediately after release.
        marker = f"task unscheduleable {ns}/worker,"
        wait_for("unchanged unschedulable condition on a subsequent scheduling cycle",
                 lambda: any(marker in line and "skip by no condition update" in line
                             for line in (case / "scheduler.log").read_text().splitlines()))
        before = wait_for("settled ungated cache before GET release", ungated_snapshot)
        self.event("cache_before_release", case=name, **before)
        self.control(addresses["control"], "/release", "POST")
        wait_for("relay to release the captured old GET", lambda: "get_released" in (case / "relay.jsonl").read_text(), 10)
        if name == "patched":
            # An unchanged snapshot alone could precede processing the released
            # response. Observe the real worker's conflict and completed retry
            # before checking the post-response cache or enabling the node.
            def retry_completed():
                log = (case / "scheduler.log").read_text()
                conflict = log.find("task changed during resync")
                rows = [json.loads(line) for line in (case / "relay.jsonl").read_text().splitlines()]
                released = False
                fresh_get = False
                for row in rows:
                    if row["event"] == "get_released":
                        released = True
                    elif released and row["event"] == "get_response" and row["uid"] == uid and not row["gates"]:
                        fresh_get = True
                return (conflict >= 0 and fresh_get and
                        log.find(f"Successfully synced task <{ns}/worker>", conflict) >= 0)

            wait_for("patched worker to reject the old result and finish its retry", retry_completed, 30)

        def expected_snapshot():
            snapshot = self.snapshot(scheduler, cache_dir, uid)
            expected = name == "baseline"
            return snapshot if snapshot["SchGated"] == expected and bool(snapshot["gates"]) == expected else None

        after = wait_for(f"{name} post-release cache state", expected_snapshot, 30)
        self.event("cache_after_release", case=name, **after)
        node = self.get("nodes")["items"][0]["metadata"]["name"]
        self.kub("label", "node", node, label + "=ready")
        if name == "patched":
            scheduled = wait_for("patched Pod to bind", lambda: self.get("pod", "worker", "-n", ns)["spec"].get("nodeName"), 90)
            self.event("patched_pod_bound", case=name, nodeName=scheduled)
        else:
            # A bounded negative window verifies the stale gate remains effective
            # after capacity becomes available; do not retry until the test passes.
            end = time.monotonic() + 5
            while time.monotonic() < end:
                assert not self.get("pod", "worker", "-n", ns)["spec"].get("nodeName"), "baseline unexpectedly bound"
                time.sleep(0.2)
            self.event("baseline_still_pending", case=name, observationSeconds=5)
        final = self.get("pod", "worker", "-n", ns)
        (case / "pod-final.json").write_text(json.dumps(final, indent=2) + "\n")
        (case / "events.json").write_text(self.kub("get", "events", "-n", ns, "-o", "json"))
        result = {"case": name, "uid": uid, "beforeRelease": before, "afterRelease": after,
                  "finalNode": final["spec"].get("nodeName", ""), "namespace": ns}
        (case / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        scheduler.terminate()
        scheduler.wait(timeout=10)
        relay.terminate()
        relay.wait(timeout=10)
        return result

    def close(self):
        for process, stdout, stderr in reversed(self.children):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            stdout.close()
            stderr.close()
        self.trace.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("kubeconfig", "context", "repo", "relay", "baseline", "patched", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--image", default="registry.k8s.io/pause:3.10.1")
    parser.add_argument("--run-id", default=datetime.datetime.now(datetime.timezone.utc).strftime("%H%M%S") + str(os.getpid()))
    args = parser.parse_args()
    if not args.context.startswith("kind-") or not args.run_id.isalnum() or len(args.run_id) > 20:
        parser.error("use an explicit disposable kind context and a short alphanumeric run ID")
    run = Run(args)
    try:
        nodes = run.get("nodes")["items"]
        if not nodes or any(not node.get("spec", {}).get("providerID", "").startswith("kind://") for node in nodes):
            raise RuntimeError("Refusing to run outside a kind cluster")
        run.event("environment", context=args.context, serverVersion=json.loads(run.kub("version", "-o", "json"))["serverVersion"],
                  repoCommit=command(["git", "-C", args.repo, "rev-parse", "HEAD"]).strip(),
                  repoStatus=command(["git", "-C", args.repo, "status", "--short"]),
                  binarySHA256={name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
                                for name, path in (("baseline", args.baseline), ("patched", args.patched), ("relay", args.relay))})
        # No Volcano controller runs in this small fixture. Set the ordinary
        # Queue status that its queue controller would otherwise initialize.
        for name in ("root", "default"):
            if not run.kub("get", "queues.scheduling.volcano.sh", name, "--ignore-not-found", "-o", "json").strip():
                run.kub("create", "-f", "-", input=json.dumps({"apiVersion": "scheduling.volcano.sh/v1beta1", "kind": "Queue", "metadata": {"name": name}, "spec": {"weight": 1}}))
            run.kub("patch", "queues.scheduling.volcano.sh", name, "--subresource=status", "--type=merge",
                    "-p", json.dumps({"status": {"state": "Open"}}))
            run.event("queue_opened", name=name)
        results = [run.run_case("baseline", args.baseline), run.run_case("patched", args.patched)]
        (run.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results, indent=2))
    finally:
        run.close()


if __name__ == "__main__":
    main()
