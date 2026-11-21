# Volcano #6382 candidate validation

Source base: upstream master `993abc8deb337c67eff18186bfcc2d043fbaee2d`.
Candidate commit: `c5fb5be058db81c35351dd44c7334e8530976aaf`. Validation used an isolated archive with the final two candidate files; no validation command modified the contribution checkout. Paths below record the local execution layout.

## Candidate results

| Check | Result | Evidence |
| --- | --- | --- |
| Darwin `go test ./pkg/scheduler/cache -count=1` | PASS, 3.684s | `candidate-cache-unit.log` |
| Darwin `go test -race ./pkg/scheduler/cache -count=1` | PASS, 4.424s | `candidate-cache-race.log` |
| Linux affected cache package within full `-race` run | PASS, 3.888s | `candidate-linux-unit-race.log` |
| Linux-target configured golangci-lint | PASS, 0 issues, 1m47s | `candidate-linux-target-lint.log` |
| `make verify` | PASS, exit 0 | `candidate-make-verify.log` |
| `make TAG=latest generate-yaml` | PASS, exit 0 | `candidate-generate-yaml.log` |
| `make TAG=latest verify-generated-yaml` | PASS, exit 0 | `candidate-verify-generated-yaml.log` |
| staging `bash hack/verify-codegen.sh` | PASS, exit 0 | `candidate-staging-codegen.log` |
| Linux CI unit package set, `go test -p 1 -race` | COMPLETED, exit 1: 160 packages PASS; 1 eBPF package FAIL reproduced on pristine baseline | `candidate-linux-unit-race.log` |
| Host Darwin default `make lint` | ENVIRONMENT LIMIT: default 10m timeout | `candidate-make-lint.log` |
| Host Darwin default `make unit-test` | PLATFORM FAIL, stopped after Linux-only build errors | `candidate-make-unit-test.log` |

`.release-version` contains `latest`; this exact value was used for generation. After successful generation, no tracked generated-file diff remained.

The successful lint command used the repository's configured linters with Go 1.26.0 and golangci-lint v2.12.2, targeted Linux on the Darwin host, disabled CGO for analysis, and reduced concurrency/increased the timeout for local resource limits:

```sh
GOOS=linux CGO_ENABLED=0 GOMAXPROCS=2 /private/tmp/volcano-6382-tools/bin/golangci-lint run -v --concurrency=2 --timeout=20m
```

The Linux unit runner uses the same 161 test-package directories under `pkg` and `cmd` as the Linux Makefile, preserving `-race` but reducing `-p 8` to `-p 1`. It runs Go 1.26.0 linux/arm64 in `golang:1.26.0-bookworm`, with a read-only host module-cache mount and a task-local build-cache volume. The container initially shared the VM with the task's kind cluster and used 1.25 CPU / 1.5 GB. After the successful reproducer and deletion of the task-local kind cluster, it was raised to 2 CPU / 2.5 GB to complete the tests. Go memory limit remained 1100 MiB. Runner: `run-linux-unit.sh`; exact package list: `packages-linux.txt`.

## Environment and baseline distinctions

The unchanged baseline passed `make verify`, YAML generation and consistency, and staging codegen before candidate validation; see `baseline-*.log`.

The Darwin full-unit failures concern unmodified Linux-only agent/cgroups packages (`build constraints exclude all Go files`, undefined `unix.Openat2`, etc.). They are not failures in the changed cache package. Default host lint reached its configured 10-minute timeout; the Linux-target lint above completed successfully.

The Linux full run completed all 161 package entries: 160 passed, and the only failure was an unrelated, intermittent failure in `pkg/networkqos/utils/ebpf` / `TestCreateAndPinArrayMap`: expected the gomonkey-injected `new map failed` but received the prior case's `mkdir failed`. There were no reported data races or OOM kills (`podman inspect`: `exited 1 false`). A separate pristine baseline tree from the source archive reproduced that same mismatch with `-count=10` (exit 1); its first single run passed. `map.go` and `map_test.go` are byte-identical between baseline and candidate. A limited diagnostic rerun with `-gcflags=-l` on this package alone also failed, including a real BPF `operation not permitted`, so inlining alone is not established as the cause. No unrelated source or test was changed. Original full-run failure remains recorded and is not replaced by the diagnostic results.

```sh
# Same Go container and environment, pristine baseline source:
podman exec --workdir /checks/baseline-pristine volcano-6382-linux-unit \
  go test -p 1 -race ./pkg/networkqos/utils/ebpf -count=1
# exit 0; baseline-linux-ebpf-race.log
podman exec --workdir /checks/baseline-pristine volcano-6382-linux-unit \
  go test -p 1 -race ./pkg/networkqos/utils/ebpf -count=10
# exit 1; baseline-linux-ebpf-race-repeat.log
podman exec --workdir /checks/baseline-pristine volcano-6382-linux-unit \
  go test -p 1 -race -gcflags=-l ./pkg/networkqos/utils/ebpf -count=10
# exit 1; baseline-linux-ebpf-race-noinline.log
```

Sandbox Go-cache permission/network errors on first attempts were retried with approved access; successful final candidate logs replace those failed attempts. These are environment errors, not code failures.

The baseline root codegen script creates an untracked nested `volcano.sh/apis/pkg/client` directory and extensionless generated files. That behavior also occurs on the clean baseline; the first nested output was preserved at `root-codegen-untracked-output`. The Linux Makefile package set excludes this untracked directory. No cleanup or source changes were made to hide tracked generation differences.

## Final source fingerprints

The user's checkout and validation tree matched at the final freeze and were rechecked unchanged at completion:

```text
59235cba299602426076eab7692360bc6788541a11089262aff9a923d2656a1f  pkg/scheduler/cache/event_handlers.go
3cdaf8fb02ed934fc497dd0a1e68f18b0f4241bda63594526541fcb38909efb5  pkg/scheduler/cache/synctask_test.go
```

The independent regression RED run is tracked separately by the parent in `regression-red.log`: 11 failing regression subcases on baseline and 5 passing normal paths.

## Real-cluster comparison

The parent completed the final reproducer (`r03`, exit 0) against the task-local kind v1.34.3 cluster. Result files: `/private/tmp/volcano-6382-run/evidence-r03/results.json`; scheduler/relay logs and snapshots are in the same evidence directory. Earlier comparison `r02` also passed, but `r03` adds an explicit ordering assertion: wait for the conflict, a real ungated retry GET, and successful sync before taking the after snapshot and enabling the node.

- Baseline: cached Pod RV changed from 576 to stale 563 and `SchGated` changed from false to true. After the node became available, the Pod remained unbound through the 5-second observation window.
- Candidate: cached Pod remained RV630 and ungated after the old RV617 GET response was released. The scheduler logged the conflict and successful sync after the retry fetched RV630; the Pod then bound to `volcano-6382-control-plane`.

Final driver command:

```sh
python3 /private/tmp/volcano-6382-repro-draft/run.py \
  --kubeconfig /private/tmp/volcano-6382-run/kubeconfig \
  --context kind-volcano-6382 \
  --repo /Users/kuantian/open-code/volcano \
  --relay /private/tmp/volcano-6382-repro-draft/api-relay-bin \
  --baseline /private/tmp/volcano-6382-run/vc-scheduler-baseline \
  --patched /private/tmp/volcano-6382-run/vc-scheduler-patched \
  --run-id r03 \
  --output /private/tmp/volcano-6382-run/evidence-r03
```

Linux race container command (CPU was updated from its initial 0.75 to 1.25 after the kind cluster became ready, then to 2 CPU / 2.5 GB after the task-local kind cluster was deleted):

```sh
podman run --name volcano-6382-linux-unit --cpus=0.75 --memory=1536m \
  --mount type=bind,source=/private/tmp/volcano-6382-checks/validation,target=/workspace \
  --mount type=bind,source=/private/tmp/volcano-6382-checks,target=/checks \
  --mount type=bind,source=/Users/kuantian/go/pkg/mod,target=/go/pkg/mod,readonly \
  --mount type=volume,source=volcano-6382-linux-build,target=/cache \
  -e GOCACHE=/cache/go-build -e GOTOOLCHAIN=local -e GOMAXPROCS=2 \
  -e GOMEMLIMIT=1100MiB -e GOPROXY=off -e GOFLAGS=-mod=readonly \
  docker.io/library/golang:1.26.0-bookworm \
  bash -c 'bash /checks/run-linux-unit.sh > /checks/candidate-linux-unit-race.log 2>&1'
podman update --cpus=1.25 volcano-6382-linux-unit
podman update --cpus=2 --memory=2560m volcano-6382-linux-unit
```
