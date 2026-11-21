# Reproduce stale `syncTask` results against a real kind API

This bundle exercises the Pod/Task cache race in Volcano scheduler baseline
`993abc8deb337c67eff18186bfcc2d043fbaee2d` and compares it with the snapshot-check
fix at `c5fb5be058db81c35351dd44c7334e8530976aaf`. The reproduction branch is based
on that fix and adds this bundle and its evidence; it uses the same repaired
scheduler source. The core fix is also available on the separate
`fix/synctask-stale-cache-6382` branch.

Both schedulers run as local processes against a disposable kind cluster. The
workload, PVC, PodGroup, gate removal, watch events, and bind requests use the real
Kubernetes API.

This is a controlled reproduction of a code path. It does **not** establish that
the same interleaving caused the original production incident. That incident
confirmed a stale cached gated Pod; its original request/watch timing was not
captured.

## How the interleaving is controlled

1. The driver creates a PodGroup and a gated Pod referencing a PVC that has not
   been created yet. The scheduler's ordinary missing-PVC handling caches the task
   and queues a `syncTask` retry.
2. The local API relay forwards the retry's target Pod GET to the real API server.
   After receiving that real gated Pod response, it delays that one response.
3. The driver creates the PVC and removes the Pod's gate with a normal API patch.
   Watches and all other requests continue through the relay.
4. The driver waits until a scheduler cache snapshot shows the gate-removal watch
   update has reached the scheduler: the cached Pod has no gates and
   `SchGated=false`. It also waits for the ordinary unschedulable Pod condition
   to settle while the node selector is still unmatched, then captures the
   ungated cache again. This prevents an in-flight status watch from incidentally
   repairing the baseline immediately after the old GET is released.
5. The driver releases the previously captured GET response without changing its
   body or resourceVersion. The baseline can then write its old gated result over
   the updated cache. The patched scheduler should reject that obsolete result
   and preserve the newer cache. In the patched case it waits for a conflict,
   a subsequent real ungated GET, and successful resync completion before taking
   the post-release snapshot.
6. Only after observing the post-release cache state does the driver add the
   workload's node-selector label. This keeps both tasks pending during the cache
   comparison, then allows the patched task to bind through normal scheduling.

The relay does not fabricate a Pod, modify scheduling gates, replay a synthetic
watch event, or call an internal scheduler method. Its injection is limited to
delaying one successful GET for the selected namespace and Pod. It forwards API
responses as JSON so the captured response can be logged. Both relay listeners
are restricted to explicit loopback addresses.

The reproduction uses a regular referenced PVC and manually removes an external
gate; it does not require Kueue, a GPU, or a CSI deployment. The manual gate patch
models an external controller's normal gate-removal operation. The missing PVC
and the initial unmatched node selector are intentional workload inputs.

## Prerequisites and source inputs

- Go 1.26, Python 3, `kubectl`, and Git.
- kind `v0.33.0` and node image `kindest/node:v1.34.3`.
- A working kind-supported container engine, such as Docker or Podman.
- A fresh working directory for this reproduction.

Clone the reproduction branch from the fork. A normal, non-shallow clone includes
the pinned baseline ancestor needed by `git archive`.

```bash
set -eu
git clone --branch repro/synctask-stale-cache-6382 --single-branch \
  https://github.com/kuantian-zhang/volcano.git volcano-6382
cd volcano-6382

export VOLCANO_PATCHED="$PWD"
export VOLCANO_BASELINE="$VOLCANO_PATCHED/../volcano-6382-baseline"
export VOLCANO_REPRO_ROOT="$VOLCANO_PATCHED/test/repro/issue-6382"
export VOLCANO_REPRO_RUN="$VOLCANO_PATCHED/../volcano-6382-run"
export VOLCANO_REPRO_KUBECONFIG="$VOLCANO_REPRO_RUN/kubeconfig"
export VOLCANO_REPRO_CONTEXT=kind-volcano-6382
export VOLCANO_BASE_COMMIT=993abc8deb337c67eff18186bfcc2d043fbaee2d

# Use a new baseline directory, so an existing source tree is not overwritten.
test ! -e "$VOLCANO_BASELINE"
mkdir "$VOLCANO_BASELINE"
git archive "$VOLCANO_BASE_COMMIT" | tar -x -C "$VOLCANO_BASELINE"
mkdir -p "$VOLCANO_REPRO_RUN/tools"

GOBIN="$VOLCANO_REPRO_RUN/tools" go install sigs.k8s.io/kind@v0.33.0
export VOLCANO_REPRO_KIND="$VOLCANO_REPRO_RUN/tools/kind"
```

The repaired scheduler can be built directly from this reproduction branch:
its scheduler implementation is the core fix commit above. The baseline binary
must be built from the separate archived source tree.

### Optional Podman proxy handling

For an ordinary Podman setup, set `KIND_EXPERIMENTAL_PROVIDER=podman` for cluster
creation. In a macOS/Podman setup with proxy environment variables, node
containers can inherit lower-case proxy variables and kubeadm's API client may
fail with EOF. If that applies, use this task-local wrapper. It adds
`--http-proxy=false` only to `podman run`; other Podman operations pass through.
No proxy values need to be printed or placed in this document.

```bash
# Capture the real executable before adding the task-local wrapper to PATH.
export VOLCANO_REPRO_REAL_PODMAN="$(command -v podman)"
mkdir -p "$VOLCANO_REPRO_RUN/no-proxy-bin"
cat > "$VOLCANO_REPRO_RUN/no-proxy-bin/podman" <<'SH'
#!/bin/sh
case "$1" in
  run)
    shift
    exec "$VOLCANO_REPRO_REAL_PODMAN" run --http-proxy=false "$@"
    ;;
  *) exec "$VOLCANO_REPRO_REAL_PODMAN" "$@" ;;
esac
SH
chmod +x "$VOLCANO_REPRO_RUN/no-proxy-bin/podman"

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u NO_PROXY \
  -u http_proxy -u https_proxy -u all_proxy -u no_proxy \
  PATH="$VOLCANO_REPRO_RUN/no-proxy-bin:$PATH" \
  KIND_EXPERIMENTAL_PROVIDER=podman \
  "$VOLCANO_REPRO_KIND" create cluster --name volcano-6382 \
    --kubeconfig "$VOLCANO_REPRO_KUBECONFIG" \
    --image kindest/node:v1.34.3 --wait 120s
```

That `env` invocation clears all eight upper- and lower-case proxy variables only
for this cluster-creation command. The wrapper directory is added only to that
command's PATH; no global shell or Podman configuration is changed. If this
command created the cluster, skip the ordinary `create cluster` command below.

## Create the disposable cluster and install CRDs

Use an explicit kubeconfig for every command. The driver also refuses contexts
that do not start with `kind-` and verifies the nodes' `kind://` provider IDs.
For ordinary Podman operation, prefix the cluster-creation command with
`KIND_EXPERIMENTAL_PROVIDER=podman`; the optional proxy-handling command above
already sets that provider explicitly.

```bash
"$VOLCANO_REPRO_KIND" create cluster --name volcano-6382 \
  --kubeconfig "$VOLCANO_REPRO_KUBECONFIG" --image kindest/node:v1.34.3 --wait 120s

kubectl --kubeconfig "$VOLCANO_REPRO_KUBECONFIG" \
  --context "$VOLCANO_REPRO_CONTEXT" apply \
  -f "$VOLCANO_BASELINE/config/crd/volcano/bases"

kubectl --kubeconfig "$VOLCANO_REPRO_KUBECONFIG" \
  --context "$VOLCANO_REPRO_CONTEXT" wait \
  --for=condition=Established --timeout=60s \
  -f "$VOLCANO_BASELINE/config/crd/volcano/bases"
```

No Volcano scheduler Deployment or controller Deployment is required. The driver
uses a distinct scheduler name for each case and creates the needed PodGroups.
It creates the `root` and `default` Queues if absent and sets their status to
`Open` through the API, replacing the queue controller's usual initialization.
The scheduler configuration has its own directory so log and snapshot writes
do not trigger the scheduler's parent-directory configuration watcher.

## Build the two schedulers and API relay

Build host binaries: the scheduler and relay run outside the kind node and use
the cluster's host-accessible API endpoint.

```bash
(
  cd "$VOLCANO_BASELINE"
  go build -o "$VOLCANO_REPRO_RUN/vc-scheduler-baseline" ./cmd/scheduler
)
(
  cd "$VOLCANO_PATCHED"
  go build -o "$VOLCANO_REPRO_RUN/vc-scheduler-patched" ./cmd/scheduler
  go build -o "$VOLCANO_REPRO_RUN/api-relay" \
    "$VOLCANO_REPRO_ROOT/api-relay/main.go"
)
```

The relay is built from within the candidate module so it uses the checked-out
client-go dependency versions. It requires no separate module or additional Go
dependencies.

## Run the baseline/candidate comparison

The output directory must not already exist; the driver creates it exclusively.
Use a new name for each intentional run. The run ID must be alphanumeric and at
most 20 characters.

```bash
python3 "$VOLCANO_REPRO_ROOT/run.py" \
  --kubeconfig "$VOLCANO_REPRO_KUBECONFIG" \
  --context "$VOLCANO_REPRO_CONTEXT" \
  --repo "$VOLCANO_PATCHED" \
  --relay "$VOLCANO_REPRO_RUN/api-relay" \
  --baseline "$VOLCANO_REPRO_RUN/vc-scheduler-baseline" \
  --patched "$VOLCANO_REPRO_RUN/vc-scheduler-patched" \
  --run-id manual01 \
  --output "$VOLCANO_REPRO_RUN/evidence-manual01"
```

`--image` can override the default `registry.k8s.io/pause:3.10.1`. Binding is the
success condition, so an image pull is not needed to compare scheduling results.
The driver stops its scheduler and relay subprocesses on completion or failure.
It leaves cluster objects and evidence files available for inspection.

## Evidence and expected assertions

These are the driver's expected checks. Record execution results separately
after inspecting the generated evidence:

- Before releasing the delayed GET, both cases must show an ungated scheduler
  cache for the actual target Pod UID.
- After release, the baseline is expected to show the captured old Pod/gates
  restored in its cache and remain unbound for a bounded five-second observation
  after the node label is added.
- The patched case is expected to retain an ungated cache and bind once the node
  label is available.

The output includes:

- `operations.jsonl`: chronological driver actions and cache observations.
- `baseline/relay.jsonl` and `patched/relay.jsonl`: the exact captured/released GET
  UID, resourceVersion, gates, and timestamps.
- Each case's `api-after-gate-removal.json`, `pod-final.json`, and `events.json`.
- Scheduler logs, emitted workload manifests, cache dumps, and copied observation
  snapshots under each case directory.
- Each case's `result.json` and the final comparison `results.json`.

Record the actual command, server version, baseline commit, candidate diff, exit
status, and resulting UID/resourceVersion observations when publishing results.
Do not describe a failed setup, timeout, or unexecuted run as a successful cluster
reproduction. Unit-test RED/GREEN output is separate evidence from this real-API
comparison.

## Cleanup

After collecting evidence, remove the disposable cluster explicitly:

```bash
"$VOLCANO_REPRO_KIND" delete cluster --name volcano-6382 \
  --kubeconfig "$VOLCANO_REPRO_KUBECONFIG"
```

## Recorded execution

The final fixture completed once on 2026-11-21 with exit code 0: one expected
baseline/candidate comparison out of one completed comparison. This deliberately
controlled ordering is not a measurement of the natural production failure rate.

| Case | Delayed GET RV | Cache before release | Cache after release | After enabling the node |
| --- | --- | --- | --- | --- |
| Baseline `993abc8` | 563, gated | 576, ungated | 563, gated | Unbound throughout five-second observation |
| Patched `c5fb5be` | 617, gated | 630, ungated | 630, ungated | Bound to `volcano-6382-control-plane` |

Resource versions in each row refer to one Pod UID. The two rows use separate
Pods. The patched relay trace records a subsequent real GET at RV630 after the
old result was rejected.

Environment: kind v0.33.0, Kubernetes v1.34.3 linux/arm64, Go 1.26.0
darwin/arm64 scheduler binaries, Python 3.13.10. See the
[manifest](evidence/r03/manifest.json), [results](evidence/r03/results.json),
and [driver timeline](evidence/r03/operations.jsonl).

Each case includes the target TaskInfo extracted from every numbered raw cache
observation, with the original full snapshot's SHA256, and the real API Pod
objects and GET trace. Scheduler logs are explicitly labeled excerpts. The full
raw output was retained locally; kubeconfigs and credentials are excluded from
this published evidence.

An earlier development trial reached the cache assertions but did not complete
the binding assertion: the fixture had not initialized Queue status and wrote
logs in the watched configuration directory. Those fixture setup errors were
corrected before the recorded completed run. Proxy-related kind setup failures
also preceded the completed run; the optional Podman procedure above records the
local environment correction.

An intermediate completed comparison (`r02`) also passed. Its actual logs
contained the same ordered conflict/retry/success sequence. The final `r03`
driver explicitly waits for that sequence to remove a possible premature
post-release observation from the fixture itself. The table above reports only
the final driver version's one completed run.

## Validation report

See [the complete check report](checks/RESULTS.md), including the original
Linux 161-package result (160 pass, one unmodified baseline-flaky eBPF failure),
cache/race results, lint, generation checks, and baseline RED evidence.
