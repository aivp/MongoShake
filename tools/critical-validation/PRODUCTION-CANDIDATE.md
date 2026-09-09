# Hangcha Domestic Critical Kafka Candidate

## Status at 2026-09-09 16:00 CST

Production-source implementation, targeted tests, local Kafka integration,
Linux/amd64 build and isolated container version check are complete. No image
was pushed, no Git commit/PR was made, and no production workload or configuration
was changed. The source remains on `codex-critical-kafka-validation` at baseline
`9af224464481f877c761963aa4bad71573657269` with local changes.

This is a release candidate for further acceptance, NOT a finished production
rollout and NOT proof that the live critical backlog is resolved.

## Fresh deployment facts

Read-only Kubernetes API inspection on 2026-09-09 around 15:51-15:56 CST used
`/Users/wangqiang/.kube/smartlink-readonly.yaml`, namespace `smartlink`, through
the already available access route. No new tunnel or Kubernetes exec was used.

- Critical Pod: `mongoshake-critical-0`, owner `StatefulSet/mongoshake-critical`.
- Normal Pod: `mongoshake-0`, owner `StatefulSet/mongoshake`.
- Both reference the same untagged image repository, with the same observed
  runtime image digest:
  `aidong-backend.tencentcloudcr.com/aidong/mongoshake@sha256:bc90a9815159ed0dee272381efd59d939910e736727c6da53757a84fa3cfa849`.
- Critical actually starts `/app/collector.linux -conf /app/collector.conf`.
  Updating `/app/collector` alone would NOT update this running program.
- Critical mounts `middleware-conf` key `critical-collector.conf` as
  `/app/collector.conf` via a read-only subPath mount. Normal uses the different
  `collector.conf` and `receiver.conf` keys in the same ConfigMap.
- Critical is incremental, JSON with the existing empty/default JSON format,
  oplog fetch, one Mongo source, one worker, one write thread and one Kafka
  partition. Existing checkpoint database/collection are
  `mongoshake.ckpt_cache_critical`. No credentials were copied into this report.
- Critical Pod has a 30-second termination grace period and no lifecycle hook.
  The new controlled-shutdown drain is not an added SIGTERM/preStop handler.
  Do not assume a Kubernetes restart drains the old implementation safely.

## Actual implementation

`incr_sync.tunnel.kafka.acknowledged` is opt-in and defaults to false. The legacy
writer remains unchanged when it is false, including its known early-ACK risk.
Only the critical workload is intended to receive this candidate.

| Mode | Acknowledged | Batch enabled | Behavior |
| --- | --- | --- | --- |
| Legacy/default | false | false | Existing path; not a safe rollback target after migration |
| Safe single send | true | false | Waits for Kafka before advancing worker ACK |
| Safe batch send | true | true | Same ACK contract, bounded ordered batches |
| Invalid | false | true | Rejected during startup validation |

The new writer is currently restricted to the verified single-source,
single-worker, single-partition incremental JSON/oplog topology. Unsupported
topologies or debug sinks are rejected, not silently changed.

- Each oplog remains one message with the same JSON encoding and manual
  partition mapping. No JSON-array envelope or field filter is introduced.
- All chunks of one worker message must succeed before its source timestamp is
  returned to the controller. The safe boundary is deliberately conservative
  when an earlier chunk succeeded but a later chunk failed.
- Sarama 1.27.2 is unchanged. Its acknowledged producer uses protocol 0.11,
  idempotence, `acks=all`, one in-flight request and three client retries.
  Kafka server software and topic partitions are not upgraded or changed.
- After a final encoding/send error the writer fences itself. No application
  whole-batch retry or later send occurs. The worker remains blocked, without
  repeatedly re-encoding the failed batch. Recovery needs operator review;
  do not add an automatic restart loop as a workaround.
- The existing default JSON format cannot encode values such as NaN/infinity.
  Unlike the legacy writer's silent skip for these encoding errors, the new
  path stops. Validate representative source payloads before rollout; do not
  silently change the CFS wire format to bypass an encoding problem.
- Filtered-tail checkpoint advancement waits for pending worker messages,
  including equal-timestamp messages. Controlled shutdown waits for the pending
  interval and persisted checkpoint rather than treating a short timeout as
  completion.
- CheckpointManager publishes its in-memory position only after storage accepts
  the write, preserving retries after a storage error. The HTTP checkpoint
  backend now treats non-200 responses as failures and closes response bodies.
  These shared checkpoint helpers change in this candidate too; the normal
  production image is not replaced.
- Existing maximum producer message size remains 18 MiB including Sarama's
  additional envelope checks. The batch byte target is NOT a per-record limit.
  A supported record larger than that target is sent on its own. The broker's
  configured acceptance limit still applies; no production size limit changed.

The proposed initial configuration overlay is
[`critical-kafka-delivery.conf.example`](critical-kafka-delivery.conf.example).
It enables acknowledged delivery but leaves batching disabled. Do not replace
the entire production configuration with this partial file.

## Tests and artifacts

All targeted tests, including the opt-in local integrations, passed with race
detection on Go 1.26.1, darwin/arm64:

```sh
CRITICAL_LOCAL_KAFKA_TEST=1 go test -mod=readonly -race ./tunnel/kafka ./tunnel ./collector ./collector/configure ./collector/ckpt ./tools/critical-validation -run '^TestCritical' -count=1 -timeout=120s
```

| Check | Result and boundary |
| --- | --- |
| Delayed ACK | Actual sender waits; no premature ACK is returned |
| Safe single vs batch | Actual factory/controller/worker with Kafka 3.6.2; 1024 ordered payloads and final stored checkpoint verified in both modes |
| Broker rejection after prefix | Real Kafka size rejection after an accepted prefix; no later sends, no whole-batch retry and no checkpoint advance |
| Process death and restart | Child exits after real Kafka ACK, before controller/ckpt progress; restart loads old checkpoint and replays the complete interval, including duplicates |
| Large message | Real Kafka accepts a 2 MiB record with a 512 KiB batching target; payload/order retained |
| Storage failure | Failed checkpoint write does not advance memory; same-position retry succeeds |
| Filter/shutdown guards | Pending and equal-timestamp messages block forced advancement; filtered-only shutdown remains possible |
| Legacy regressions | Existing `TestKafkaWriter`, `TestCalculateWorkerLowestCheckpoint` and `TestCheckFcv` passed |

The initial full-path 1024-record sample measured 543.704 ms with acknowledged
single sends and 272.305 ms with batches. These one-shot local timings include
more of the real pipeline than the earlier producer-only benchmark. Neither
measurement predicts production capacity. A local loopback HTTP store backs
the actual checkpoint manager in collector tests; this is not a production
Mongo persistence or real CFS/Redis test.

The previously observed unchanged `collector/filter/TestNamespaceFilter`
applyOps baseline failure is not fixed. Full repository tests, some of which
expect local databases and can mutate them, were not indiscriminately run.
No claim of a fully green repository suite is made.

Local candidate image: `mongoshake-critical-candidate:20260909` (not pushed).
Image ID:
`sha256:867f58d18682c04ef5c36662676705dc27d19bf2fa95ee06279d8d59331603cd`.
It uses the exact observed production runtime image as its base and replaces
only `/app/collector.linux`. The container was run with no network and a read-only
filesystem for `-version`; it reported
`critical-ack-batch-candidate,9af2244,dirty,go1.26.1,NOT-RELEASED`.
The binary checksum inside the image matched the host build. The isolated local
Kafka process was stopped after testing; no test broker is left running.

Local Linux/amd64 binary:
`.validation-kafka-20260909/collector-linux-amd64`.
SHA-256:
`2112c20b36d22bbaf64902deb08be11cb786119a43b873c07af03c862a5ef019`.

Important toolchain distinction: production was built with Go 1.15.10; this
local candidate was built with Go 1.26.1. It does NOT prove production-toolchain
equivalence. Fix the release toolchain explicitly and validate it in CI/staging;
do not silently promote this dirty local build. No Go module/dependency version
was upgraded. The three pre-existing checksum additions are recorded in
`go.sum`; `go.mod` remains unchanged.

## Production release gates

1. Review and commit the exact patch. Build a clean, traceable image with an
   explicitly chosen toolchain. Review inherited runtime/dependency security
   separately; reusing the current base is not a security acceptance.
2. Run real CFS/Redis binding, unbinding, rebinding, duplicate, out-of-order and
   partial multi-key failure tests, including two binding documents sharing one
   logical device key. Run the normal and critical consumer interaction too.
   Fix CFS first if these fail. Local sender replay tests do not satisfy this.
3. Stage representative Mongo/oplog traffic and failure/recovery under the
   actual topic, ACL, message-size and load constraints. The single-replica
   Kafka topic remains a durability limitation; acks=all does not add replicas.
4. Establish a reviewed old-to-new cutover point. The old checkpoint may be
   ahead of broker delivery. Verify the retained oplog window and actual Kafka
   boundary; do not trust the old ACK alone, force-stop the old process, or
   silently rewind/overwrite checkpoints. Any bounded replay needs its own
   explicit record and consumer-safety acceptance.
5. Publish a NEW immutable image reference and update ONLY the critical
   StatefulSet and ONLY the critical ConfigMap key. Never overwrite the shared
   untagged/latest image or change ordinary-chain keys, topics, offsets or
   Mongo/Redis data as part of this release. Refresh the workload before action.
6. Start acknowledged single-send mode, verify real progress, then enable the
   tested batch settings separately. Front filtering remains test-only and
   belongs to a different rollout.
7. Monitor at least the first hour and one full workday's morning/afternoon
   peaks. Compare actual source events, Kafka appends, CFS group lag and business
   results; legacy queue-acceptance counters are not a like-for-like ACK metric.

`/worker` exposes `kafka_delivery` with `acknowledged`, `last_broker_ack`,
`in_flight_records`, `confirmed_records`, `confirmed_batches`, and `fenced`, plus
`pending_kafka_batches`. `last_broker_ack` is the safe whole-worker-message
boundary; confirmed chunk counters can be ahead of it. These are not CFS/Redis
acknowledgements or unique-event counters.

Any `fenced=true`, lost/wrong binding, missing event or growing downstream lag
stops further rollout. Arrange an actionable fence alert before enabling the
mode. For a performance regression, turn ONLY `batch.enabled` off while keeping
acknowledged=true. This startup-bound setting needs a controlled process
replacement; it is not a hot runtime toggle. An image rollback must also
preserve a compatible, reviewed checkpoint/replay position. Do not clear queues,
reset offsets, replace the current checkpoint with its old backup, or claim
that a green Pod proves successful synchronization.
