# Critical Kafka Validation - 2026-09-09

For the 2026-09-16 recovery patch, changed dependency/toolchain requirements and
current local verification, see [RECOVERY-20260916.md](RECOVERY-20260916.md).
The results below describe the earlier candidate, not the recovery candidate.

## Scope and status

Target: Hangcha domestic production, namespace `smartlink`, critical MongoShake
lane. Production remains READ ONLY. No deployment, configuration, partition,
checkpoint, Mongo data, Redis data, or RabbitMQ changes were made.

The initial validation package has now been extended with an opt-in production
implementation and a locally built candidate image. It is NOT deployed or
approved as production-ready. See [the current implementation and release status](PRODUCTION-CANDIDATE.md).
The earlier laboratory sender/noise models remain test-only; the actual new
sender is `tunnel/kafka_acknowledged_writer.go`.

- Repository: `https://github.com/aivp/MongoShake.git`.
- Local branch: `codex-critical-kafka-validation`.
- Baseline: `9af224464481f877c761963aa4bad71573657269`, matching the live version
  report (`improve-2.8.4`, built with Go 1.15.10).
- Company `origin/develop` was `acbaf60`; its Kafka, worker, controller and filter
  implementations are identical to the baseline. No company-branch changes
  were silently folded into a production release.
- Local tests: Go 1.26.1, darwin/arm64, Sarama 1.27.2. Production-toolchain and
  deployed-image acceptance are separate, still required.

## Confirmed findings

1. Legacy `KafkaWriter.Send` accepts an in-memory queued message without any
   broker writer. `AckRequired()` is false. A characterization test reproduces
   this using the unchanged production implementation.
2. Actual `WriteController.Send` advances `LatestLsnAck` under that queue-only
   contract. This is an acknowledgement/checkpoint safety gap, not evidence
   that a particular production event was already lost. A process exit after
   checkpoint persistence but before delivery can skip the in-memory interval
   on restart. Increasing buffers is not a safe repair.
3. Batch submission reduces producer round trips. With a local protocol mock,
   1024 records of 512 bytes and a fixed 1 ms response delay used 1024 Produce
   requests in the unchanged `SimpleWrite`, versus 8 requests with batches of
   128. One run measured 1.401 s versus 13.309 ms. These timings are synthetic,
   not production capacity, broker disk performance, or a promised speedup.
4. Candidate idempotence requires protocol >= 0.11, acks=all, one in-flight
   request and retries. Sarama 1.27.2 rejects the tested unsafe combinations.
   This changes the producer contract; it is not achieved by increasing
   `incr_sync.tunnel.write_thread` or the collector adaptive-batch limit alone.

## Safety tests and their limits

- An illustrative state model covers bind, unbind, rebind, duplicates,
  out-of-order old events, interrupted replay, and a 0/1 state transition.
- The model demonstrates that whole-batch replay without a version guard can
  regress a state. It is NOT a reproduction of CFS business behavior.
- The initial test-only sender model advanced checkpoint only after delivery.
  The subsequent opt-in implementation now exercises the real writer factory,
  controller, worker and checkpoint manager, including a subprocess crash and
  restart against a local Kafka broker. This is not a deployed CFS business test.
- The candidate noise classifier keeps every namespace except exact
  `ds_production.Carriers`. Only supported, identified, non-transactional, pure
  `$set` updates containing the four time fields can be classified as noise:
  `survivalTime`, `canTimestamp`, `updated_at`, `created_at`.
- Business fields, mixed updates, inserts, deletes, replacements, `$unset`,
  upsert shapes, v2 diffs, unknown modifiers, missing identity, and protected
  binding/driver/organization namespaces are retained. This is a test-only
  candidate; source Mongo writes and existing consumers are unchanged.

Local CFS source was inspected, not changed:
`/Users/wangqiang/.codex/worktrees/3024/CarrierFoundationService`.
`MongoShakeServiceImpl.processCarrier` and `processCarrierDevice` reload Mongo
by object ID, so it is incorrect to equate every replay with applying an old
field value. `CarrierDeviceServiceImpl.updateCache` also has a deletion branch
that removes mappings by carrier/device keys. Real acceptance must therefore
cover two different binding document IDs sharing the same logical device key,
including an old unbind/delete after a new bind. A document-only timestamp guard
must not be assumed sufficient. No deployed CFS/Redis business E2E claim is made.

## Executed checks

Targeted tests and the targeted race run passed:

```sh
go test -mod=readonly ./tunnel/kafka ./tunnel ./collector ./tools/critical-validation -run '^TestCritical' -count=1 -timeout=90s -v
go test -mod=readonly -race ./tunnel/kafka ./tunnel ./collector ./tools/critical-validation -run '^TestCritical' -count=1 -timeout=90s
```

The opt-in local broker test is skipped unless explicitly enabled below.
The existing `TestKafkaWriter` passed. The unchanged upstream
`collector/filter/TestNamespaceFilter` failed at `filter_test.go:173`:
its applyOps case expects one operation but receives two. No production filter
file was modified. This is a baseline failure, not a fully green repository
suite. `go.sum` gained three checksums for already-selected YAML test dependencies;
`go.mod` and dependency versions were not changed.

## Local Kafka integration

Use ONLY the loopback-only properties in this directory. The integration test
has no user-supplied broker address and is fixed to `127.0.0.1:39092`. It creates
uniquely named `codex-critical-validation-*` topics and deletes only those topics.
It verifies ordered offsets and exact payload bytes for both original and
candidate producers, then checks replay across a new producer session.

```sh
CRITICAL_LOCAL_KAFKA_TEST=1 go test -mod=readonly ./tunnel/kafka -run '^TestCriticalLocalKafkaOrderingAndReplay$' -count=1 -timeout=90s -v
```

Real-broker result: PASS at 2026-09-09 15:11 CST. Apache Kafka 3.6.2 ran locally
with Java 11, one partition and one replica; it was stopped after the test and
both loopback listener ports were confirmed closed.

| Local case | Records | Send time | Observed rate | Payload and ordered offset check |
| --- | ---: | ---: | ---: | --- |
| Unchanged `SimpleWrite` | 4096 x 512 bytes | 868.231 ms | 4718/s | PASS |
| Test-only batch of 128 | 4096 x 512 bytes | 24.827 ms | 164983/s | PASS |

These are one-shot loopback measurements, not production throughput promises.
The candidate also changes protocol, acknowledgement and idempotence settings;
this is an end-to-end candidate comparison, not an isolated batching benchmark.
There was no injected broker failure, process crash, network timeout, production
load, or actual collector-to-CFS cache update in this integration test.

After reopening the candidate idempotent producer, replay of the oldest payload
was accepted at offset 4096, after the complete original sequence. This verifies
that producer idempotence does not deduplicate application replay across a new
producer session. Consumer correctness must not rely on it doing so.

The [Apache binary distribution](https://archive.apache.org/dist/kafka/3.6.2/kafka_2.13-3.6.2.tgz)
matched its [official SHA-512 checksum](https://archive.apache.org/dist/kafka/3.6.2/kafka_2.13-3.6.2.tgz.sha512)
before extraction or execution:

```text
e5d5935df6e687898e71e583e8ea376275c6fbac2e7872e78f7c55ab2528485582362e3778678600c5368384437c1da6b3d612a748917f4b294c06ea160173ee
```

This patch level is a 3.6-series compatibility test, not an assertion that
production uses patch version 3.6.2.

## Implementation and remaining acceptance gates

1. Implemented as an opt-in bounded sender, preserving partition mapping and
   one oplog per message. The 128-record, 512 KiB and 5 ms settings are candidate
   settings, not live changes. A larger supported single record is sent alone;
   the existing 18 MiB producer message limit has not been reduced.
2. Implemented acknowledgement, filtered-tail and controlled-shutdown guards.
   Local tests cover delayed ACK, failure after an accepted prefix, checkpoint
   persistence failure and real subprocess death/restart. Staging network,
   source Mongo and actual consumer failure tests remain required. Exactly-once
   delivery is NOT claimed.
3. Verify duplicate/replay and cross-record binding correctness against actual
   CFS services and Redis. Preserve legitimate later binds when old mappings
   are deleted, preferably through current-state reconciliation or conditional
   removal of the specific binding. Verify partial multi-key failures too.
4. Stage field-aware filtering separately, initially count-only. Confirm live
   field shapes, source event shares and downstream consumers before enabling
   it. Do not stop writing Mongo fields merely because the critical lane does
   not need their events. Normal-lane consumers may have different contracts.
5. Build with an explicitly approved toolchain, review the immutable image,
   run staging load/failure tests, and prepare checkpoint-aware rollback before
   a separately approved critical-only production rollout. Do not rewrite a
   checkpoint or restart the lagging collector as an unreviewed shortcut.

The existing single-replica Kafka topic remains a durability limitation; acks=all
with one in-sync replica does not create additional data replicas. Production
CPU throttling, broker request/disk latency and current collection shares remain
observability gaps from the earlier read-only investigation.
