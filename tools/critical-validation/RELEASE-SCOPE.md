# Critical-only release scope

The scope below records the 2026-09-09 release. The separate
`codex-critical-kafka-recovery` candidate starts at `ea1175ba` and is described in
[RECOVERY-20260916.md](RECOVERY-20260916.md). It is not a deployment or an extension
of the earlier release approval.

## Decision recorded on 2026-09-09

Target: Hangcha domestic Tencent production, namespace `smartlink`.
The user requires this release to exclude the six unrelated commits on the
previous `develop` branch. Merging code into `develop` did not authorize using
that branch as the production build source.

## Pinned source

- Production-reported baseline: `9af224464481f877c761963aa4bad71573657269`.
- Only implementation patch: `b3e2b478b6f920eb62389cb12551d3e63dc74472`.
- Production candidate source commit: `b3e2b478b6f920eb62389cb12551d3e63dc74472`.
- Dedicated branch: `codex-critical-kafka-release`.
- This branch may additionally contain this documentation-only scope record;
  it must not acquire other implementation changes without scope review.
- Do not build this production release from `develop`,
  `codex-critical-kafka-validation`, or a floating branch/tag reference.

The implementation patch has the production-reported baseline as its direct
parent. Its compiled source is the same source previously validated locally;
the later merge of `develop` is not included here.

## Explicitly excluded commits

`934b01a`, `18fe416`, `932c0c3`, `44056ca`, `c1b7f00`, and `acbaf60`.

Their changes to `build.sh`, `collector/coordinator/incr.go`,
`collector/coordinator/utils.go`, `collector/reader/event_reader.go`, and
`scripts/comparison_3x.py` must be absent from the baseline-to-release diff.
Do not revert or rewrite `develop`; keep the release lineage separate.

## Production gates remain open

Git isolation is not proof that the old binary was built from a clean checkout
of its reported commit. Confirm the old image/build provenance separately.
The local candidate used Go 1.26.1 while the production binary reports
Go 1.15.10; release toolchain selection and verification remain unresolved.
Do not promote the earlier dirty local image or its candidate-only Dockerfile
as an approved production artifact.

Actual CFS/Redis binding and replay acceptance, old-checkpoint cutover review,
and an immutable reviewed release image remain required. Any future deployment
must replace only `StatefulSet/mongoshake-critical` and the
`middleware-conf` key `critical-collector.conf`, not the ordinary lane.

This scope record does not deploy an image, alter production configuration,
restart a workload, replay data, or authorize any of those operations.
