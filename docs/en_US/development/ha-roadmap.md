# RMQTT HA Cluster Roadmap

Status: draft (fork-only). Written generically so it can later be proposed upstream.

Goal: run RMQTT as a 3-node `cluster-raft` cluster on Kubernetes with no client-visible
interruption on planned events (rolling upgrade, node reboot, pod eviction), automatic
recovery from single-node failures and partitions, and — mid-term — no loss of
acknowledged QoS 1/2 messages. Because much of the implementation will be done by AI agents
under human review, **an automated multi-node test layer comes before any change to the
clustering internals.**

Baseline: `master` @ 0.25.0 (57eb853b).

---

## 1. Current state (summary)

| Area | State |
|---|---|
| Protocol | MQTT 3.1 / 3.1.1 / 5.0; v5 conformance well covered by `mqtt_harness` |
| Clustering | `cluster-broadcast` (every publish to all nodes, no partition handling) and `cluster-raft` (replicated route table + client states, targeted forwarding) |
| Raft | `rmqtt-raft` 0.7.2 over raft-rs, log in `MemStorage` (in-memory) |
| Membership | Static `node_grpc_addrs` / `raft_peer_addrs`; DNS names resolved, no dynamic join/leave |
| Session state | Lives only on the owning node; optional `session-storage` (sled/redis) keyed per node id |
| Retained | `retainer` (ram/sled/redis); cluster sync is best-effort broadcast + peer sync on start |
| k8s | Dockerfiles only (root user, demo certs); no chart on master |
| CI | `fmt`, `clippy`, `cargo test`. **`mqtt_harness` is not run in CI** |

## 2. Gaps

Each gap is written so it can stand alone as an upstream issue.

### G1 — No graceful shutdown (high)
`rmqtt-bin/src/server.rs:202-205`: on SIGTERM the process sleeps 100 ms and calls
`process::exit(0)` (`@TODO ... Hook when exiting`). No DISCONNECT to clients (v5 reason
`0x8B Server shutting down` / `0x9C Use another server`), no storage flush, no raft
leadership transfer or leave, no session handoff. In Kubernetes every rollout or node drain
is effectively a crash.

Proposed: a shutdown sequence driven by a configurable grace period —
stop accepting → mark not-ready → transfer raft leadership if leader → hand off / persist
sessions (see §4 Phase 1) → DISCONNECT clients → flush storage plugins → exit. Expose a
`BeforeShutdown` hook for plugins.

### G2 — Dead-node state is never cleaned up in cluster-raft (high)
There is no node-down handling in `rmqtt-plugins/rmqtt-cluster-raft/src/`: routes and
`client_states` owned by a node that disappears stay in the replicated state machine.
Publishes keep being forwarded to the dead node until clients resubscribe; entries for
clients that never return live forever. `Raft::leave()` exists in `rmqtt-raft` but is never
called.

Proposed: leader-driven reaping of routes/client states for nodes that are unreachable longer
than a configurable timeout (and immediately on graceful leave); metric + log per reap.

### G3 — Inter-node forwards are fire-and-forget (medium-high)
`rmqtt-plugins/rmqtt-cluster-raft/src/shared.rs:438,509,524` (`//@TODO messasge dropped`): a
failed gRPC forward is logged and dropped. No retry, no `message_dropped` hook, no metric, no
backpressure (`shared.rs:513`). QoS 1/2 messages can be silently lost between nodes.

Proposed (incremental): (a) fire the `message_dropped` hook + metric, (b) bounded retry with
backoff for QoS ≥ 1, (c) flow control per peer.

### G4 — Raft log is in memory; cold-start split brain (medium)
`rmqtt-raft-0.7.2/src/raft_node.rs:422` uses `MemStorage`. A full-cluster restart loses the
route table and client states (clean-session clients recover by resubscribing; persistent
sessions depend on session-storage rebuild). With `leader_id = 0` nodes that cannot find a
leader elect themselves (`cluster-raft/src/lib.rs:226-231`), so a simultaneous cold start can
form several single-node clusters. With `leader_id = N` startup is ordered and non-leaders
give up after ~30 s.

Proposed: document the safe k8s bootstrap (`leader_id` + `OrderedReady`) now; later either
persist the raft log (sled) or make bootstrap quorum-aware (`expected_nodes`).

### G5 — Static membership (low for fixed-size clusters)
gRPC clients are built once from the static list; resizing means editing config and rolling
every pod. Acceptable for fixed 3-node deployments; document it.

### G6 — Retained messages replicated best-effort (medium)
`retain_set_broadcast` + `sync_retains_from_peers` 2 s after start are fire-and-forget. A node
that misses a broadcast diverges until restart. Proposed: tests first (§3), then
anti-entropy or raft-backed retained store if the tests show divergence.

### G7 — Cluster behaviour is effectively untested in CI (high; prerequisite)
- `mqtt_harness` (~290 functional + chaos cases) is manual; CI runs only `cargo test`.
- Zero unit tests in `cluster-raft`, `cluster-broadcast`, `session-storage`, `acl`,
  `auth-http`, `auth-jwt`, bridges.
- Cluster cases only restart the *same* node. No permanent node loss, partition, leader kill
  under load, cross-node shared subscriptions or stale-route cases.
- `rmqtt-test/src/chaos.rs` `ChaosController` is dead code; `chaos_qos1_reliability` injects
  nothing. Cluster fixture (`cluster_session_restart.rs:98-210`) uses fixed ports and shared
  sled dirs, so tests cannot run in parallel; `stop()` is SIGKILL, there is no SIGTERM path.
- Paho interop runs are manual; benchmarks date from v0.3.0.

### G8 — Container image not production-ready (low-medium)
Runs as root with demo certs baked in; no Helm chart upstream.

### Also noted (lower priority)
- Session-storage writes offline messages asynchronously and drops them when the exec queue is
  full (`messages_offline_saves_dropped`); in-flight messages are persisted only at disconnect
  (`rmqtt/src/session.rs:316-319`).
- `rmqtt/src/session.rs:1633`, `session.rs:2334`, `inflight.rs:352`: open `@TODO`s around
  failed deliveries and expiry.
- `unreachable!()` if the raft mailbox is unset (`cluster-raft/src/lib.rs:292`, `router.rs:108`).
- `cluster-broadcast` maps a failed kick to `OfflineSession::NotExist` (`shared.rs:119-122`);
  `cluster-raft` does the same (`shared.rs:207,214`).

## 3. Test strategy

Extend the existing `mqtt_harness` (keeps upstream conventions; scenarios are already
selectable with `--suites` / `--test`). Layers follow `docs/en_US/development/testing.md` and
add three:

| Layer | Content | Runs | Notes |
|---|---|---|---|
| Unit | Router, trie, session, raft state machine (`Store::apply`) | every PR | Add property tests for route add/remove/reap |
| Harness functional | Existing v3/v311/v5 suites | every PR (new) | Needs `rmqttd` release build in CI |
| Interop | paho.mqtt.testing `client_test.py` / `client_test5.py` | every PR (new) | ~1 day to automate |
| **Cluster fault** | Multi-process `rmqttd` on localhost + fault proxy + loss checker | PR subset (<10 min), full nightly with repeats | Main layer for clustering work |
| **k8s e2e** | kind + Helm chart + Cilium | nightly; smoke on chart/cluster PRs | Probes, PDB, DNS, PVC, grace period |
| **Performance** | `rmqtt-bench` / emqtt-bench reproducing README scenarios | nightly on GitHub runners (coarse), self-hosted later | Scenarios, not absolute numbers, on shared runners |

### Cluster fault layer — building blocks
1. **`Cluster` fixture** — generated configs, dynamic ports, temp dir per test, per-node
   `sigterm()` / `sigkill()` / `sigstop()` / `sigcont()` / `restart()`, leader discovery via the
   HTTP API, readiness via `/api/v1/health/check/{id}`.
2. **Fault proxy** — tokio TCP proxy on every inter-node link (gRPC per source→destination;
   raft per destination because the advertised raft address is shared in the peer table,
   `rmqtt-raft-0.7.2/src/raft_node.rs:1136`). Modes: pass, latency, drop, **hang** (keep
   socket open, stop forwarding — needed for HTTP/2 channels), reset. Symmetric single-node
   partitions need a small test-only raft address override (candidate upstream change) or
   SIGSTOP as a stand-in.
3. **Loss / duplicate / order checker** — every payload carries `(publisher, seq)`; after the
   run compute lost / duplicated / reordered per publisher→subscriber and assert per QoS.
4. Revive `ChaosController` on the client↔broker path using the same proxy.

### Scenario placement

| Scenario | Cluster fault PR | Cluster fault nightly | k8s e2e |
|---|---|---|---|
| SIGKILL a node during publish (QoS 1 loss = 0 for clients reconnecting) | ✓ | repeats | `delete --grace-period=0` |
| Leader kill under publish load | ✓ | repeats | |
| Rolling SIGTERM restart under load (no interruption; needs G1) | ✓ | | `rollout restart` |
| Retained messages survive full restart | ✓ | | with PVC |
| Session-storage across same-node restart | ✓ | | with PVC |
| Isolate one node, heal automatically | | ✓ | NetworkPolicy |
| 2 of 3 nodes down → unavailable by design → recovery | | ✓ | ✓ |
| Permanent node loss: stale routes reaped (G2) | | ✓ | |
| Cross-node shared subscriptions | | ✓ | |
| Soak / stress variants | | ✓ | |

Deterministic simulation (turmoil / madsim) was evaluated and rejected for now: cluster-raft
runs its own thread + runtime, `rmqtt-raft` binds real sockets via socket2, sled and ~50
wall-clock call sites would all need shims (weeks of work plus forked dependencies).
Revisit, scoped to `rmqtt-raft`, only if the cluster fault layer produces hard-to-reproduce
raft bugs.

## 4. Session HA phases

Availability target: 2 of 3 nodes = full service; 1 of 3 = unavailable by design (raft has no
quorum; a local-only mode would risk split brain), with a documented and tested recovery.

- **Phase 1a — Handoff on graceful drain (builds on G1).** Before exit, transfer each session
  (subscriptions, queue, in-flight) to a peer through the existing kick/transfer path, then
  DISCONNECT with `Use another server`. Covers rollouts, reboots, evictions.
- **Phase 1b — Crash recovery from per-pod storage.** StatefulSet pod returns (possibly on
  another node, same replicated PVC) and rebuilds its sessions from `session-storage`. Needs a
  defined merge rule when the client already reconnected elsewhere with a fresh session.
- **Phase 2 — Durability fixes.** G3 retries, synchronous-or-bounded offline persists,
  persisting in-flight while connected (configurable; measure cost).
- **Phase 3 — Replicated session state (mid-term).** Each session's queue and in-flight
  window replicated to a follower node so an unplanned node loss keeps acknowledged QoS 1/2
  messages. Largest item; design with maintainers first. Must not degrade steady-state
  publish latency noticeably — benchmark gate required.

Note on semantics: MQTT persistent sessions queue messages for a *subscribed* client while it
is offline. There are no offsets and no replay for new consumers (unlike Kafka / RabbitMQ
streams). Offline queues need hard per-session limits (count and bytes) to avoid write
amplification on high-rate topics.

## 5. Candidate upstream issues (standalone, in suggested order)

1. Run `mqtt_harness` (functional + interop) in CI; dynamic ports + temp dirs for the cluster
   fixture.
2. Graceful shutdown sequence (G1).
3. Reap routes / client states of departed nodes in cluster-raft (G2).
4. `message_dropped` hook + metric + retry for failed inter-node forwards (G3).
5. Document safe k8s bootstrap; discuss raft log persistence (G4).
6. Non-root image without baked-in demo certs; offer the Helm chart (G8).
7. Test-only raft advertise-address override to enable partition testing.
8. Retained-message anti-entropy, if tests show divergence (G6).
9. Discussion: replicated session state (Phase 3).

Open issues/discussions before PRs; each must stand on its own merit.
