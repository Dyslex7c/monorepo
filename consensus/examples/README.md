# Simplex Consensus Examples

## `network_10_nodes`

A self-contained simulation of **10 validator nodes** running the Simplex BFT consensus
protocol inside a single process using the Commonware deterministic runtime and simulated P2P network.

### What it demonstrates

| Property | Value |
|----------|-------|
| Validators | 10 |
| Signing scheme | Ed25519 (attributable) |
| Leader election | Round-robin |
| Fault tolerance | f < n/3 → tolerates up to **3** Byzantine faults |
| Finality latency | 3 network hops |
| Simulation goal | Persistent — all nodes finalize views continuously with no forks or faults |

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                 deterministic::Runner                   │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Simulated Network (P2P)             │   │
│  │  ┌──────────┐  10ms  ┌──────────┐                │   │
│  │  │  Node 0  │◄──────►│  Node 1  │  · · ·         │   │
│  │  └──────────┘        └──────────┘                │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  Per node:                                              │
│  ┌────────────────────────────────────────────────┐     │
│  │  Application (propose/verify/certify blocks)   │     │
│  │  ──────────────────────────────────────────    │     │
│  │  Engine                                        │     │
│  │    ├── Voter    (vote aggregation + state)     │     │
│  │    ├── Batcher  (batch sig verification)       │     │
│  │    └── Resolver (missing cert fetch)           │     │
│  │  Reporter (observability / assertion)          │     │
│  └────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────┘
```

### Network channels per node

Each node opens **four independent P2P channels**:

| Channel | ID | Carries | Purpose |
|---------|----|---------|---------|
| `vote` | 0 | Individual `notarize`/`nullify`/`finalize` votes | Routed through the batcher for batch signature verification |
| `certificate` | 1 | Assembled `notarization`/`nullification`/`finalization` certs | Fast-pathed to the voter for view advancement |
| `resolver` | 2 | Request/response for missing certificates | Used during catch-up when a node missed a view |
| `da_sampling` | 3 | *(reserved — currently idle)* | Placeholder for FRIDA DA chunk delivery and sampling messages |

> **Note on channel 3:** The DA sampling channel is registered against the P2P oracle at
> startup but carries no traffic yet. It is reserved now because channel IDs are
> positionally stable — adding a new ID after the network is running requires a
> coordinated restart of every node. When FRIDA is integrated, this channel will carry
> `ChunkDelivery`, `SampleRequest`, and `SampleResponse` messages routed to and from
> the DA actor.

### Simplex protocol flow

```
View v:

1. Leader L   →  notarize(block, v)           [hop 1]
2. Validators →  notarize(block, v)           [hop 2 → 2f+1 votes = notarization cert]
3. Validators →  certify (app check)
4. Validators →  finalize(block, v)           [hop 3 → 2f+1 votes = finalization cert]
5. All nodes commit block ✓

Timeout path (leader absent):
   After 3Δ: nullify(v) fires → nullification cert → advance to v+1
```

### Running

```bash
# From the monorepo root:
cargo run -p commonware-consensus --example network_10_nodes --features mocks

# Or from the consensus/ directory:
cargo run --example network_10_nodes --features mocks
```

Press `Ctrl+C` to stop the simulation. Progress is logged every 10 finalized views per node.

### Customisation

Edit `examples/network_10_nodes.rs` to experiment:

```rust
const N: u32 = 10;            // change node count
const LOG_EVERY_VIEWS: u64 = 10;  // how often each node logs its finalized view count

// Link parameters — try lossy links:
Link {
    latency: Duration::from_millis(50),  // higher latency
    jitter:  Duration::from_millis(10),  // more jitter
    success_rate: 0.95,                  // 5% message loss
}

// Certifier — try Sometimes for partial certification:
let certifier = Certifier::Sometimes;

// Leader election — try shuffled round-robin:
let elector = RoundRobin::shuffled(b"my-seed");
```

---

## FRIDA Integration Notes

This network is structured as a preparation point for integrating
[FRIDA](https://github.com/NethermindEth/Frida-poc) — a FRI-based data availability
sampling scheme — in place of the current mock application layer. Three deliberate
hooks have been left in the source to make that integration straightforward:

### 1. `ActiveRelay` type alias

```rust
// examples/network_10_nodes.rs
type ActiveRelay = Relay<Sha256Digest, PublicKey>;
```

When integrating FRIDA, change this alias to `FridaChunkRelay<...>`. The relay is
instantiated in one place only, so no other site in the file needs to change.

### 2. DA sampling channel (channel ID 3)

Already registered. During FRIDA integration, replace the `da_channels.push(da_net)`
line in the node startup loop with a `DaActor::new(..., da_net, ...).start()` call.
The actor will handle incoming chunk delivery and respond to sampling requests using
FRIDA's `verify()` API.

### 3. `certifier` variable

```rust
// Currently:
let certifier = Certifier::Always;

// After FRIDA integration:
let certifier = Certifier::DaVerified(da_actor_handle);
```

The certifier is declared as a named local variable once per node. Changing it
here — and threading in the DA actor handle — is the only edit needed to gate
block certification on successful FRI proof verification.

### Remaining work for full FRIDA integration

The following components still need to be built before the integration is complete.
They live outside this file and are not part of the consensus engine itself:

| Component | Description |
|-----------|-------------|
| `DaBlock` | Structured block type carrying a `ProverCommitment<H>` from FRIDA alongside transactions |
| Chunk assignment function | Deterministic map from `(validator, num_validators, num_chunks)` → assigned query positions (mirrors DeFRIDA benchmark logic) |
| Chunk dispatcher | Replaces `ActiveRelay`; calls `open(positions)` per validator and routes `FridaProof` + evaluations over channel 3 |
| `DaActor` | Wraps FRIDA's `verify()`; holds per-node chunk store; emits `DaAttestation` on success |
| DA attestation aggregator | Collects signed attestations; gates DA-availability flag at 2f+1 threshold |
| Persistent chunk store | Disk-backed store (RocksDB / sled) keyed by `(block_hash, chunk_position)` |