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
| Simulation goal | Every node finalizes 50 views with no forks or faults |

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

Each node opens **three independent P2P channels**:

| Channel | Carries | Purpose |
|---------|---------|---------|
| `vote` | Individual `notarize`/`nullify`/`finalize` votes | Routed through the batcher for batch signature verification |
| `certificate` | Assembled `notarization`/`nullification`/`finalization` certs | Fast-pathed to the voter for view advancement |
| `resolver` | Request/response for missing certificates | Used during catch-up when a node missed a view |

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

### Customisation

Edit `examples/network_10_nodes.rs` to experiment:

```rust
const N: u32 = 10;           // change node count
const REQUIRED_VIEWS: u64 = 50;  // change number of views to finalize

// Link parameters — try lossy links:
Link {
    latency: Duration::from_millis(50),  // higher latency
    jitter:  Duration::from_millis(10),  // more jitter
    success_rate: 0.95,                  // 5% message loss
}

// Certifier — try Sometimes for partial certification:
should_certify: Certifier::Sometimes,

// Leader election — try shuffled round-robin:
let elector = RoundRobin::shuffled(b"my-seed");
```
