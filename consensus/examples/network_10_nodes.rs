//! # 10-Node Simplex Consensus Network: Persistent Simulation
//!
//! Spins up 10 BFT validators running Simplex Consensus and keeps them running
//!
//! The network lives entirely in-process using Commonware's deterministic runtime
//! and simulated P2P layer. Simulated time advances automatically, so timeouts,
//! leader elections, and view changes all fire correctly, no real wall-clock
//! delay is needed.
//!
//!
//! With n=10 nodes the protocol tolerates up to f=3 Byzantine faults.

use commonware_consensus::{
    simplex::{
        Config, Engine, ForwardingPolicy,
        elector::RoundRobin,
        mocks::{
            application::{Application, Certifier, Config as AppConfig},
            relay::Relay,
            reporter::{Config as ReporterConfig, Reporter},
        },
        scheme::ed25519,
    },
    types::{Epoch, View, ViewDelta},
    Monitor,
};
use commonware_cryptography::{
    certificate::mocks::Fixture,
    ed25519::PublicKey,
    sha256::Digest as Sha256Digest,
    Sha256,
};
use commonware_p2p::simulated::{Config as NetworkConfig, Link, Network, Oracle};
use commonware_parallel::Sequential;
use commonware_runtime::{
    buffer::paged::CacheRef,
    deterministic,
    Metrics, Quota, Runner, Spawner,
};
use commonware_utils::{NZUsize, NZU16};
use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Duration,
};
use tracing::info;

// ─── Configuration ────────────────────────────────────────────────────────────

const N: u32 = 10;
const PAGE_SIZE: NonZeroU16 = NZU16!(1024);
const PAGE_CACHE_SIZE: NonZeroUsize = NZUsize!(10);
const MSG_QUOTA: Quota = Quota::per_second(NonZeroU32::MAX);

// How often (in finalized views) each node logs its progress.
const LOG_EVERY_VIEWS: u64 = 10;

// ─── Type aliases ─────────────────────────────────────────────────────────────

type ActiveRelay = Relay<Sha256Digest, PublicKey>;

// ─── Network helpers ──────────────────────────────────────────────────────────

async fn open_channels(
    oracle: &mut Oracle<PublicKey, deterministic::Context>,
    validator: PublicKey,
) -> (
    (
        commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
        commonware_p2p::simulated::Receiver<PublicKey>,
    ),
    (
        commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
        commonware_p2p::simulated::Receiver<PublicKey>,
    ),
    (
        commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
        commonware_p2p::simulated::Receiver<PublicKey>,
    ),
    // Channel 3: DA sampling (reserved for FRIDA integration).
    (
        commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
        commonware_p2p::simulated::Receiver<PublicKey>,
    ),
) {
    let ctrl        = oracle.control(validator);
    let votes       = ctrl.register(0, MSG_QUOTA).await.unwrap();
    let certs       = ctrl.register(1, MSG_QUOTA).await.unwrap();
    let resolver    = ctrl.register(2, MSG_QUOTA).await.unwrap();
    let da_sampling = ctrl.register(3, MSG_QUOTA).await.unwrap();
    (votes, certs, resolver, da_sampling)
}

async fn register_all(
    oracle: &mut Oracle<PublicKey, deterministic::Context>,
    validators: &[PublicKey],
) -> HashMap<
    PublicKey,
    (
        (
            commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
            commonware_p2p::simulated::Receiver<PublicKey>,
        ),
        (
            commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
            commonware_p2p::simulated::Receiver<PublicKey>,
        ),
        (
            commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
            commonware_p2p::simulated::Receiver<PublicKey>,
        ),
        (
            commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
            commonware_p2p::simulated::Receiver<PublicKey>,
        ),
    ),
> {
    let mut map = HashMap::new();
    for v in validators {
        map.insert(v.clone(), open_channels(oracle, v.clone()).await);
    }
    map
}

async fn link_all_pairs(
    oracle: &mut Oracle<PublicKey, deterministic::Context>,
    validators: &[PublicKey],
    link: Link,
) {
    for v1 in validators {
        for v2 in validators {
            if v1 == v2 { continue; }
            oracle.add_link(v1.clone(), v2.clone(), link.clone()).await.unwrap();
        }
    }
}

// ─── main ─────────────────────────────────────────────────────────────────────

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    ctrlc::set_handler(|| {
        println!("\nCtrl+C received, shutting down the network.");
        std::process::exit(0);
    })
    .expect("failed to set Ctrl+C handler");

    info!("starting {N}-node Simplex consensus network (press Ctrl+C to stop)");

    // `Runner::new` with no timeout runs the simulation indefinitely.
    // Simulated time advances automatically as futures yield, so protocol
    // timers (leader timeout, nullify, etc.) fire at the right simulated moments.
    let executor = deterministic::Runner::new(
        deterministic::Config::new().with_timeout(None),
    );

    executor.start(|mut context| async move {
        // ── Simulated network ─────────────────────────────────────────────
        let (network, mut oracle) = Network::new(
            context.with_label("network"),
            NetworkConfig {
                max_size: 1024 * 1024,
                disconnect_on_block: true,
                tracked_peer_sets: None,
            },
        );
        network.start();

        // ── Key material ─────────────────────────────────────────────────
        let namespace = b"simplex-10-nodes";
        let Fixture { participants, schemes, .. } =
            ed25519::fixture(&mut context, namespace, N);

        info!("generated {N} Ed25519 key pairs");

        // ── Full-mesh topology ────────────────────────────────────────────
        // 10ms latency, 1ms jitter, lossless.
        // Tune these to simulate different network conditions.
        let mut registrations = register_all(&mut oracle, &participants).await;
        link_all_pairs(
            &mut oracle,
            &participants,
            Link {
                latency: Duration::from_millis(10),
                jitter:  Duration::from_millis(1),
                success_rate: 1.0,
            },
        ).await;
        info!("full-mesh topology established (10ms latency, lossless)");

        // ── Shared block relay ────────────────────────────────────────────
        let relay: Arc<ActiveRelay> = Arc::new(Relay::new());

        // ── Leader election ───────────────────────────────────────────────
        let elector: RoundRobin<Sha256> = RoundRobin::default();

        // ── Start all nodes ───────────────────────────────────────────────
        let mut reporters     = Vec::new();
        let mut engine_handles = Vec::new();
        // Collected DA sampling channels, one per node.
        let mut da_channels: Vec<(
            commonware_p2p::simulated::Sender<PublicKey, deterministic::Context>,
            commonware_p2p::simulated::Receiver<PublicKey>,
        )> = Vec::new();

        for (idx, validator) in participants.iter().enumerate() {
            let ctx = context.with_label(&format!("node_{idx}"));

            let reporter = Reporter::new(
                ctx.with_label("reporter"),
                ReporterConfig {
                    participants: participants.clone().try_into().unwrap(),
                    scheme: schemes[idx].clone(),
                    elector: elector.clone(),
                },
            );
            reporters.push(reporter.clone());

            let certifier = Certifier::Always;

            let (app_actor, app_mailbox) = Application::new(
                ctx.with_label("application"),
                AppConfig {
                    hasher: Sha256::default(),
                    relay: relay.clone(),
                    me: validator.clone(),
                    propose_latency: (10.0, 5.0),
                    verify_latency:  (10.0, 5.0),
                    certify_latency: (10.0, 5.0),
                    should_certify: certifier,
                },
            );
            app_actor.start();

            let blocker = oracle.control(validator.clone());
            let cfg = Config {
                scheme: schemes[idx].clone(),
                elector: elector.clone(),
                blocker,
                automaton: app_mailbox.clone(),
                relay:     app_mailbox.clone(),
                reporter:  reporter.clone(),
                strategy:  Sequential,
                partition: validator.to_string(),
                mailbox_size: 1024,
                epoch: Epoch::new(1),
                leader_timeout:        Duration::from_secs(1),
                certification_timeout: Duration::from_secs(2),
                timeout_retry:         Duration::from_secs(10),
                fetch_timeout:         Duration::from_secs(1),
                fetch_concurrent: 4,
                activity_timeout: ViewDelta::new(10),
                skip_timeout:     ViewDelta::new(5),
                replay_buffer: NZUsize!(1024 * 1024),
                write_buffer:  NZUsize!(1024 * 1024),
                page_cache: CacheRef::from_pooler(&ctx, PAGE_SIZE, PAGE_CACHE_SIZE),
                forwarding: ForwardingPolicy::Disabled,
            };

            let engine = Engine::new(ctx.with_label("engine"), cfg);
            let (vote_net, cert_net, resolver_net, da_net) = registrations
                .remove(validator)
                .expect("validator should have been registered");

            engine_handles.push(engine.start(vote_net, cert_net, resolver_net));

            da_channels.push(da_net);
        }

        info!("all {N} engines running, network is live");

        // ── Progress monitor runs forever ──────────────────────────────
        // Spawn one monitoring task per node. Each task logs whenever its node
        // crosses a LOG_EVERY_VIEWS boundary, then waits for the next update.
        // Because these tasks never complete, the simulation runs indefinitely.
        let mut monitor_handles = Vec::new();

        for (idx, reporter) in reporters.iter_mut().enumerate() {
            let (mut latest, mut monitor): (View, _) = reporter.subscribe().await;

            let handle = context
                .with_label(&format!("monitor_{idx}"))
                .spawn(move |_| async move {
                    let mut last_logged = 0u64;
                    loop {
                        // Log on the current value first, then wait for the next update.
                        let v = latest.get();
                        if v >= last_logged + LOG_EVERY_VIEWS {
                            info!("node {idx:2}  latest_finalized={v}");
                            last_logged = v;
                        }

                        // Block until the next finalization update.
                        latest = match monitor.recv().await {
                            Some(v) => v,
                            None => break, // reporter dropped, engine stopped
                        };
                    }
                });

            monitor_handles.push(handle);
        }

        // Block the top-level future forever.
        // The OS Ctrl+C signal (handled above via ctrlc::set_handler) will
        // call process::exit(0), cleanly terminating the whole process.
        std::future::pending::<()>().await;

        // Unreachable, here only to keep the handles alive until exit.
        drop(engine_handles);
        drop(monitor_handles);
        drop(da_channels);
    });
}