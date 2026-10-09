use std::sync::Arc;
use std::time::{Duration, Instant};

use edgelink_core::runtime::model::{MsgHandle, Variant};
use tokio::sync::mpsc;

/// These tests intentionally stay short. They are smoke pressure tests for regressions in the
/// message primitives, not long-running capacity or soak tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_lock_contention_remains_bounded() {
    let msg = Arc::new(MsgHandle::with_payload(Variant::from("pressure")));
    let started = Instant::now();
    let mut workers = Vec::new();

    for worker in 0..8 {
        let msg = msg.clone();
        workers.push(tokio::spawn(async move {
            for _ in 0..500 {
                if worker == 0 {
                    let mut guard = msg.write().await;
                    guard.set("worker".to_owned(), Variant::from("writer"));
                } else {
                    let guard = msg.read().await;
                    assert!(guard.contains("payload"));
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for worker in workers {
        worker.await.expect("lock contention worker panicked");
    }

    assert!(started.elapsed() < Duration::from_secs(3), "message lock pressure test exceeded its bound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deep_clone_pressure_completes_without_losing_clones() {
    let payload = Variant::from("x".repeat(4096));
    let msg = Arc::new(MsgHandle::with_payload(payload));
    let started = Instant::now();
    let mut workers = Vec::new();

    for _ in 0..4 {
        let msg = msg.clone();
        workers.push(tokio::spawn(async move {
            let mut clones = Vec::new();
            for _ in 0..500 {
                clones.push(msg.deep_clone(false).await);
            }
            clones.len()
        }));
    }

    let mut clone_count = 0;
    for worker in workers {
        clone_count += worker.await.expect("deep clone worker panicked");
    }

    assert_eq!(clone_count, 2_000);
    assert!(started.elapsed() < Duration::from_secs(3), "deep clone pressure test exceeded its bound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_queue_backpressure_delivers_every_message() {
    const PRODUCERS: usize = 4;
    const MESSAGES_PER_PRODUCER: usize = 1_000;
    let expected = PRODUCERS * MESSAGES_PER_PRODUCER;
    let (tx, mut rx) = mpsc::channel::<MsgHandle>(8);
    let started = Instant::now();

    let consumer = tokio::spawn(async move {
        let mut received = 0;
        while rx.recv().await.is_some() {
            received += 1;
            if received % 128 == 0 {
                tokio::task::yield_now().await;
            }
        }
        received
    });

    let mut producers = Vec::new();
    for _ in 0..PRODUCERS {
        let tx = tx.clone();
        producers.push(tokio::spawn(async move {
            for _ in 0..MESSAGES_PER_PRODUCER {
                tx.send(MsgHandle::default()).await.expect("consumer closed early");
            }
        }));
    }
    drop(tx);

    let result = tokio::time::timeout(Duration::from_secs(3), async {
        for producer in producers {
            producer.await.expect("producer panicked");
        }
        consumer.await.expect("consumer panicked")
    })
    .await
    .expect("bounded queue pressure test timed out");

    assert_eq!(result, expected);
    assert!(started.elapsed() < Duration::from_secs(3), "backpressure test exceeded its bound");
}
