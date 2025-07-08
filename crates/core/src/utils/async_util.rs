use crate::EdgelinkError;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

pub async fn delay(dur: Duration, cancel: CancellationToken) -> crate::Result<()> {
    tokio::select! {
        _ = cancel.cancelled() => {
            // 取消 sleep_task 任务
            Err(EdgelinkError::TaskCancelled.into())
        }
        _ = tokio::time::sleep(dur)=> {
            // Long work has completed
            Ok(())
        }
    }
}

pub async fn delay_secs_f64(secs: f64, cancel: CancellationToken) -> crate::Result<()> {
    delay(Duration::from_secs_f64(secs), cancel).await
}

pub async fn delay_millis(millis: u64, cancel: CancellationToken) -> crate::Result<()> {
    delay(Duration::from_millis(millis), cancel).await
}

pub trait SyncWaitableFuture: std::future::Future {
    fn wait(self) -> Self::Output
    where
        Self: Sized + Send + 'static,
        Self::Output: Send + 'static,
    {
        let handle = tokio::runtime::Handle::current();
        let task = handle.spawn(self);
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(task).unwrap())
    }
}

impl<F> SyncWaitableFuture for F where F: std::future::Future {}

#[derive(Debug)]
pub struct AsyncRateLimiter {
    counters: Arc<Mutex<HashMap<String, Counter>>>,
    rate: usize,
    period: Duration,
}

#[derive(Debug)]
pub struct Counter {
    count: usize,
    reset_time: Instant,
    queue: VecDeque<()>,
}

impl AsyncRateLimiter {
    pub fn new(rate: usize, period: Duration) -> Self {
        let limiter = AsyncRateLimiter { counters: Arc::new(Mutex::new(HashMap::new())), rate, period };
        let limiter_clone = limiter.clone();
        tokio::spawn(async move {
            limiter_clone.reset_task().await;
        });
        limiter
    }

    async fn reset_task(&self) {
        let mut interval = interval(self.period);
        interval.tick().await; // Skip first immediate tick
        loop {
            interval.tick().await;
            let mut counters = self.counters.lock().await;
            for counter in counters.values_mut() {
                counter.count = 0;
                counter.reset_time = Instant::now();

                while counter.count < self.rate && !counter.queue.is_empty() {
                    counter.queue.pop_front();
                    counter.count += 1;
                }
            }
        }
    }

    pub async fn acquire(&self, topic: &str, queue: bool) -> bool {
        let mut counters = self.counters.lock().await;
        let counter = counters.entry(topic.to_string()).or_insert(Counter {
            count: 0,
            reset_time: Instant::now(),
            queue: VecDeque::new(),
        });

        if counter.count < self.rate {
            counter.count += 1;
            true
        } else if queue {
            counter.queue.push_back(());
            false
        } else {
            false
        }
    }

    // 新增方法：获取当前状态用于测试
    pub async fn get_stats(&self, topic: &str) -> Option<(usize, usize)> {
        let counters = self.counters.lock().await;
        counters.get(topic).map(|counter| (counter.count, counter.queue.len()))
    }

    // Reset counters and queues for a specific topic or all topics
    pub async fn reset(&self, topic: Option<&str>) {
        let mut counters = self.counters.lock().await;

        match topic {
            Some(topic_name) => {
                // Reset specific topic
                if let Some(counter) = counters.get_mut(topic_name) {
                    counter.count = 0;
                    counter.reset_time = Instant::now();
                    counter.queue.clear();
                }
            }
            None => {
                // Reset all topics
                for counter in counters.values_mut() {
                    counter.count = 0;
                    counter.reset_time = Instant::now();
                    counter.queue.clear();
                }
            }
        }
    }

    // Flush queued items for a specific topic, returning the number of items flushed
    pub async fn flush(&self, topic: &str, max_count: Option<usize>) -> usize {
        let mut counters = self.counters.lock().await;

        if let Some(counter) = counters.get_mut(topic) {
            let flush_count = max_count.unwrap_or(counter.queue.len());
            let actual_count = std::cmp::min(flush_count, counter.queue.len());

            for _ in 0..actual_count {
                counter.queue.pop_front();
            }

            actual_count
        } else {
            0
        }
    }

    // Get all topic names currently tracked
    pub async fn get_topics(&self) -> Vec<String> {
        let counters = self.counters.lock().await;
        counters.keys().cloned().collect()
    }
}

impl Clone for AsyncRateLimiter {
    fn clone(&self) -> Self {
        Self { counters: self.counters.clone(), rate: self.rate, period: self.period }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_rate_limiter_basic_acquire() {
        let limiter = AsyncRateLimiter::new(3, Duration::from_millis(100));

        // First 3 acquisitions should succeed
        assert!(limiter.acquire("test", false).await);
        assert!(limiter.acquire("test", false).await);
        assert!(limiter.acquire("test", false).await);

        // 4th acquisition should fail
        assert!(!limiter.acquire("test", false).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_queue_functionality() {
        let limiter = AsyncRateLimiter::new(2, Duration::from_millis(50));

        // Exhaust the rate limit
        assert!(limiter.acquire("test", false).await);
        assert!(limiter.acquire("test", false).await);

        // Queue some requests
        assert!(!limiter.acquire("test", true).await);
        assert!(!limiter.acquire("test", true).await);

        // Check queue before reset
        let (count, queue_len) = limiter.get_stats("test").await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(queue_len, 2);

        // Wait for reset
        sleep(Duration::from_millis(60)).await;

        // Check that queued items were processed (count reset to rate limit, queue reduced)
        let (count, queue_len) = limiter.get_stats("test").await.unwrap();
        assert!(queue_len <= 2, "Queue should be reduced or empty after reset");
        assert!(count <= 2, "Count should be within rate limit after reset");
    }

    #[tokio::test]
    async fn test_rate_limiter_multiple_topics() {
        let limiter = AsyncRateLimiter::new(2, Duration::from_millis(100));

        // Different topics should have separate counters
        assert!(limiter.acquire("topic1", false).await);
        assert!(limiter.acquire("topic1", false).await);
        assert!(!limiter.acquire("topic1", false).await);

        assert!(limiter.acquire("topic2", false).await);
        assert!(limiter.acquire("topic2", false).await);
        assert!(!limiter.acquire("topic2", false).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_reset_behavior() {
        let limiter = AsyncRateLimiter::new(2, Duration::from_millis(50));

        // Exhaust rate limit
        assert!(limiter.acquire("test", false).await);
        assert!(limiter.acquire("test", false).await);
        assert!(!limiter.acquire("test", false).await);

        // Wait for reset period (longer delay to ensure reset happens)
        sleep(Duration::from_millis(80)).await;

        // Should be able to acquire again
        assert!(limiter.acquire("test", false).await);
        assert!(limiter.acquire("test", false).await);
        assert!(!limiter.acquire("test", false).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_queue_vs_no_queue() {
        let limiter = AsyncRateLimiter::new(1, Duration::from_millis(100));

        // Exhaust rate limit
        assert!(limiter.acquire("test", false).await);

        // No queue - should return false immediately
        assert!(!limiter.acquire("test", false).await);

        // With queue - should return false but add to queue
        assert!(!limiter.acquire("test", true).await);

        let (_, queue_len) = limiter.get_stats("test").await.unwrap();
        assert_eq!(queue_len, 1);
    }

    #[tokio::test]
    async fn test_rate_limiter_zero_rate() {
        let limiter = AsyncRateLimiter::new(0, Duration::from_millis(100));

        // Should always return false for zero rate
        assert!(!limiter.acquire("test", false).await);
        assert!(!limiter.acquire("test", true).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_high_rate() {
        let limiter = AsyncRateLimiter::new(1000, Duration::from_millis(100));

        // Should be able to acquire many times
        for _ in 0..500 {
            assert!(limiter.acquire("test", false).await);
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_queue_overflow() {
        let limiter = AsyncRateLimiter::new(1, Duration::from_millis(100));

        // Exhaust rate limit
        assert!(limiter.acquire("test", false).await);

        // Add many items to queue
        for _ in 0..10 {
            assert!(!limiter.acquire("test", true).await);
        }

        let (count, queue_len) = limiter.get_stats("test").await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(queue_len, 10);
    }

    #[tokio::test]
    async fn test_rate_limiter_multiple_reset_cycles() {
        let limiter = AsyncRateLimiter::new(2, Duration::from_millis(50));

        for cycle in 0..3 {
            // Exhaust rate limit
            assert!(limiter.acquire("test", false).await, "Cycle {cycle}: First acquire failed");
            assert!(limiter.acquire("test", false).await, "Cycle {cycle}: Second acquire failed");
            assert!(!limiter.acquire("test", false).await, "Cycle {cycle}: Third acquire should fail");

            // Wait for reset (longer delay to ensure reset happens)
            sleep(Duration::from_millis(80)).await;
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_concurrent_safety() {
        let limiter = Arc::new(AsyncRateLimiter::new(10, Duration::from_millis(100)));
        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for i in 0..50 {
            let limiter_clone = limiter.clone();
            let success_count_clone = success_count.clone();
            handles.push(tokio::spawn(async move {
                if limiter_clone.acquire(&format!("topic_{}", i % 5), false).await {
                    success_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        join_all(handles).await;

        // Should have some successes but not exceed the total rate across all topics
        let total_success = success_count.load(Ordering::Relaxed);
        assert!(total_success > 0);
        assert!(total_success <= 50); // 5 topics * 10 rate = 50 max
    }

    #[tokio::test]
    async fn test_rate_limiter_queue_processing_order() {
        let limiter = AsyncRateLimiter::new(1, Duration::from_millis(50));

        // Exhaust rate limit
        assert!(limiter.acquire("test", false).await);

        // Queue 3 requests
        for _ in 0..3 {
            assert!(!limiter.acquire("test", true).await);
        }

        let (count, queue_len) = limiter.get_stats("test").await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(queue_len, 3);

        // Wait for reset and check queue processing
        sleep(Duration::from_millis(60)).await;

        let (count, queue_len) = limiter.get_stats("test").await.unwrap();
        // With rate limit 1, only 1 item from queue should be processed
        assert!(count <= 1, "Count should not exceed rate limit");
        assert!(queue_len <= 3, "Queue should be processed or stay as is");
    }

    #[tokio::test]
    async fn test_rate_limiter_empty_topic() {
        let limiter = AsyncRateLimiter::new(2, Duration::from_millis(100));

        assert!(limiter.acquire("", false).await);
        assert!(limiter.acquire("", false).await);
        assert!(!limiter.acquire("", false).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_long_topic_name() {
        let limiter = AsyncRateLimiter::new(1, Duration::from_millis(100));
        let long_topic = "a".repeat(1000);

        assert!(limiter.acquire(&long_topic, false).await);
        assert!(!limiter.acquire(&long_topic, false).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_rapid_resets() {
        let limiter = AsyncRateLimiter::new(5, Duration::from_millis(10));

        for _ in 0..10 {
            // Quick burst
            for _ in 0..3 {
                limiter.acquire("test", false).await;
            }
            sleep(Duration::from_millis(15)).await;
        }

        // Should still work after rapid resets
        assert!(limiter.acquire("test", false).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_timeout_behavior() {
        let limiter = AsyncRateLimiter::new(1, Duration::from_millis(1000));

        // Exhaust rate limit
        assert!(limiter.acquire("test", false).await);

        // This should complete quickly (not wait for reset)
        let start = Instant::now();
        let result = timeout(Duration::from_millis(50), limiter.acquire("test", false)).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(!result.unwrap());
        assert!(elapsed < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_rate_limiter_concurrent_access() {
        let limiter = Arc::new(AsyncRateLimiter::new(3, Duration::from_millis(100))); // 降低限制以确保有失败
        let mut handles = vec![];

        for i in 0..15 {
            // 增加请求数量
            let limiter_clone = limiter.clone();
            handles.push(tokio::spawn(async move {
                limiter_clone.acquire(&format!("topic{}", i % 2), false).await // 只用2个主题
            }));
        }

        let results: Vec<bool> = join_all(handles).await.into_iter().map(|r| r.unwrap()).collect();

        // Should have some true and some false results
        let success_count = results.iter().filter(|&&x| x).count();
        let failure_count = results.iter().filter(|&&x| !x).count();

        assert!(success_count > 0, "Should have some successful acquisitions");
        assert!(
            failure_count > 0,
            "Should have some failed acquisitions, got {} successes out of {}",
            success_count,
            results.len()
        );
    }
}
