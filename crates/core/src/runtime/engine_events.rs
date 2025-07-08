use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// Engine internal event types.
///
/// This enum defines all possible events that can be emitted by the engine and observed by subscribers.
/// It is used for broadcasting engine lifecycle and flow deployment events, as well as custom user events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineEvent {
    /// Engine started event
    EngineStarted,
    /// Engine stopped event
    EngineStopped,
    /// Engine restart started event
    EngineRestartStarted,
    /// Engine restart completed event
    EngineRestartCompleted,
    /// Debug channel reinitialized event
    DebugChannelReinitialized,
    /// Flow deployment started event
    FlowDeploymentStarted,
    /// Flow deployment completed event
    FlowDeploymentCompleted,
    /// Custom event
    Custom { event_type: String, data: serde_json::Value },
}

/// Engine event bus.
///
/// Provides a publish-subscribe mechanism for engine events. Allows multiple subscribers to receive
/// notifications about engine state changes and custom events in a thread-safe, asynchronous way.
/// Create a new event bus
///
/// # Arguments
///
/// * `capacity` - The maximum number of events that can be buffered in the channel.
///
/// # Returns
///
/// A new `EngineEventBus` instance.
/// Publish an event
///
/// Sends an event to all current subscribers. If there are no subscribers, the event is dropped.
///
/// # Arguments
///
/// * `event` - The `EngineEvent` to publish.
///   Subscribe to events
///
/// Returns a new receiver that will receive all subsequent events published to the bus.
/// Each subscriber receives events independently.
/// Get the current number of subscribers
///
/// Returns the number of active subscribers to the event bus.
/// Provides a default event bus with a buffer capacity of 1000 events.
#[derive(Debug, Clone)]
pub struct EngineEventBus {
    sender: broadcast::Sender<EngineEvent>,
}

impl EngineEventBus {
    /// Create a new event bus
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publish an event
    pub fn publish(&self, event: EngineEvent) {
        match self.sender.send(event.clone()) {
            Ok(subscriber_count) => {
                log::debug!("Published engine event {event:?} to {subscriber_count} subscribers");
            }
            Err(e) => {
                log::warn!("Failed to publish engine event {event:?}: {e}");
            }
        }
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.sender.subscribe()
    }

    /// Get the current number of subscribers
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EngineEventBus {
    fn default() -> Self {
        Self::new(1000)
    }
}
