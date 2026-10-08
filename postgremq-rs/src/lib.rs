#![doc = include_str!("../README.md")]

mod admin;
mod checkout;
mod connection;
mod consumer;
mod delivery;
mod error;
mod handler;
mod keepalive;
mod listener;
mod options;
mod renewal;
mod retry;
mod scheduler;
mod sync;
mod types;

pub use admin::{DlqMessage, MaintenanceCounters, QueueInfo, QueueStatistics};
pub use connection::Connection;
pub use consumer::Consumer;
pub use delivery::Delivery;
pub use error::{Error, ErrorKind, Result};
pub use handler::{HandlerConsumer, HandlerError};
pub use options::{
    ConnectionOptions, ConsumeOptions, PublishOptions, QueueFatalHook, QueueOptions, RetryConfig,
};
pub use types::{Generation, MessageId};

/// Re-exported so applications use the same versions as this crate's API.
pub use serde_json;
/// Re-exported so applications use the same versions as this crate's API.
pub use sqlx;
/// Re-exported so applications use the same versions as this crate's API.
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_types_are_send_and_sync() {
        assert_send_sync::<Connection>();
        assert_send_sync::<Consumer>();
        assert_send_sync::<Delivery>();
        assert_send_sync::<HandlerConsumer>();
        assert_send_sync::<Error>();
    }
}
