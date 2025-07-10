pub mod runtime;
pub mod text;
pub mod utils;
pub mod web;

/// The `PluginRegistrar` is defined by the application and passed to `plugin_entry`. It's used
/// for a plugin module to register itself with the application.
pub trait PluginRegistrar {
    fn register_plugin(&mut self, plugin: Box<dyn Plugin>);
}

/// `Plugin` is implemented by a plugin library for one or more types. As you need additional
/// callbacks, they can be defined here. These are first class Rust trait objects, so you have the
/// full flexibility of that system. The main thing you'll lose access to is generics, but that's
/// expected with a plugin system
pub trait Plugin {
    /// This is a callback routine implemented by the plugin.
    fn callback1(&self);
    /// Callbacks can take arguments and return values
    fn callback2(&self, i: i32) -> i32;
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EdgelinkError {
    #[error("Permission Denied")]
    PermissionDenied,

    #[error("Invalid 'flows.json': {0}")]
    BadFlowsJson(String),

    #[error("Unsupported 'flows.json' format: {0}")]
    UnsupportedFlowsJsonFormat(String),

    #[error("Not supported: {0}")]
    NotSupported(String),

    #[error("Invalid arguments: {0}")]
    BadArgument(&'static str),

    #[error("Task cancelled")]
    TaskCancelled,

    #[error("{0}")]
    InvalidOperation(String),

    #[error("Out of range")]
    OutOfRange,

    #[error("Invalid configuration")]
    Configuration,

    #[error("Timed out")]
    Timeout,

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] crate::Error), // source and Display delegate to anyhow::Error
}

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T, E = anyhow::Error> = anyhow::Result<T, E>;

pub use anyhow::Context as ErrorContext;

impl EdgelinkError {
    pub fn invalid_operation(msg: &str) -> anyhow::Error {
        EdgelinkError::InvalidOperation(msg.into()).into()
    }
}

#[cfg(test)]
mod tests {

    #[ctor::ctor]
    fn initialize_test_logger() {
        let stderr = log4rs::append::console::ConsoleAppender::builder()
            .target(log4rs::append::console::Target::Stdout)
            .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new("[{h({l})}]\t{m}{n}")))
            .build();

        let config = log4rs::Config::builder()
            .appender(log4rs::config::Appender::builder().build("stderr", Box::new(stderr)))
            .build(log4rs::config::Root::builder().appender("stderr").build(log::LevelFilter::Warn))
            .unwrap();

        let _ = log4rs::init_config(config).unwrap();
    }
}
