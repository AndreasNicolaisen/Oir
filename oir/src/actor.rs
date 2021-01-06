use tokio;
use tokio::sync::mpsc;

pub type ErrorBox = Box<dyn std::error::Error>;

#[derive(Debug, Copy, Clone)]
pub enum ShutdownReason {
    Shutdown,
    Crashed,
}

#[derive(Debug, Copy, Clone)]
pub enum SystemMessage {
    Shutdown,
}
