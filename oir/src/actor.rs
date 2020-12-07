use tokio;
use tokio::sync::mpsc;

pub type ErrorBox = Box<dyn std::error::Error>;

#[derive(Debug)]
pub enum ShutdownReason {
    Shutdown,
    Crashed,
}

#[derive(Debug)]
pub enum SystemMessage {
    Shutdown,
}
