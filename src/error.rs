use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum RedisSyncError {
    #[error("disconnected")]
    Disconnect(#[from] io::Error),
}