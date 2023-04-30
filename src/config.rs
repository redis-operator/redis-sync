use std::time;

#[derive(Default)]
pub struct Config{
    pub host: String,
    pub port: u16,
    pub read_timeout: Option<time::Duration>,
    pub write_timeout: Option<time::Duration>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub is_tls_enabled: bool,
    pub is_tls_insecure: bool,
    pub identity: Option<String>,
    pub identity_passwd: Option<String>,
}