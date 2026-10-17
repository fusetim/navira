use serde::{Deserialize, Serialize};

/// Configuration for the navira-store server
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Config {
    /// The port on which the navira-store server will listen for incoming HTTP requests
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// The list of IP addresses on which the navira-store server will bind for incoming HTTP requests
    #[serde(default = "default_http_bind_address")]
    pub http_bind_address: Vec<String>,

    /// Path to the identity key for the navira-store server, used for signing and verifying messages
    #[serde(default = "default_identity_key_path")]
    pub identity_key_path: String,

    /// Multiaddr to announce to the IPNI index for this navira-store server
    #[serde(default)]
    pub annouce_address: Vec<String>,
}

const fn default_http_port() -> u16 {
    8080
}

fn default_http_bind_address() -> Vec<String> {
    vec!["0.0.0.0".into(), "::".into()]
}

fn default_identity_key_path() -> String {
    // Inside the config directory, we will store the identity key in a file named "navira.keys"
    "./navira.keys".into()
}

impl Config {
    /// Validate the configuration, ensuring that all required fields are set and valid.
    pub fn validate(&self) -> Result<(), String> {
        if self.annouce_address.is_empty() {
            return Err("announce_address must not be empty".into());
        }
        Ok(())
    }
}
