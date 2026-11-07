//! The driver's settings (`config` feature).
//!
//! [`Config`] is a plain serde struct that implements flightrelay's
//! `DriverConfig`. It needs no network code, so a settings form (a
//! WebAssembly UI) can use it with `--no-default-features --features config`.
//! Every field is left out of the serialized form at its default, so a
//! default config serializes to nothing.
//!
//! ```
//! use flightrelay::config::DriverConfig;
//! use threeputt::config::Config;
//!
//! // Blank (the default) finds the device on the network.
//! assert_eq!(Config::default().ip(), Ok(None));
//! let config = Config { address: Some("192.168.1.40".into()) };
//! assert!(config.validate().is_ok());
//! let bad = Config { address: Some("exputt.local".into()) };
//! assert_eq!(bad.validate().unwrap_err().field.as_deref(), Some("address"));
//! ```

use std::net::IpAddr;

use flightrelay::config::{ConfigError, DriverConfig, is_default};
use serde::{Deserialize, Serialize};

/// Settings of the ExPutt driver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The device's IP address. Unset or blank finds the device on the
    /// network by its discovery broadcast; anything else must be an IP
    /// address (the device listens on port 8889).
    #[serde(skip_serializing_if = "is_default")]
    pub address: Option<String>,
}

impl Config {
    /// The device's IP address: `None` to find the device on the network.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] naming `address` for a value that is neither blank
    /// nor an IP address.
    pub fn ip(&self) -> Result<Option<IpAddr>, ConfigError> {
        match self.address.as_deref().map(str::trim).filter(|a| !a.is_empty()) {
            None => Ok(None),
            Some(a) => a.parse().map(Some).map_err(|_| {
                ConfigError::field(
                    "address",
                    format!(
                        "\"{a}\" is not an IP address; leave it blank to find the device on the network"
                    ),
                )
            }),
        }
    }
}

impl DriverConfig for Config {
    fn validate(&self) -> Result<(), ConfigError> {
        self.ip().map(drop)
    }

    /// A new session only when the address it reaches changes: `" 10.0.0.2"`
    /// and `"10.0.0.2"` are the same device, and so are unset and blank.
    fn restart_needed(old: &Self, new: &Self) -> bool {
        old.ip() != new.ip()
    }

    fn example() -> Self {
        Self {
            address: Some(String::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(value: &serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn a_default_config_writes_nothing_and_reads_back() {
        let config = Config::default();
        assert!(config.to_map().is_empty());
        assert_eq!(Config::from_map(serde_json::Map::new()), Ok(config));
    }

    #[test]
    fn an_address_round_trips() {
        let config = Config {
            address: Some("10.77.0.131".into()),
        };
        let written = config.to_map();
        assert_eq!(written.len(), 1);
        assert_eq!(Config::from_map(written), Ok(config.clone()));
        assert_eq!(config.ip(), Ok(Some("10.77.0.131".parse().unwrap())));
    }

    #[test]
    fn blank_or_unset_finds_the_device() {
        for address in [None, Some(""), Some("   ")] {
            let config = Config {
                address: address.map(str::to_owned),
            };
            assert_eq!(config.ip(), Ok(None), "{address:?}");
            assert!(config.validate().is_ok());
        }
        let padded = Config {
            address: Some(" 10.0.0.2 ".into()),
        };
        assert_eq!(padded.ip(), Ok(Some("10.0.0.2".parse().unwrap())));
        let v6 = Config {
            address: Some("fe80::1".into()),
        };
        assert!(v6.validate().is_ok());
    }

    #[test]
    fn an_address_that_is_not_an_ip_names_the_field() {
        for bad in ["exputt.local", "10.0.0.2:8889", "10.0.0"] {
            let e = Config::from_map(map(&serde_json::json!({ "address": bad }))).unwrap_err();
            assert_eq!(e.field.as_deref(), Some("address"), "{bad}: {e}");
            assert!(e.to_string().starts_with("address: "), "{e}");
            assert!(e.message.contains(bad), "{e}");
        }
        // A wrong type is located too.
        let e = Config::from_map(map(&serde_json::json!({ "address": 5 }))).unwrap_err();
        assert_eq!(e.field.as_deref(), Some("address"), "{e}");
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let config = Config::from_map(map(
            &serde_json::json!({ "name": "Putting mat", "address": "" }),
        ));
        assert_eq!(
            config,
            Ok(Config {
                address: Some(String::new())
            })
        );
    }

    #[test]
    fn only_a_new_device_address_restarts() {
        let at = |a: Option<&str>| Config {
            address: a.map(str::to_owned),
        };
        assert!(!Config::restart_needed(&at(None), &at(Some(""))));
        assert!(!Config::restart_needed(
            &at(Some("10.0.0.2")),
            &at(Some(" 10.0.0.2"))
        ));
        assert!(Config::restart_needed(&at(None), &at(Some("10.0.0.2"))));
        assert!(Config::restart_needed(
            &at(Some("10.0.0.2")),
            &at(Some("10.0.0.3"))
        ));
    }

    #[test]
    fn the_example_lists_the_address() {
        assert!(Config::example().to_map().contains_key("address"));
    }
}
