use std::fs;

use serde::Deserialize;

const CONFIG_PATH: &str = "config.toml";

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub disk: DiskConfig,
    pub p2p: P2pConfig,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct DiskConfig {
    pub download_path: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct P2pConfig {
    pub max_peer_connections: u32,
    pub timeout_duration: u64,
    pub connection_retries: u32,
    pub piece_standard_size: u64,
}

impl Config {
    pub fn load() -> Result<Config, Box<dyn std::error::Error>> {
        let toml_str = fs::read_to_string(CONFIG_PATH)?;
        let config: Config = toml::de::from_str(&toml_str)?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() {
        let result = Config::load();

        // Step 5: Assert that the loaded configuration is correct
        match result {
            Ok(config) => {
                assert_eq!(
                    config,
                    Config {
                        disk: DiskConfig {
                            download_path: "./downloads".to_string()
                        },
                        p2p: P2pConfig {
                            max_peer_connections: 2,
                            timeout_duration: 3,
                            connection_retries: 2,
                            piece_standard_size: 16384
                        },
                    }
                );
            }
            Err(e) => panic!("Failed to load config: {}", e),
        }
    }
}
