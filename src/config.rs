use std::fs;

use serde::Deserialize;

const CONFIG_PATH: &str = "config.toml";

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub disk: DiskConfig,
    pub network: NetworkConfig,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct DiskConfig {
    pub download_path: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct NetworkConfig {
    pub max_peer_connections: u32,
    pub queue_capacity: u32,
    pub timeout_threshold: u64,
    pub connection_retries: u32,
    pub block_size: u64,
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
                            download_path: "./downloads/".to_string(),
                        },
                        network: NetworkConfig {
                            max_peer_connections: 1,
                            timeout_threshold: 15,
                            connection_retries: 1,
                            block_size: 16384,
                            queue_capacity: 40
                        },
                    }
                );
            }
            Err(e) => panic!("Failed to load config: {}", e),
        }
    }
}
