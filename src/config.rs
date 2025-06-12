use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, PartialEq)]
pub enum Mode {
    #[serde(rename = "FromStart")]
    FromStart,
    #[serde(rename = "FromCurrent")]
    FromCurrent,
    #[serde(rename = "ProtoBuf")]
    ProtoBuf,
    #[serde(rename = "Error")]
    Error,
}

#[derive(Debug, Deserialize)]
pub struct RedisPubberConfig {
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub max_stream_size: usize,
    pub mode: Mode,
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub exchange: String,
    pub redis_pubber: RedisPubberConfig,
}

impl RedisConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(path)?;
        let config: RedisConfig = serde_yaml::from_str(&contents)?;
        
        // 验证mode
        if let Mode::Error = config.redis_pubber.mode {
            anyhow::bail!("Invalid mode in configuration");
        }
        
        Ok(config)
    }
} 