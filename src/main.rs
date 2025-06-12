use anyhow::Result;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error};
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

mod config;
mod message;

use config::Config;
use message::PeriodMessage;
use redis_publisher::RedisPublisher;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();
    
    // 加载配置
    let config = Config::load()?;
    info!("Loaded config: {:?}", config);
    
    // 创建 Redis 发布者
    let publisher = Arc::new(Mutex::new(RedisPublisher::new(&config.redis).await?));
    
    // 从命令行参数获取文件路径
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        error!("Usage: {} <capnp_message_file>", args[0]);
        std::process::exit(1);
    }
    
    let file_path = PathBuf::from(&args[1]);
    info!("Reading message from file: {:?}", file_path);
    
    // 读取文件内容
    let mut file = File::open(&file_path)?;
    let mut compressed_data = Vec::new();
    file.read_to_end(&mut compressed_data)?;
    info!("Read {} bytes from file", compressed_data.len());
    
    // 解析 Cap'n Proto 消息
    let message = PeriodMessage::from_capnp(&compressed_data, true)?;
    message.print_info();
    
    // 根据配置决定是否转换为 Protocol Buffers
    let serialized = if config.redis.publish_as_protobuf {
        info!("Converting to Protocol Buffers format");
        message.to_protobuf(true)?
    } else {
        info!("Keeping Cap'n Proto format");
        compressed_data
    };
    
    // 发布消息
    let mut publisher = publisher.lock().await;
    publisher.publish(&serialized).await?;
    
    Ok(())
}
