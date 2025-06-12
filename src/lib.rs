use anyhow::Result;
use redis::{Client, AsyncCommands};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error};
use prost::Message;

mod config;
mod message;
mod proto;

// Cap'n Proto generated code
pub mod period_capnp {
    include!(concat!(env!("OUT_DIR"), "/period_capnp.rs"));
}

pub use config::{RedisConfig, Mode};
pub use message::PeriodMessage;
pub use proto::message_old;

// 嵌入Lua脚本
const PUSH_MSG_SCRIPT: &str = r#"
    -- KEYS[1]: Stream名称 如 "mystream"
    -- ARGV[1]: 新消息的 key 格式 "timestamp-period" 直接作为Stream ID
    -- ARGV[2]: 新消息的 info_count
    -- ARGV[3]: 新消息的 msg_content

    local stream = KEYS[1]
    local new_id = ARGV[1]           -- 格式: "tp-period" (如 "bbb-100")
    local new_info_count = tonumber(ARGV[2])
    local new_msg_content = ARGV[3]

    -- 1. 提取 period (如 "100")
    local dash_pos = string.find(new_id, "-")
    if not dash_pos then
        return {err = "INVALID_KEY_FORMAT"}
    end
    local new_period = string.sub(new_id, dash_pos + 1)

    -- 2. 查找相同 period 的消息
    local msgs = redis.call("XREVRANGE", stream, "+", "-", "COUNT", 10)  -- 查最新 10 条
    local max_info_count = 0
    local msg_to_delete = nil

    for _, msg in ipairs(msgs) do
        local fields = msg[2]
        local msg_key = fields["key"]  -- 使用字段名作为键
        if msg_key then
            local msg_dash_pos = string.find(msg_key, "-")
            if msg_dash_pos then
                local msg_period = string.sub(msg_key, msg_dash_pos + 1)
                if msg_period == new_period then
                    local current_count = tonumber(fields["info_count"]) or 0
                    if current_count > max_info_count then
                        max_info_count = current_count
                        msg_to_delete = msg[1]  -- 记录要删除的消息 ID
                    end
                end
            end
        end
    end

    -- 3. 判断是否要更新
    if new_info_count > max_info_count then
        -- 删除旧消息（如果存在）
        if msg_to_delete then
            redis.call("XDEL", stream, msg_to_delete)
        end
        -- 写入新消息
        redis.call("XADD", stream, "*",
            "key", new_id,
            "info_count", new_info_count,
            "msg_content", new_msg_content
        )
        return {ok = "UPDATED"}
    elseif new_info_count == max_info_count then
        return {ok = "EQUAL"}
    else
        return {ok = "SKIPPED"}
    end
"#;

#[derive(Debug)]
pub struct MktArchiveMsg {
    pub key: String,
    pub info_count: i32,
    pub msg_content: String,
}

impl MktArchiveMsg {
    pub fn new(period: i64, post_ts: i64, info_count: i32, msg_content: String) -> Self {
        Self {
            key: format!("{}-{}", post_ts, period),
            info_count,
            msg_content,
        }
    }
}

pub struct RedisStreamMktPubber {
    client: Arc<Client>,
    last_future: Mutex<Option<tokio::task::JoinHandle<()>>>,
    config: RedisConfig,
}

impl RedisStreamMktPubber {
    pub fn new(cfg_path: &str) -> Result<Self> {
        let config = RedisConfig::from_file(cfg_path)?;
        let client = Client::open(format!(
            "redis://{}:{}@{}:{}/",
            config.redis_pubber.username,
            config.redis_pubber.password,
            config.redis_pubber.host,
            config.redis_pubber.port
        ))?;

        Ok(Self {
            client: Arc::new(client),
            last_future: Mutex::new(None),
            config,
        })
    }

    pub async fn publish(&self, msg: MktArchiveMsg) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        
        let keys = vec![self.config.exchange.clone()];
        let args = vec![
            msg.key,
            msg.info_count.to_string(),
            msg.msg_content,
        ];

        let result: Option<String> = redis::cmd("EVAL")
            .arg(PUSH_MSG_SCRIPT)
            .arg(keys.len())
            .arg(keys)
            .arg(args)
            .query_async(&mut conn)
            .await?;

        if let Some(result) = result {
            info!("Message published to stream {} with result: {}", self.config.exchange, result);
        } else {
            info!("Message not published to stream {}", self.config.exchange);
        }

        Ok(())
    }

    pub async fn publish_async(&self, msg: MktArchiveMsg) -> Result<()> {
        // 等待之前的异步操作完成
        let mut last_future = self.last_future.lock().await;
        if let Some(future) = last_future.take() {
            if let Err(e) = future.await {
                error!("Previous async operation failed: {}", e);
            }
        }

        let client = self.client.clone();
        let stream = self.config.exchange.clone();
        
        // 启动新的异步操作
        let handle = tokio::spawn(async move {
            if let Err(e) = async {
                let mut conn = client.get_async_connection().await?;
                
                let keys = vec![stream.clone()];
                let args = vec![
                    msg.key,
                    msg.info_count.to_string(),
                    msg.msg_content,
                ];

                let result: Option<String> = redis::cmd("EVAL")
                    .arg(PUSH_MSG_SCRIPT)
                    .arg(keys.len())
                    .arg(keys)
                    .arg(args)
                    .query_async(&mut conn)
                    .await?;

                if let Some(result) = result {
                    info!("Message published asynchronously to stream {} with result: {}", stream, result);
                } else {
                    info!("Message not published asynchronously to stream {}", stream);
                }

                Ok::<(), anyhow::Error>(())
            }.await {
                error!("Failed to publish message asynchronously: {}", e);
            }
        });

        *last_future = Some(handle);
        Ok(())
    }
} 