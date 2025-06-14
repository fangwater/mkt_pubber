use anyhow::Result;
use log::{info, error};
use mkt_pubber::receiver::ZmqReceiver;
use tokio::{select, sync::watch};
use tokio::signal;
use tokio_util::sync::CancellationToken;
// 从 lib crate 导入
use mkt_pubber::{PeriodMessage, MktArchiveMsg, RedisStreamMktPubber};

#[tokio::main]
async fn main() -> Result<()> {
    // 先设置日志级别，再初始化日志
    std::env::set_var("RUST_LOG", "info");
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(buf, "{}", record.args())
        })
        .init();
    println!("正在创建 Redis 发布者...");
    let publisher = match RedisStreamMktPubber::new("./mkt_cfg.yaml").await {
        Ok(p) => {
            println!("Redis 发布者创建成功");
            p
        },
        Err(e) => {
            println!("创建 Redis 发布者失败: {}", e);
            return Err(e);
        }
    };
    // 创建接收器
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut receiver = match ZmqReceiver::new(shutdown_rx) {
        Ok(r) => r,
        Err(e) => {
            println!("创建接收器失败: {}", e);
            return Err(e.into());
        }
    };
    
    // 获取消息接收通道
    let mut msg_rx = receiver.get_msg_rx();
    
    // 在后台任务中启动接收器
    tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            receiver.start_receiving();
        }).await.expect("接收器任务失败");
    });
        // 主循环等待关闭信号
    // 等待 SIGINT (Ctrl+C) 或 SIGTERM
    //使用tokio的canceltoken检测关闭信号
    let token = CancellationToken::new();
    let token_for_ctrl_c = token.clone();
    let token_for_sigterm = token.clone();
        // 创建一个tokio的task，用于检测ctrl_c信号
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to wait for Ctrl+C");
            info!("SIGINT (Ctrl+C) received");
            // 只发送关闭信号给主循环，让主循环来处理接收器的关闭
            token_for_ctrl_c.cancel();
        });
    
        tokio::spawn(async move {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
            sigterm.recv().await.expect("Failed to wait for SIGTERM");
            info!("SIGTERM received");
            // 只发送关闭信号给主循环，让主循环来处理接收器的关闭
            token_for_sigterm.cancel();
        });
        
    
    loop {
        select! {
            // 优先检查取消信号
            _ = token.cancelled() => {
                info!("收到关闭信号，开始退出...");
                break;
            }
            
            msg = msg_rx.recv() => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        if token.is_cancelled() {
                            info!("程序正在关闭，停止处理消息");
                            break;
                        }
                        println!("接收消息失败: {}", e);
                        // 如果channel关闭且不是因为程序退出，则退出循环
                        if e.to_string().contains("closed") {
                            error!("消息通道已关闭，退出主循环");
                            break;
                        }
                        continue;
                    }
                };
                
                let message = match PeriodMessage::from_capnp(&msg, true) {
                    Ok(m) => m,
                    Err(e) => {
                        println!("解析消息失败: {}", e);
                        continue;
                    }
                };
                
                message.print_info();
                let archive_msg = MktArchiveMsg::new(
                    message.period,
                    message.post_ts,
                    message.total_info_count(),
                    msg
                );
                
                if let Err(e) = publisher.publish(archive_msg).await {
                    println!("发布消息失败: {}", e);
                } else {
                    println!("发布消息成功");
                }
            }
        }
    }
    
    // 优雅关闭
    info!("发送关闭信号给ZMQ接收器...");
    let _ = shutdown_tx.send(true);
    
    // 等待一段时间让接收器优雅关闭
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    info!("程序退出完成");
    Ok(())
}
