//测试用的zmq receiver
use zmq::{Context, Socket, SocketType};
use log::{info, warn, error};
use std::time::Duration;
use tokio::sync::watch;
pub struct ZmqReceiver{
    #[allow(dead_code)]
    context: Context,
    ipc_socket: Socket,
    receive_count: u64,
    receiver_shutdown_rx: watch::Receiver<bool>,
    pub msg_tx : tokio::sync::broadcast::Sender<Vec<u8>>,
}

#[allow(dead_code)]
impl ZmqReceiver {
    pub fn get_msg_rx(&self) -> tokio::sync::broadcast::Receiver<Vec<u8>> {
        self.msg_tx.subscribe()
    }

    pub fn new(receiver_shutdown_rx: watch::Receiver<bool>) -> Result<Self, zmq::Error> {
        // 创建ZMQ上下文
        let context = Context::new();
        context.set_io_threads(1)?;
        
        // 创建PULL socket用于接收消息
        let ipc_socket = context.socket(SocketType::SUB)?;
        
        // 设置接收水位线
        ipc_socket.set_rcvhwm(100 as i32)?;

        let (msg_tx, _msg_rx) = tokio::sync::broadcast::channel(3);
        
        let mut receiver = Self {
            context,
            ipc_socket,
            receive_count: 0,
            receiver_shutdown_rx: receiver_shutdown_rx,
            msg_tx: msg_tx,
        };
        
        receiver.subscribe()?;
        Ok(receiver)
    }
    
    fn subscribe(&mut self) -> Result<(), zmq::Error> {
        let ipc_addr = format!("ipc://{}", "/tmp/mkt_archive.ipc");
        // 订阅所有主题
        self.ipc_socket.set_subscribe(b"")?;
        match self.ipc_socket.connect(&ipc_addr) {
            Ok(_) => {
                info!("ZmqReceiver connect success, ipc: {}", ipc_addr);
                Ok(())
            }
            Err(e) => {
                error!("ZmqReceiver IPC connect failed, path: {}, error: {}", ipc_addr, e);
                Err(e)
            }
        }
    }
    
    pub fn start_receiving(&mut self) {
        info!("ZmqReceiver started, listening on ipc://{}", "/tmp/mkt_archive.ipc");
        
        loop {
            // 检查关闭信号
            if *self.receiver_shutdown_rx.borrow() {
                info!("ZmqReceiver received shutdown signal, stopping...");
                break;
            }
            
            // 接收消息
            match self.ipc_socket.recv_bytes(zmq::DONTWAIT) {
                Ok(msg) => {
                    self.receive_count += 1;
                    self.process_message(&msg);
                }
                Err(e) => {
                    if e == zmq::Error::EAGAIN {
                        // 没有消息可接收，短暂睡眠后继续
                        std::thread::sleep(Duration::from_millis(1));
                    } else {
                        error!("Error receiving message: {}", e);
                        std::thread::sleep(Duration::from_millis(1000));
                    }
                }
            }
        }
        info!("ZmqReceiver stopped gracefully");
    }
    
    fn process_message(&self, msg: &[u8]) {
        // 处理接收到的消息
        info!("接收到消息，长度: {} 字节", msg.len());
        
        // 发送消息到broadcast channel
        if let Err(e) = self.msg_tx.send(msg.to_vec()) {
            warn!("发送消息到channel失败: {}", e);
        }
    }
}

impl Drop for ZmqReceiver {
    fn drop(&mut self) {
        info!("ZmqReceiver stopping...");
        let ipc_addr = format!("ipc://{}", "/tmp/mkt_archive.ipc");
        
        if let Err(e) = self.ipc_socket.unbind(&ipc_addr) {
            warn!("Failed to unbind IPC socket: {}", e);
        } else {
            info!("ZmqReceiver ipc_socket unbind success");
        }
        
        info!("ZmqReceiver stopped");
    }
}
