#!/bin/bash

# 启动脚本 - 按顺序启动三个进程
LOG_DIR="./logs"

# 创建日志目录
mkdir -p "$LOG_DIR"
current_time=$(date +%Y%m%d_%H%M%S)

# 清理旧日志（可选）
# find "$LOG_DIR" -name "*.log" -mtime +7 -delete

# 停止现有进程
echo "停止现有进程..."
./stop_mkt.sh

# 启动顺序函数
start_mkt_pubber() {
    echo "启动 mkt_pubber..."
    nohup "./mkt_pubber" > "$LOG_DIR/mkt_pubber_$current_time.log" 2>&1 &
    sleep 3  # 确保完全启动
}

start_mkt_processor() {
    echo "启动 mkt_processor..."
    export ASAN_OPTIONS="detect_leaks=1:halt_on_error=0:log_path=./asan.log"
    nohup "./mkt_processor" > "$LOG_DIR/mkt_processor_$current_time.log" 2>&1 &
    sleep 2
}

start_crypto_proxy() {
    echo "启动 crypto_proxy..."
    nohup "./crypto_proxy" > "$LOG_DIR/crypto_proxy_$current_time.log" 2>&1 &
}

# 主启动流程
start_mkt_pubber
start_mkt_processor
start_crypto_proxy

echo ""
echo "服务启动完成，日志保存在:"
echo "  mkt_pubber:    $LOG_DIR/mkt_pubber_$current_time.log"
echo "  mkt_processor: $LOG_DIR/mkt_processor_$current_time.log"
echo "  crypto_proxy:  $LOG_DIR/crypto_proxy_$current_time.log"