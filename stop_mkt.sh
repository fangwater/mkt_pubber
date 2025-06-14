#!/bin/bash

# 停止脚本 - 按顺序停止三个进程

stop_process() {
    local name=$1
    local signal=$2
    local pids=$(pgrep -f "$name")
    
    if [ -n "$pids" ]; then
        echo "向 $name 发送 $signal 信号..."
        pkill -$signal -f "$name"
        return 0
    fi
    return 1
}

stop_crypto_proxy() {
    stop_process "crypto_proxy" TERM
}

stop_mkt_processor() {
    stop_process "mkt_processor" TERM
}

stop_mkt_pubber() {
    stop_process "mkt_pubber" TERM
}

force_kill() {
    local name=$1
    local pids=$(pgrep -f "$name")
    
    if [ -n "$pids" ]; then
        echo "强制停止 $name..."
        pkill -9 -f "$name"
    fi
}

# 主停止流程 - 按指定顺序
stop_crypto_proxy && sleep 2
stop_mkt_processor && sleep 1
stop_mkt_pubber

# 检查并强制停止残留进程
for process in crypto_proxy mkt_processor mkt_pubber; do
    pids=$(pgrep -f "$process")
    if [ -n "$pids" ]; then
        echo "$process 仍然在运行 (PID: $pids)，尝试强制停止..."
        force_kill "$process"
    fi
done

# 最终检查
for process in crypto_proxy mkt_processor mkt_pubber; do
    pids=$(pgrep -f "$process")
    [ -z "$pids" ] && echo "$process 已停止" || echo "警告: $process 可能未完全停止 (PID: $pids)"
done