#!/bin/bash

# 主控制脚本 - 启动/停止/状态查看
START_SCRIPT="./start_mkt.sh"
STOP_SCRIPT="./stop_mkt.sh"

case "$1" in
    start)
        echo "启动市场数据服务..."
        "$START_SCRIPT"
        ;;
    stop)
        echo "停止市场数据服务..."
        "$STOP_SCRIPT"
        ;;
    status)
        echo "服务状态："
        echo "  mkt_pubber:    $(pgrep mkt_pubber >/dev/null && echo '运行中' || echo '未运行')"
        echo "  mkt_processor: $(pgrep mkt_processor >/dev/null && echo '运行中' || echo '未运行')"
        echo "  crypto_proxy:  $(pgrep crypto_proxy >/dev/null && echo '运行中' || echo '未运行')"
        echo "  symbol_server: $(pgrep symbol_server >/dev/null && echo '运行中' || echo '未运行')"
        ;;
    restart)
        "$STOP_SCRIPT"
        sleep 2
        "$START_SCRIPT"
        ;;
    *)
        echo "用法: $0 {start|stop|status|restart}"
        exit 1
esac