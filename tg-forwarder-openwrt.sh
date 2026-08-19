#!/bin/sh
# OpenWrt 常驻入口：先等待 WAN/NTP，再启动 Web UI 与转发调度器。

set -eu

APP_DIR="$(CDPATH='' cd -- "$(dirname "$0")" && pwd)"
cd "$APP_DIR"
umask 077

OPENWRT_READY_LABEL="tg-forwarder-ready" \
	/bin/sh "$APP_DIR/scripts/openwrt/wait-runtime-ready.sh"

# copy-mode 的临时媒体必须落到 overlay 磁盘，不能使用 OpenWrt 的 /tmp tmpfs。
export TMPDIR="$APP_DIR/temp"
mkdir -p "$TMPDIR"
chmod 700 "$TMPDIR"
# 仅清理由异常退出遗留的空任务目录；含文件的目录保留，便于人工排查/恢复。
for stale_dir in "$TMPDIR"/*; do
	[ ! -d "$stale_dir" ] || rmdir "$stale_dir" 2>/dev/null || true
done

# 收紧已有运行态文件权限；不存在的文件由 umask 保证以 0600 创建。
for runtime_file in \
	config.json pairs.json watermarks.json message_map.json run_log.json \
	retry_queue.json tg_session.session tg_session.session-journal; do
	[ ! -f "$APP_DIR/$runtime_file" ] || chmod 600 "$APP_DIR/$runtime_file"
done

# 无缓冲输出，保证 copy pipeline 与 scheduler 日志按真实时序进入 syslog。
exec /usr/bin/python3 -u "$APP_DIR/server.py"
