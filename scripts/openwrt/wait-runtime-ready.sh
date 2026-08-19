#!/bin/sh
# 等待 OpenWrt 的 WAN 路由和系统校时完成，再启动依赖 Telegram 的常驻进程。
#
# ImmortalWrt/OpenWrt 在开机早期可能先启动 procd 服务，随后才拨号和 NTP
# 校时。此时直接连接 Telegram 会形成高频重连；若公网出口同时变化，还可能
# 触发 AUTH_KEY_DUPLICATED。目标系统安装 dnsmasqsec 时，ntpd 的 stratum
# hotplug 会创建 /var/state/dnsmasqsec，因而它是比“年份看起来正常”更可靠的
# 校时完成信号。

set -eu

POLL_SECONDS="${OPENWRT_READY_POLL_SECONDS:-5}"
LOG_INTERVAL_SECONDS="${OPENWRT_READY_LOG_INTERVAL_SECONDS:-60}"
ROUTE_TARGET="${OPENWRT_READY_ROUTE_TARGET:-1.1.1.1}"
NTP_HOOK="${OPENWRT_NTP_VALID_HOOK:-/etc/hotplug.d/ntp/25-dnsmasqsec}"
TIME_VALID_MARKER="${OPENWRT_TIME_VALID_MARKER:-/var/state/dnsmasqsec}"
MIN_VALID_EPOCH="${OPENWRT_MIN_VALID_EPOCH:-1704067200}"
READY_LABEL="${OPENWRT_READY_LABEL:-openwrt-ready}"

case "$POLL_SECONDS" in
	''|0|*[!0-9]*) POLL_SECONDS=5 ;;
esac
case "$LOG_INTERVAL_SECONDS" in
	''|0|*[!0-9]*) LOG_INTERVAL_SECONDS=60 ;;
esac
case "$MIN_VALID_EPOCH" in
	''|*[!0-9]*) MIN_VALID_EPOCH=1704067200 ;;
esac

log() {
	# procd 把 stdout 记为 daemon.info、stderr 记为 daemon.err。这里的等待与
	# 就绪消息都是正常生命周期事件，输出到 stdout，避免健康日志被误标为错误。
	printf '%s\n' "[${READY_LABEL}] $*"
}

route_is_ready() {
	command -v ip >/dev/null 2>&1 || return 1
	ip -4 route get "$ROUTE_TARGET" 2>/dev/null | grep -q ' dev '
}

clock_is_ready() {
	if [ -e "$NTP_HOOK" ]; then
		[ -e "$TIME_VALID_MARKER" ]
		return
	fi

	# 通用 OpenWrt 没有统一的 NTP ready 文件；未安装上述 hotplug 时退化为
	# 合理 epoch 检查。可用 OPENWRT_TIME_VALID_MARKER 指向自定义标记。
	now="$(date +%s 2>/dev/null || printf '0')"
	case "$now" in
		''|*[!0-9]*) return 1 ;;
	esac
	[ "$now" -ge "$MIN_VALID_EPOCH" ]
}

if [ "${OPENWRT_SKIP_READY_CHECK:-0}" = "1" ]; then
	log "已通过 OPENWRT_SKIP_READY_CHECK 跳过启动门禁"
	exit 0
fi

waited=0
next_log=0
while ! route_is_ready || ! clock_is_ready; do
	if [ "$waited" -ge "$next_log" ]; then
		route_state="等待"
		clock_state="等待"
		route_is_ready && route_state="就绪"
		clock_is_ready && clock_state="就绪"
		log "WAN=${route_state} NTP=${clock_state}；服务暂不连接 Telegram"
		next_log=$((waited + LOG_INTERVAL_SECONDS))
	fi
	sleep "$POLL_SECONDS"
	waited=$((waited + POLL_SECONDS))
done

log "WAN 与系统时钟已就绪（等待 ${waited}s）"
