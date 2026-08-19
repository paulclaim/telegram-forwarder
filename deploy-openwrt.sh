#!/usr/bin/env bash
# deploy-openwrt.sh — 一键把 telegram-forwarder 部署/更新到 OpenWrt
#
# 用法:
#   ./deploy-openwrt.sh           # 默认:同步代码 + 重启服务
#   ./deploy-openwrt.sh --no-restart   # 只同步代码,不重启(改动还没测完时用)
#   ./deploy-openwrt.sh --status       # 只看远程运行状态,不同步
#   ./deploy-openwrt.sh --logs         # 只看远程日志
#   ./deploy-openwrt.sh --init         # 首次部署(装依赖+装 init 脚本)
#   ./deploy-openwrt.sh --service      # 只更新 procd 服务脚本/UCI 配置
#
# SSH 主机别名 openwrt,部署目录 /root/tg-forwarder。
# 可通过 TG_FORWARDER_SSH_HOST / TG_FORWARDER_REMOTE_DIR / TG_FORWARDER_SSH_OPTS 覆盖。
# 只同步代码,绝不覆盖远程的运行态文件(watermarks/pairs/message_map/run_log),
# 避免把转存进度和水印清零。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

SSH_HOST="${TG_FORWARDER_SSH_HOST:-openwrt}"
REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-/root/tg-forwarder}"
SSH_OPTS="${TG_FORWARDER_SSH_OPTS:--o ConnectTimeout=8}"
INIT_SCRIPT="tg-forwarder.init"
INIT_CONFIG="tg-forwarder.config"
RUNNER_SCRIPT="tg-forwarder-openwrt.sh"
READY_SCRIPT="scripts/openwrt/wait-runtime-ready.sh"
PY_DEPS="telethon quart hypercorn openpyxl"
TARGET_MARKER=".tg-forwarder-root"

# 将选项解析为数组，避免通配符展开。含空格的 ProxyCommand 等复杂配置应写进 ~/.ssh/config。
read -r -a SSH_ARGS <<< "$SSH_OPTS"
printf -v RSYNC_SSH '%q ' ssh "${SSH_ARGS[@]}"
RSYNC_SSH="${RSYNC_SSH% }"
# shellcheck disable=SC2029 # 参数中的命令有意在 OpenWrt 端执行。
ssh_run() { ssh "${SSH_ARGS[@]}" "$SSH_HOST" "$@"; }
scp_run() { scp "${SSH_ARGS[@]}" "$@"; }

case "$REMOTE_DIR" in
  /*) ;;
  *) echo "远程目录必须是绝对路径: $REMOTE_DIR" >&2; exit 1 ;;
esac
case "$REMOTE_DIR" in
  *[!A-Za-z0-9_./-]*) echo "远程目录包含不支持的字符: $REMOTE_DIR" >&2; exit 1 ;;
esac
case "$REMOTE_DIR" in
  /|/.) echo "远程目录不能是文件系统根目录: $REMOTE_DIR" >&2; exit 1 ;;
esac
case "$REMOTE_DIR/" in
  *//*|*/./*|*/../*) echo "远程目录不能包含空、. 或 .. 路径段: $REMOTE_DIR" >&2; exit 1 ;;
esac

# ── 颜色 ────────────────────────────────────────────────────────
if [ -t 1 ]; then
  G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; B=$'\033[36m'; N=$'\033[0m'
else
  G=""; Y=""; R=""; B=""; N=""
fi
log()  { echo "${G}✓${N} $*"; }
warn() { echo "${Y}⚠${N} $*"; }
err()  { echo "${R}✗${N} $*" >&2; }
step() { echo "${B}▶${N} $*"; }

install_service_files() {
  step "更新 procd 服务脚本和持久配置"
  if [ ! -f "$INIT_SCRIPT" ] || [ ! -f "$INIT_CONFIG" ] || [ ! -f "$RUNNER_SCRIPT" ] || [ ! -f "$READY_SCRIPT" ]; then
    err "缺少 $INIT_SCRIPT、$INIT_CONFIG、$RUNNER_SCRIPT 或 $READY_SCRIPT"
    return 1
  fi
  ssh_run "mkdir -p '$REMOTE_DIR/scripts/openwrt'"
  scp_run "$INIT_SCRIPT" "$SSH_HOST:/etc/init.d/tg-forwarder"
  scp_run "$INIT_CONFIG" "$SSH_HOST:/tmp/tg-forwarder.config"
  scp_run "$RUNNER_SCRIPT" "$SSH_HOST:$REMOTE_DIR/$RUNNER_SCRIPT"
  scp_run "$READY_SCRIPT" "$SSH_HOST:$REMOTE_DIR/$READY_SCRIPT"
  ssh_run "chmod +x /etc/init.d/tg-forwarder '$REMOTE_DIR/$RUNNER_SCRIPT' '$REMOTE_DIR/$READY_SCRIPT'
    if [ ! -f /etc/config/tg-forwarder ]; then
      cp /tmp/tg-forwarder.config /etc/config/tg-forwarder
      echo '已创建 /etc/config/tg-forwarder（启动前必须设置 dash_pass）'
    else
      echo '保留已有 /etc/config/tg-forwarder'
    fi
    rm -f /tmp/tg-forwarder.config
    uci set 'tg-forwarder.main.app_dir=$REMOTE_DIR'
    uci commit tg-forwarder
    chmod 600 /etc/config/tg-forwarder
    /etc/init.d/tg-forwarder enable"
}

remote_auth_is_safe() {
  # shellcheck disable=SC2016 # UCI 值应在 OpenWrt 端读取。
  ssh_run '
    pass="$(uci -q get tg-forwarder.main.dash_pass 2>/dev/null || true)"
    allow="$(uci -q get tg-forwarder.main.allow_insecure_open_dash 2>/dev/null || true)"
    [ -n "$pass" ] || [ "$allow" = 1 ]'
}

ACTION="deploy"
for arg in "$@"; do
  case "$arg" in
    --no-restart) ACTION="deploy" RESTART=0 ;;
    --status)     ACTION="status" ;;
    --logs)       ACTION="logs" ;;
    --init)       ACTION="init" ;;
    --service)    ACTION="service" ;;
    -h|--help)
      sed -n '2,12p' "$0"; exit 0 ;;
    *) err "未知参数: $arg"; exit 1 ;;
  esac
done
RESTART="${RESTART:-1}"

# ── 只读动作 ─────────────────────────────────────────────────────
if [ "$ACTION" = "status" ]; then
  step "远程运行状态"
  # shellcheck disable=SC2016 # $health 应在 OpenWrt 端展开。
  ssh_run '/etc/init.d/tg-forwarder status 2>&1 | head -3
                   echo "--- 进程 ---"; pgrep -af "^(/usr/bin/)?python3 .*/server\\.py$|^(/bin/)?(ash|sh) [^ ]*/tg-forwarder-openwrt\\.sh($| )" || echo "(无进程)"
                   echo "--- 健康检查 ---"; health="$(curl -fsS --max-time 5 http://127.0.0.1:5000/healthz 2>/dev/null || true)"; if [ -n "$health" ]; then echo "$health"; else echo "(无响应)"; fi
                   echo; echo "--- 启动门禁 ---"; date "+time=%Y-%m-%dT%H:%M:%S%z"; if [ -e /etc/hotplug.d/ntp/25-dnsmasqsec ]; then [ -e /var/state/dnsmasqsec ] && echo "ntp=ready" || echo "ntp=waiting"; else echo "ntp=unknown(no marker hook)"; fi
                   echo; echo "--- session 诊断 ---"; if echo "$health" | grep -q "telegram_session_error.*AuthKey"; then echo "session=invalid(需重新登录并重启服务)"; elif echo "$health" | grep -q "telegram_connected.*true"; then echo "session=active"; elif grep -qi "authorization key.*different IP" '"$REMOTE_DIR"'/run_log.json 2>/dev/null; then echo "session=needs-check(历史出现 AUTH_KEY_DUPLICATED，当前 Telegram 未连接)"; else echo "session=当前未连接，未发现 AUTH_KEY_DUPLICATED"; fi
                   echo; echo "--- pairs.json ---"; if [ -f '"$REMOTE_DIR"'/pairs.json ]; then cat '"$REMOTE_DIR"'/pairs.json; else echo "(不存在)"; fi'
  exit $?
fi

if [ "$ACTION" = "logs" ]; then
  step "远程日志(最近 80 行)"
  ssh_run 'logread | grep -iE " (tg-forwarder|tg-forwarder-openwrt\\.sh)\\[[0-9]+\\]:|procd:.*tg-forwarder" | tail -80'
  exit $?
fi

# ── 连通性检查 ───────────────────────────────────────────────────
step "检查 SSH 连通性"
if ! ssh_run true 2>/dev/null; then
  err "无法连接 $SSH_HOST —— 检查 SSH 配置、密钥和网络"
  exit 1
fi
log "SSH 连通"

if [ "$ACTION" = "service" ]; then
  install_service_files
  log "procd 服务脚本已更新"
  exit 0
fi

# ── 首次部署:装依赖 + 装 init 脚本 ──────────────────────────────
if [ "$ACTION" = "init" ]; then
  step "首次部署:创建远程目录 + 安装系统依赖"
  ssh_run "mkdir -p $REMOTE_DIR
    if command -v apk >/dev/null 2>&1; then
      apk update >/dev/null 2>&1 || true
      if ! apk add --no-cache python3 python3-pip rsync openssh-sftp-server >/tmp/tg-forwarder-pkg.log 2>&1; then
        apk add --no-cache python3 py3-pip rsync openssh-sftp-server >/tmp/tg-forwarder-pkg.log 2>&1 || {
          tail -20 /tmp/tg-forwarder-pkg.log; rm -f /tmp/tg-forwarder-pkg.log; exit 1;
        }
      fi
      tail -8 /tmp/tg-forwarder-pkg.log
      rm -f /tmp/tg-forwarder-pkg.log
    elif command -v opkg >/dev/null 2>&1; then
      opkg update >/dev/null 2>&1 || true
      opkg install python3 python3-pip rsync openssh-sftp-server >/tmp/tg-forwarder-pkg.log 2>&1 || {
        tail -20 /tmp/tg-forwarder-pkg.log; rm -f /tmp/tg-forwarder-pkg.log; exit 1;
      }
      tail -8 /tmp/tg-forwarder-pkg.log
      rm -f /tmp/tg-forwarder-pkg.log
    else
      echo '未找到 apk 或 opkg，无法安装系统依赖' >&2
      exit 1
    fi
    echo '--- 安装 Python 依赖 ---'
    if python3 -m pip install --break-system-packages $PY_DEPS >/tmp/tg-forwarder-pip.log 2>&1; then
      tail -8 /tmp/tg-forwarder-pip.log
    else
      tail -8 /tmp/tg-forwarder-pip.log
      python3 -m pip install $PY_DEPS >/tmp/tg-forwarder-pip.log 2>&1 || {
        tail -20 /tmp/tg-forwarder-pip.log
        rm -f /tmp/tg-forwarder-pip.log
        exit 1
      }
      tail -8 /tmp/tg-forwarder-pip.log
    fi
    rm -f /tmp/tg-forwarder-pip.log
    echo '--- 验证导入 ---'
    python3 -c 'import telethon,quart,hypercorn,openpyxl; print(\"deps OK\")' 2>&1"

  install_service_files
  log "系统初始化完成。请设置 dashboard 密码、同步代码和运行态文件后再启动服务。"
  exit 0
fi

# ── 常规部署:同步代码 ───────────────────────────────────────────
step "同步代码到 $SSH_HOST:$REMOTE_DIR"
log "排除项: .venv .git .claude .spec-workflow __pycache__ *.pyc"
log "排除项: 会话/配置/状态文件(保护远程运行态,不被覆盖)"
step "校验远端部署目录"
ssh_run "
  set -eu
  mkdir -p '$REMOTE_DIR'
  marker='$REMOTE_DIR/$TARGET_MARKER'
  if [ -e \"\$marker\" ]; then
    if grep -qx 'telegram-forwarder-pro' \"\$marker\"; then
      exit 0
    fi
    echo '部署目录标记内容异常，拒绝 rsync --delete：'\"\$marker\" >&2
    exit 3
  fi
  if [ -z \"\$(ls -A '$REMOTE_DIR' 2>/dev/null)\" ] || {
    [ -f '$REMOTE_DIR/server.py' ] && [ -f '$REMOTE_DIR/automate.py' ];
  }; then
    printf '%s\n' 'telegram-forwarder-pro' > \"\$marker\"
    chmod 600 \"\$marker\"
    exit 0
  fi
  echo '远端目录非空且不像 telegram-forwarder-pro，拒绝 rsync --delete：$REMOTE_DIR' >&2
  exit 3
"
log "远端部署目录已确认"
RSYNC_RSH="$RSYNC_SSH" rsync -avz --delete \
  --exclude='.venv' --exclude='venv' \
  --exclude='.git' --exclude='.claude' --exclude='.spec-workflow' \
  --exclude='__pycache__' --exclude='*.pyc' --exclude='*.pyo' \
  --exclude='tg_session.session' --exclude='*.session' --exclude='*.session-journal' \
  --exclude='config.json' --exclude='pairs.json' --exclude='sources.json' \
  --exclude='watermarks.json' --exclude='message_map.json' --exclude='run_log.json' \
  --exclude='retry_queue.json' \
  --exclude='downloads' --exclude='temp' --exclude='data' --exclude='.env' \
  --exclude="$TARGET_MARKER" \
  ./ "$SSH_HOST:$REMOTE_DIR/" 2>&1 | grep -vE '/$' | tail -20
ssh_run "chmod +x '$REMOTE_DIR/$RUNNER_SCRIPT' '$REMOTE_DIR/scripts/openwrt/wait-runtime-ready.sh'"
log "代码同步完成"

# 每次部署都同步服务脚本；UCI 配置只在缺失时创建，已有密码不会被覆盖。
install_service_files

# ── 重启 ─────────────────────────────────────────────────────────
if [ "$RESTART" = "1" ]; then
  if ! remote_auth_is_safe >/dev/null 2>&1; then
    err "远程未设置 dash_pass，拒绝重启以避免无认证管理界面。"
    err "请先运行 ./manage-openwrt.sh --auth"
    exit 1
  fi
  step "重启服务以加载新代码"
  # shellcheck disable=SC2016 # 状态变量应由远程 ash 展开。
  ssh_run '/etc/init.d/tg-forwarder restart 2>&1
    ready=0; attempt=0
    while [ "$attempt" -lt 20 ]; do
      if curl -fsS --max-time 3 http://127.0.0.1:5000/healthz >/tmp/tg-forwarder-health 2>/dev/null; then
        ready=1; break
      fi
      attempt=$((attempt + 1)); sleep 3
    done
    echo "--- 状态 ---"
    /etc/init.d/tg-forwarder status 2>&1 | head -3
    echo "--- 健康检查 ---"
    if [ "$ready" = 1 ]; then cat /tmp/tg-forwarder-health; else echo "(60 秒内未通过健康检查)"; fi
    rm -f /tmp/tg-forwarder-health
    echo; echo "--- 启动日志 ---"
    logread | grep -iE " (tg-forwarder|tg-forwarder-openwrt\\.sh)\\[[0-9]+\\]:|procd:.*tg-forwarder" | tail -20
    [ "$ready" = 1 ]'
  log "已重启,新代码生效"
else
  warn "跳过重启(--no-restart)。改动需手动重启才生效:"
  warn "  ssh $SSH_HOST '/etc/init.d/tg-forwarder restart'"
fi

echo
step "完成"
