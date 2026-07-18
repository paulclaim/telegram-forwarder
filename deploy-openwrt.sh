#!/usr/bin/env bash
# deploy-openwrt.sh — 一键把 telegram-forwarder 部署/更新到 OpenWrt
#
# 用法:
#   ./deploy-openwrt.sh           # 默认:同步代码 + 重启服务
#   ./deploy-openwrt.sh --no-restart   # 只同步代码,不重启(改动还没测完时用)
#   ./deploy-openwrt.sh --status       # 只看远程运行状态,不同步
#   ./deploy-openwrt.sh --logs         # 只看远程日志
#   ./deploy-openwrt.sh --init         # 首次部署(装依赖+装 init 脚本)
#
# SSH 主机别名 openwrt,部署目录 /root/tg-forwarder。
# 只同步代码,绝不覆盖远程的运行态文件(watermarks/pairs/message_map/run_log),
# 避免把转存进度和水印清零。

set -euo pipefail

SSH_HOST="openwrt"
REMOTE_DIR="/root/tg-forwarder"
INIT_SCRIPT="tg-forwarder.init"
PY_DEPS="telethon quart hypercorn openpyxl"

# ── 颜色 ────────────────────────────────────────────────────────
if [ -t 1 ]; then
  G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; B=$'\033[36m'; D=$'\033[2m'; N=$'\033[0m'
else
  G=""; Y=""; R=""; B=""; D=""; N=""
fi
log()  { echo "${G}✓${N} $*"; }
warn() { echo "${Y}⚠${N} $*"; }
err()  { echo "${R}✗${N} $*" >&2; }
step() { echo "${B}▶${N} $*"; }

ACTION="deploy"
for arg in "$@"; do
  case "$arg" in
    --no-restart) ACTION="deploy" RESTART=0 ;;
    --status)     ACTION="status" ;;
    --logs)       ACTION="logs" ;;
    --init)       ACTION="init" ;;
    -h|--help)
      sed -n '2,12p' "$0"; exit 0 ;;
    *) err "未知参数: $arg"; exit 1 ;;
  esac
done
RESTART="${RESTART:-1}"

# ── 只读动作 ─────────────────────────────────────────────────────
if [ "$ACTION" = "status" ]; then
  step "远程运行状态"
  ssh "$SSH_HOST" '/etc/init.d/tg-forwarder status 2>&1 | head -3
                   echo "--- 进程 ---"; pgrep -af "server.py" || echo "(无进程)"
                   echo "--- 健康检查 ---"; curl -s http://127.0.0.1:5000/healthz || echo "(无响应)"
                   echo; echo "--- pairs.json ---"; cat '"$REMOTE_DIR"'/pairs.json'
  exit $?
fi

if [ "$ACTION" = "logs" ]; then
  step "远程日志(最近 40 行)"
  ssh "$SSH_HOST" 'logread | grep -iE "tg-forwarder|scheduler|mode=|cycle|forwarded" | tail -40'
  exit $?
fi

# ── 连通性检查 ───────────────────────────────────────────────────
step "检查 SSH 连通性"
if ! ssh -o ConnectTimeout=8 "$SSH_HOST" true 2>/dev/null; then
  err "无法连接 $SSH_HOST —— 检查 ~/.ssh/config 里的 Host openwrt 配置"
  exit 1
fi
log "SSH 连通"

# ── 首次部署:装依赖 + 装 init 脚本 ──────────────────────────────
if [ "$ACTION" = "init" ]; then
  step "首次部署:创建远程目录 + 安装系统依赖"
  ssh "$SSH_HOST" "mkdir -p $REMOTE_DIR
    apk update >/dev/null 2>&1 || true
    apk add --no-cache rsync openssh-sftp-server 2>&1 | tail -3
    echo '--- 安装 Python 依赖 ---'
    python3 -m pip install --break-system-packages $PY_DEPS 2>&1 | tail -5
    echo '--- 验证导入 ---'
    python3 -c 'import telethon,quart,hypercorn,openpyxl; print(\"deps OK\")' 2>&1"

  step "传输 init 脚本并启用开机自启"
  if [ ! -f "$INIT_SCRIPT" ]; then
    err "找不到 $INIT_SCRIPT —— 它应该和本脚本在同一目录"
    exit 1
  fi
  scp "$INIT_SCRIPT" "$SSH_HOST:/etc/init.d/tg-forwarder"
  ssh "$SSH_HOST" "chmod +x /etc/init.d/tg-forwarder
    mkdir -p /var/log
    /etc/init.d/tg-forwarder enable
    echo 'init 脚本已启用开机自启'"
  log "首次部署完成。现在运行 ./deploy-openwrt.sh 同步代码,或手动 scp 会话/配置文件。"
  exit 0
fi

# ── 常规部署:同步代码 ───────────────────────────────────────────
step "同步代码到 $SSH_HOST:$REMOTE_DIR"
log "排除项: .venv .git .claude .spec-workflow __pycache__ *.pyc"
log "排除项: 会话/配置/状态文件(保护远程运行态,不被覆盖)"
rsync -avz --delete \
  --exclude='.venv' --exclude='venv' \
  --exclude='.git' --exclude='.claude' --exclude='.spec-workflow' \
  --exclude='__pycache__' --exclude='*.pyc' --exclude='*.pyo' \
  --exclude='tg_session.session' --exclude='*.session' --exclude='*.session-journal' \
  --exclude='config.json' --exclude='pairs.json' --exclude='sources.json' \
  --exclude='watermarks.json' --exclude='message_map.json' --exclude='run_log.json' \
  --exclude='retry_queue.json' \
  --exclude='downloads' --exclude='temp' --exclude='data' --exclude='.env' \
  ./ "$SSH_HOST:$REMOTE_DIR/" 2>&1 | grep -vE '/$' | tail -20
log "代码同步完成"

# ── 重启 ─────────────────────────────────────────────────────────
if [ "$RESTART" = "1" ]; then
  step "重启服务以加载新代码"
  ssh "$SSH_HOST" '/etc/init.d/tg-forwarder restart 2>&1
    sleep 5
    echo "--- 状态 ---"
    /etc/init.d/tg-forwarder status 2>&1 | head -3
    echo "--- 健康检查 ---"
    curl -s http://127.0.0.1:5000/healthz || echo "(服务还没起来,稍等再查)"
    echo; echo "--- 启动日志 ---"
    logread | grep -iE "server ready|cycle start|mode=" | tail -5'
  log "已重启,新代码生效"
else
  warn "跳过重启(--no-restart)。改动需手动重启才生效:"
  warn "  ssh $SSH_HOST '/etc/init.d/tg-forwarder restart'"
fi

echo
step "完成"
