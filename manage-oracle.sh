#!/usr/bin/env bash
# manage-oracle.sh — telegram-forwarder 的单文件 Oracle Cloud VPS 管理与部署入口
#
# 默认使用 ~/.ssh/config 中的 Host sanjose（Oracle Cloud VPS），远程目录为
# /opt/tg-forwarder，systemd 服务为 tg-forwarder.service（以 ubuntu 用户运行
# venv 里的 server.py）。本文件内置代码同步、systemd 单元安装、Web 认证配置、
# Telegram 重新登录、运行态备份和 SSH 隧道，不依赖仓库中的其他 Shell 脚本。
# 可通过环境变量或 ~/.config/telegram-forwarder/oracle-manager.conf 覆盖：
#   TG_FORWARDER_SSH_HOST=sanjose
#   TG_FORWARDER_REMOTE_DIR=/opt/tg-forwarder
#   TG_FORWARDER_SERVICE=tg-forwarder
#   TG_FORWARDER_REMOTE_USER=ubuntu
#   TG_FORWARDER_WEB_PORT=5000
#   TG_FORWARDER_LOCAL_PORT=5000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${TG_FORWARDER_MANAGER_CONFIG:-${HOME:-/tmp}/.config/telegram-forwarder/oracle-manager.conf}"

# 先读用户配置，再让命令行参数覆盖配置。
SSH_HOST="${TG_FORWARDER_SSH_HOST:-sanjose}"
REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-/opt/tg-forwarder}"
SERVICE_NAME="${TG_FORWARDER_SERVICE:-tg-forwarder}"
REMOTE_USER="${TG_FORWARDER_REMOTE_USER:-ubuntu}"
WEB_PORT="${TG_FORWARDER_WEB_PORT:-5000}"
LOCAL_PORT="${TG_FORWARDER_LOCAL_PORT:-5000}"
SSH_OPTS="${TG_FORWARDER_SSH_OPTS:--o ConnectTimeout=8}"
TARGET_MARKER=".tg-forwarder-root"
DRY_RUN=0
ASSUME_YES=0
ACTION="menu"
HOST_OVERRIDE_SET=0
REMOTE_OVERRIDE_SET=0

if [ -f "$CONFIG_FILE" ]; then
  # 这是用户自己创建的本地 shell 配置文件，不会从远程设备执行。
  # shellcheck disable=SC1090
  . "$CONFIG_FILE"
  SSH_HOST="${TG_FORWARDER_SSH_HOST:-${SSH_HOST:-sanjose}}"
  REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-${REMOTE_DIR:-/opt/tg-forwarder}}"
  SERVICE_NAME="${TG_FORWARDER_SERVICE:-${SERVICE_NAME:-tg-forwarder}}"
  REMOTE_USER="${TG_FORWARDER_REMOTE_USER:-${REMOTE_USER:-ubuntu}}"
  WEB_PORT="${TG_FORWARDER_WEB_PORT:-${WEB_PORT:-5000}}"
  LOCAL_PORT="${TG_FORWARDER_LOCAL_PORT:-${LOCAL_PORT:-5000}}"
  SSH_OPTS="${TG_FORWARDER_SSH_OPTS:-${SSH_OPTS:--o ConnectTimeout=8}}"
fi

if [ -t 1 ]; then
  G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; C=$'\033[36m'; D=$'\033[2m'; N=$'\033[0m'
else
  G=""; Y=""; R=""; C=""; D=""; N=""
fi

log()  { printf '%s✓%s %s\n' "$G" "$N" "$*"; }
warn() { printf '%s⚠%s %s\n' "$Y" "$N" "$*" >&2; }
err()  { printf '%s✗%s %s\n' "$R" "$N" "$*" >&2; }
step() { printf '%s▶%s %s\n' "$C" "$N" "$*"; }

usage() {
  cat <<EOF
用法: $(basename "$0") [选项]

默认进入交互式菜单。选项可用于脚本化调用：
  --status              查看服务、健康检查和 pairs.json
  --logs                查看最近日志（journalctl）
  --follow-logs         持续跟踪日志（Ctrl-C 返回）
  --deploy              同步代码并重启服务
  --sync                只同步代码，不重启
  --init                首次部署向导（系统依赖、venv、代码、单元、认证、启动）
  --install-unit        只安装/更新 systemd 单元并 enable
  --auth                设置或更新 Web 管理密码（写入远程 env）
  --login               在 VPS 上扫码登录 Telegram（自动停服务并备份旧 session）
  --login-phone         在 VPS 上用手机号和验证码登录（自动停服务并备份旧 session）
  --start / --stop      启动 / 停止服务
  --restart             重启服务
  --backup              备份远程配置、会话和运行态文件到 backups/oracle/
  --tunnel              通过 SSH 隧道访问 Web UI（http://127.0.0.1:$LOCAL_PORT）
  --shell               打开 VPS SSH shell
  --check               检查本机工具和 SSH 连通性
  --print-unit          输出生成的 systemd 单元内容（不安装）
  --host HOST           覆盖 SSH 主机别名（默认 sanjose）
  --remote-dir DIR      覆盖远程项目目录（默认 /opt/tg-forwarder）
  --service NAME        覆盖 systemd 服务名（默认 tg-forwarder）
  --remote-user USER    覆盖单元运行用户（默认 ubuntu）
  --web-port PORT       覆盖远程 Web 端口（默认 5000）
  --local-port PORT     覆盖隧道本地端口（默认 5000）
  --config FILE         指定本地配置文件
  --yes                 跳过部署、初始化、启停操作的确认
  --dry-run             只打印将执行的动作（不会连接设备）
  -h, --help            显示帮助

本地配置示例（${CONFIG_FILE}）：
  TG_FORWARDER_SSH_HOST=sanjose
  TG_FORWARDER_REMOTE_DIR=/opt/tg-forwarder
  TG_FORWARDER_SERVICE=tg-forwarder
  TG_FORWARDER_SSH_OPTS="-o ConnectTimeout=8"
EOF
}

die() { err "$*"; exit 1; }

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "找不到本机命令 '$1'。请先安装或加入 PATH。"
}

confirm() {
  if [ "$ASSUME_YES" -eq 1 ] || [ "$DRY_RUN" -eq 1 ]; then
    return 0
  fi
  if [ ! -t 0 ]; then
    die "此操作会改变远程服务，非交互调用请加 --yes。"
  fi
  printf '%s确认执行？[y/N] %s' "$Y" "$N"
  read -r answer
  case "$answer" in
    y|Y|yes|YES) return 0 ;;
    *) warn "已取消"; return 1 ;;
  esac
}

pause_screen() {
  if [ -t 0 ]; then
    printf '\n%s按回车返回菜单...%s' "$D" "$N"
    read -r _ || true
  fi
}

remote_exec() {
  # shellcheck disable=SC2029 # 远程命令必须在 VPS 端展开。
  ssh "${SSH_ARGS[@]}" "$SSH_HOST" "$1"
}

check_connection() {
  require_command ssh
  step "检查 SSH 连通性: $SSH_HOST"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh %s %s true\n' "$SSH_OPTS" "$SSH_HOST"
    return 0
  fi
  if ssh "${SSH_ARGS[@]}" -o BatchMode=yes "$SSH_HOST" true 2>/dev/null; then
    log "SSH 连通"
  else
    err "无法连接 ${SSH_HOST}。请检查 ~/.ssh/config、密钥和 VPS 网络。"
    return 1
  fi
}

check_local_tools() {
  require_command ssh
  case "$ACTION" in
    deploy|sync|init|menu|check) require_command rsync ;;
  esac
  case "$ACTION" in
    backup|menu|check) require_command scp ;;
  esac
  case "$ACTION" in
    backup|menu|check) require_command tar ;;
  esac
  log "本机依赖检查通过"
}

# systemd 单元内容与首次部署保持一致：env 文件提供 DASH_USER/DASH_PASS/PORT，
# TMPDIR 指向项目目录（Always Free 实例磁盘充足，copy-mode 临时媒体不进 tmpfs）。
emit_systemd_unit() {
  cat <<EOF
[Unit]
Description=telegram-forwarder-pro
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$REMOTE_USER
WorkingDirectory=$REMOTE_DIR
EnvironmentFile=$REMOTE_DIR/env
Environment=TMPDIR=$REMOTE_DIR/temp
ExecStart=$REMOTE_DIR/.venv/bin/python -u $REMOTE_DIR/server.py
Restart=always
RestartSec=5
NoNewPrivileges=yes
ProtectSystem=full
ReadWritePaths=$REMOTE_DIR

[Install]
WantedBy=multi-user.target
EOF
}

install_service_unit() {
  step "安装 systemd 单元 /etc/systemd/system/$SERVICE_NAME.service"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '上传单元到 %s:/etc/systemd/system/%s.service（经 sudo install）\n' "$SSH_HOST" "$SERVICE_NAME"
    printf '随后执行: sudo systemctl daemon-reload && sudo systemctl enable %s\n' "$SERVICE_NAME"
    return 0
  fi
  local unit_tmp="/tmp/tg-forwarder-unit-$$.service"
  if ! emit_systemd_unit | ssh "${SSH_ARGS[@]}" "$SSH_HOST" "umask 022; cat > '$unit_tmp' && sudo install -m 644 '$unit_tmp' /etc/systemd/system/$SERVICE_NAME.service && rm -f '$unit_tmp'"; then
    err "写入 systemd 单元失败"
    return 1
  fi
  remote_exec "
    sudo mkdir -p '$REMOTE_DIR/temp'
    sudo chown $REMOTE_USER:$REMOTE_USER '$REMOTE_DIR/temp'
    sudo systemctl daemon-reload
    sudo systemctl enable $SERVICE_NAME >/dev/null" || return 1
  log "systemd 单元已安装并 enable"
}

# rsync --delete 之前必须确认远端目录确实是本项目的部署目录，
# 防止把 --delete 打到别的目录上。
verify_remote_marker() {
  remote_exec "
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
    exit 3" || return $?
}

sync_code() {
  local rsync_ssh
  step "同步代码到 $SSH_HOST:$REMOTE_DIR"
  log "保护远程会话、env、配置、水印、消息映射与运行日志"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '校验部署目录标记：%s:%s/%s\n' "$SSH_HOST" "$REMOTE_DIR" "$TARGET_MARKER"
    printf 'rsync --delete（排除运行态文件）%s/ -> %s:%s/\n' "$SCRIPT_DIR" "$SSH_HOST" "$REMOTE_DIR"
    return 0
  fi
  step "校验远端部署目录"
  verify_remote_marker || return $?
  log "远端部署目录已确认"

  printf -v rsync_ssh '%q ' ssh "${SSH_ARGS[@]}"
  rsync_ssh="${rsync_ssh% }"
  if ! RSYNC_RSH="$rsync_ssh" rsync -avz --delete \
      --exclude='.venv' --exclude='venv' \
      --exclude='.git' --exclude='.claude' --exclude='.spec-workflow' \
      --exclude='__pycache__' --exclude='*.pyc' --exclude='*.pyo' --exclude='.DS_Store' \
      --exclude='tg_session.session' --exclude='*.session' --exclude='*.session-journal' \
      --exclude='config.json' --exclude='pairs.json' --exclude='sources.json' \
      --exclude='watermarks.json' --exclude='message_map.json' --exclude='run_log.json' \
      --exclude='retry_queue.json' --exclude='backups-*' --exclude='downloads' --exclude='temp' --exclude='data' \
      --exclude='.env' --exclude='env' --exclude='backups' --exclude="$TARGET_MARKER" \
      "$SCRIPT_DIR/" "$SSH_HOST:$REMOTE_DIR/" 2>&1 | awk '!/\/$/' | tail -20; then
    err "代码同步失败"
    return 1
  fi
  log "代码同步完成"
}

show_startup_logs() {
  [ "$DRY_RUN" -eq 1 ] && return 0
  step "最近启动日志"
  remote_exec "journalctl -u $SERVICE_NAME -n 20 --no-pager -o cat" || true
}

invoke_deploy() {
  local mode="$1"
  case "$mode" in
    init)
      step "首次初始化（${SSH_HOST}:${REMOTE_DIR}）"
      install_system_dependencies || return 1
      ensure_remote_venv || return 1
      ;;
    sync|deploy)
      if [ "$mode" = "deploy" ]; then
        step "同步代码并重启（${SSH_HOST}:${REMOTE_DIR}）"
      else
        step "只同步代码（${SSH_HOST}:${REMOTE_DIR}）"
      fi
      sync_code || return $?
      if [ "$mode" = "deploy" ]; then
        service_action_internal restart || return 1
        show_startup_logs || true
        log "已重启，新代码生效"
      else
        warn "已跳过重启；改动需执行 $(basename "$0") --restart --yes 后生效"
      fi
      ;;
    *) die "内部错误：未知部署模式 $mode" ;;
  esac
}

run_deploy() {
  confirm || return 0
  if [ "$1" = "deploy" ] || [ "$1" = "sync" ]; then
    require_command rsync
  fi
  check_connection || return 1
  invoke_deploy "$1"
}

configure_auth_internal() {
  local dash_user dash_pass pass_confirm entered_user user_b64 pass_b64
  dash_user="${TG_FORWARDER_DASH_USER:-admin}"
  dash_pass="${TG_FORWARDER_DASH_PASS:-}"

  if [ "$DRY_RUN" -eq 1 ]; then
    printf '将为 %s 写入 %s:%s/env（DASH_USER/DASH_PASS/PORT，保留已有 PORT）\n' "$dash_user" "$SSH_HOST" "$REMOTE_DIR"
    return 0
  fi

  if [ -t 0 ]; then
    printf 'Web 管理用户名 [%s]: ' "$dash_user"
    read -r entered_user
    [ -n "$entered_user" ] && dash_user="$entered_user"
    if [ -z "$dash_pass" ]; then
      printf 'Web 管理密码（输入不显示）: '
      read -r -s dash_pass
      printf '\n再次输入密码: '
      read -r -s pass_confirm
      printf '\n'
      if [ "$dash_pass" != "$pass_confirm" ]; then
        err "两次输入的密码不一致"
        return 1
      fi
    fi
  elif [ -z "$dash_pass" ]; then
    err "非交互模式必须通过 TG_FORWARDER_DASH_PASS 提供 Web 管理密码"
    return 1
  fi

  if [ -z "$dash_user" ] || [ -z "$dash_pass" ]; then
    err "用户名和密码不能为空"
    return 1
  fi
  if [ "${#dash_pass}" -lt 12 ]; then
    warn "建议使用至少 12 位的 Web 管理密码"
  fi
  require_command base64
  user_b64="$(printf '%s' "$dash_user" | base64 | tr -d '\n')"
  pass_b64="$(printf '%s' "$dash_pass" | base64 | tr -d '\n')"
  # 凭据经 base64 从 stdin 传入，不落命令行；env 文件保留已有 PORT。
  # shellcheck disable=SC2029 # REMOTE_DIR 已经过字符白名单校验。
  if ! printf '%s\n%s\n' "$user_b64" "$pass_b64" | \
    ssh "${SSH_ARGS[@]}" "$SSH_HOST" "
      umask 077
      IFS= read -r user_b64
      IFS= read -r pass_b64
      dash_user=\$(printf '%s' \"\$user_b64\" | base64 -d)
      dash_pass=\$(printf '%s' \"\$pass_b64\" | base64 -d)
      port=$WEB_PORT
      if [ -f '$REMOTE_DIR/env' ]; then
        existing_port=\$(grep -E '^PORT=[0-9]+' '$REMOTE_DIR/env' | tail -1 | cut -d= -f2)
        [ -n \"\$existing_port\" ] && port=\$existing_port
      fi
      printf 'DASH_USER=%s\nDASH_PASS=%s\nPORT=%s\n' \"\$dash_user\" \"\$dash_pass\" \"\$port\" > '$REMOTE_DIR/env'
      chmod 600 '$REMOTE_DIR/env'
    "; then
    err "Web 认证配置失败"
    return 1
  fi
  log "Web 认证已写入 $REMOTE_DIR/env"
}

ensure_remote_env() {
  if [ "$DRY_RUN" -eq 1 ]; then
    return 0
  fi
  # shellcheck disable=SC2016 # env 内容应在 VPS 端读取。
  if remote_exec "[ -f '$REMOTE_DIR/env' ] && grep -qE '^DASH_PASS=.+' '$REMOTE_DIR/env'" >/dev/null 2>&1; then
    log "远程 env 已配置 Web 认证"
    return 0
  fi
  warn "远程尚未配置 Web 管理密码，需要先写入 env。"
  configure_auth_internal
}

configure_auth() {
  confirm || return 0
  check_connection || return 1
  configure_auth_internal || return 1
  if [ "$DRY_RUN" -eq 1 ]; then
    return 0
  fi
  if [ "$(remote_exec "systemctl is-active $SERVICE_NAME 2>/dev/null || true")" = "active" ]; then
    service_action_internal restart || return 1
  else
    warn "服务当前未运行，密码将在下次启动时生效"
  fi
}

install_system_dependencies() {
  step "检查系统依赖（python3-venv、rsync）"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '如缺失则: sudo apt-get install -y python3-venv rsync\n'
    return 0
  fi
  remote_exec "
    set -eu
    missing=''
    command -v rsync >/dev/null 2>&1 || missing=\"\$missing rsync\"
    dpkg -s python3-venv >/dev/null 2>&1 || missing=\"\$missing python3-venv\"
    if [ -n \"\$missing\" ]; then
      sudo apt-get update -qq
      sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \$missing
      echo \"已安装:\$missing\"
    else
      echo '系统依赖已就绪'
    fi" || return 1
}

ensure_remote_venv() {
  step "创建 venv 并安装 Python 依赖"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'python3 -m venv %s/.venv && %s/.venv/bin/pip install -r %s/requirements.txt\n' "$REMOTE_DIR" "$REMOTE_DIR" "$REMOTE_DIR"
    return 0
  fi
  remote_exec "
    set -eu
    cd '$REMOTE_DIR'
    [ -x .venv/bin/python ] || python3 -m venv .venv
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet -r requirements.txt
    .venv/bin/python -c 'import telethon, quart, hypercorn, openpyxl, qrcode; print(\"deps OK\")'" || return 1
}

seed_runtime_files() {
  local file remote_exists missing_required=0
  step "补齐首次运行所需的本地配置和会话文件"
  for file in config.json pairs.json tg_session.session; do
    remote_exists=0
    if [ "$DRY_RUN" -eq 1 ]; then
      if [ -f "$SCRIPT_DIR/$file" ]; then
        printf '如远程缺失则上传：%s -> %s:%s/%s\n' "$SCRIPT_DIR/$file" "$SSH_HOST" "$REMOTE_DIR" "$file"
      else
        warn "本地没有 ${file}；实际执行时会检查远程是否存在"
      fi
      continue
    fi
    if remote_exec "test -f '$REMOTE_DIR/$file'" >/dev/null 2>&1; then
      remote_exists=1
    fi
    if [ "$remote_exists" -eq 1 ]; then
      log "远程已存在 ${file}，保留不覆盖"
    elif [ -f "$SCRIPT_DIR/$file" ]; then
      if scp "${SSH_ARGS[@]}" "$SCRIPT_DIR/$file" "$SSH_HOST:$REMOTE_DIR/$file" >/dev/null; then
        remote_exec "chmod 600 '$REMOTE_DIR/$file'"
        log "已上传 $file"
      else
        err "上传 $file 失败"
        return 1
      fi
    else
      warn "本地和远程都没有 $file"
    fi

    if [ "$file" = "config.json" ] || [ "$file" = "tg_session.session" ]; then
      if [ "$remote_exists" -eq 0 ] && [ ! -f "$SCRIPT_DIR/$file" ]; then
        missing_required=1
      fi
    fi
  done
  return "$missing_required"
}

initial_setup() {
  local runtime_ready=1
  confirm || return 0
  require_command ssh
  require_command scp
  require_command rsync
  check_connection || return 1
  invoke_deploy init || return 1
  ensure_remote_env || return 1
  seed_runtime_files || runtime_ready=0
  invoke_deploy sync || return 1
  install_service_unit || return 1
  if [ "$runtime_ready" -eq 0 ]; then
    warn "代码已部署，但缺少 config.json 或 tg_session.session，暂不启动服务。"
    warn "执行 $(basename "$0") --login --yes 扫码登录，或补齐文件后 --restart --yes。"
    return 1
  fi
  service_action_internal start || return 1
  log "首次部署完成"
}

service_action_internal() {
  local action="$1"
  case "$action" in
    start|stop|restart) ;;
    *) die "内部错误：未知服务动作 $action" ;;
  esac
  step "远程服务 $action"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" "sudo systemctl $action $SERVICE_NAME"
    return 0
  fi
  if ! remote_exec "sudo systemctl $action $SERVICE_NAME"; then
    err "服务 $action 操作失败"
    return 1
  fi
  if [ "$action" = "stop" ]; then
    remote_exec "systemctl status $SERVICE_NAME --no-pager 2>&1 | head -3; true"
  else
    # shellcheck disable=SC2016 # 循环变量应由远程 shell 展开。
    remote_exec "
      ready=0; attempt=0
      while [ \"\$attempt\" -lt 20 ]; do
        if curl -fsS --max-time 3 http://127.0.0.1:$WEB_PORT/healthz >/tmp/tg-forwarder-health 2>/dev/null; then
          ready=1; break
        fi
        attempt=\$((attempt + 1)); sleep 3
      done
      systemctl is-active $SERVICE_NAME
      if [ \"\$ready\" = 1 ]; then
        cat /tmp/tg-forwarder-health
      else
        echo '60 秒内未通过健康检查' >&2
      fi
      rm -f /tmp/tg-forwarder-health
      [ \"\$ready\" = 1 ]"
  fi
}

service_action() {
  confirm || return 0
  service_action_internal "$1"
}

ensure_forwarder_stopped_for_login() {
  step "确认转发进程已停止"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '检查并停止残留进程：%s/server.py\n' "$REMOTE_DIR"
    return 0
  fi
  # systemctl 状态丢失时可能残留旧 Python 进程。登录前必须确保它退出，
  # 防止旧进程继续持有正在替换的 session。
  local escaped_dir pattern
  escaped_dir="${REMOTE_DIR//./\\.}"
  pattern="python.*${escaped_dir}/server\\.py"
  remote_exec "
    if pgrep -f '$pattern' >/dev/null 2>&1; then
      echo '发现残留 telegram-forwarder 进程，正在停止...'
      pkill -TERM -f '$pattern' || true
      attempt=0
      while pgrep -f '$pattern' >/dev/null 2>&1 && [ \"\$attempt\" -lt 10 ]; do
        attempt=\$((attempt + 1))
        sleep 1
      done
    fi
    if pgrep -f '$pattern' >/dev/null 2>&1; then
      echo '残留 telegram-forwarder 进程未退出，拒绝替换 session' >&2
      exit 1
    fi
    echo '转发进程已停止'"
}

verify_telegram_login() {
  step "验证新 Telegram session"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '等待 healthz 确认 telegram_connected=true 且 telegram_session_error=null\n'
    return 0
  fi
  # 最长等待约 60 秒；一旦出现明确 session 错误就立即失败。
  # shellcheck disable=SC2016 # health 与循环变量应在 VPS 端展开。
  remote_exec "
    attempt=0
    while [ \"\$attempt\" -lt 20 ]; do
      health=\"\$(curl -fsS --max-time 3 http://127.0.0.1:$WEB_PORT/healthz 2>/dev/null || true)\"
      if printf '%s' \"\$health\" | grep -q 'telegram_connected.*true' &&
         printf '%s' \"\$health\" | grep -q 'telegram_session_error.*null'; then
        echo \"\$health\"
        exit 0
      fi
      if [ -n \"\$health\" ] && ! printf '%s' \"\$health\" | grep -q 'telegram_session_error.*null'; then
        echo \"新 session 验证失败：\$health\" >&2
        exit 1
      fi
      attempt=\$((attempt + 1))
      sleep 3
    done
    echo \"60 秒内未确认 Telegram 登录成功：\${health:-无健康响应}\" >&2
    exit 1"
}

telegram_login() {
  local login_mode="${1:-qr}" login_command login_option login_script login_script_b64
  case "$login_mode" in
    qr) login_command=login-qr; login_option=--login ;;
    phone) login_command=login-phone; login_option=--login-phone ;;
    *) err "未知 Telegram 登录方式：$login_mode"; return 1 ;;
  esac
  require_command ssh
  require_command base64
  if [ "$DRY_RUN" -eq 0 ] && [ ! -t 0 ]; then
    err "Telegram 登录需要交互式终端，请直接运行：$(basename "$0") $login_option"
    return 1
  fi

  warn "登录期间会停止转发服务；成功后自动启动。"
  warn "新的 tg_session.session 只能供当前服务使用，不能复制给其他机器或服务。"
  confirm || return 0
  check_connection || return 1
  service_action_internal stop || return 1
  ensure_forwarder_stopped_for_login || return 1

  step "备份旧 Telegram session 并开始 Telegram 登录"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh -t %s %s：备份 %s/tg_session.session，然后运行 .venv/bin/python cli.py %s\n' \
      "$SSH_OPTS" "$SSH_HOST" "$REMOTE_DIR" "$login_command"
  else
    login_script="$(cat <<'REMOTE_LOGIN_SCRIPT'
set -eu

app_dir=$1
login_mode=$2
cd "$app_dir"
[ -f config.json ] || {
  echo "缺少 $app_dir/config.json，无法读取 Telegram api_id/api_hash" >&2
  exit 2
}
[ -f cli.py ] || {
  echo "缺少 $app_dir/cli.py，请先部署代码" >&2
  exit 2
}

stamp=$(date +%Y%m%d-%H%M%S)-$$
if [ -f tg_session.session ]; then
  mv tg_session.session "tg_session.invalid-$stamp.session"
  chmod 600 "tg_session.invalid-$stamp.session"
  echo "旧 session 已备份为 tg_session.invalid-$stamp.session"
fi
if [ -f tg_session.session-journal ]; then
  mv tg_session.session-journal "tg_session.invalid-$stamp.session-journal"
  chmod 600 "tg_session.invalid-$stamp.session-journal"
fi

unset TELETHON_SESSION_STRING
export TELETHON_SESSION_FILE=tg_session
APP_PY="$app_dir/.venv/bin/python"
[ -x "$APP_PY" ] || APP_PY=python3

echo
case "$login_mode" in
  qr)
    echo '请使用已登录的 Telegram 手机客户端扫描终端二维码。'
    echo '若账号启用了两步验证，扫码后仍需在终端输入密码。'
    "$APP_PY" cli.py login-qr
    ;;
  phone)
    echo '请按提示输入手机号、Telegram 验证码和两步验证密码。'
    "$APP_PY" cli.py login-phone
    ;;
  *) echo "未知 Telegram 登录方式：$login_mode" >&2; exit 2 ;;
esac
[ -f tg_session.session ] || {
  echo '登录命令结束，但没有生成 tg_session.session' >&2
  exit 3
}
chmod 600 tg_session.session
echo 'Telegram 登录成功，新 session 已保存。'
REMOTE_LOGIN_SCRIPT
)"
    login_script_b64="$(printf '%s' "$login_script" | base64 | tr -d '\n')"
    # 必须分配伪终端，以显示二维码或读取手机号、验证码和两步验证密码。
    # 脚本编码进命令行而不是占用 stdin，以便远程 Python 保持交互输入。
    # shellcheck disable=SC2029 # 登录命令必须在 VPS 端展开并交互执行。
    if ! ssh "${SSH_ARGS[@]}" -t "$SSH_HOST" "
      set -eu
      remote_script=/tmp/tg-forwarder-login-\$\$.sh
      trap 'rm -f \"\$remote_script\"' EXIT HUP INT TERM
      python3 -c 'import base64, pathlib, sys; pathlib.Path(sys.argv[1]).write_bytes(base64.b64decode(sys.argv[2]))' \
        \"\$remote_script\" '$login_script_b64'
      chmod 700 \"\$remote_script\"
      \"\$remote_script\" '$REMOTE_DIR' '$login_mode'
    "; then
      err "Telegram 登录失败；服务保持停止，请修正后重新选择登录。"
      return 1
    fi
  fi

  service_action_internal start || return 1
  if ! verify_telegram_login; then
    err "服务已启动，但新 Telegram session 验证失败。"
    show_status || true
    return 1
  fi
  log "Telegram 登录完成，转发服务已启动"
  show_status
}

show_status() {
  step "远程运行状态"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" '服务状态、健康检查、session 诊断、资源和 pairs.json'
    return 0
  fi
  # shellcheck disable=SC2016 # health 与远程文件应在 VPS 端展开。
  remote_exec "
    systemctl is-enabled $SERVICE_NAME 2>/dev/null || true
    systemctl status $SERVICE_NAME --no-pager -l 2>&1 | head -6
    echo '--- 健康检查 ---'
    health=\"\$(curl -fsS --max-time 5 http://127.0.0.1:$WEB_PORT/healthz 2>/dev/null || true)\"
    if [ -n \"\$health\" ]; then echo \"\$health\"; else echo '(无响应)'; fi
    echo; echo '--- session 诊断 ---'
    if echo \"\$health\" | grep -q 'telegram_session_error.*AuthKey'; then
      echo 'session=invalid(需重新登录并重启服务)'
    elif echo \"\$health\" | grep -q 'telegram_connected.*true'; then
      echo 'session=active'
    else
      echo 'session=当前未连接'
    fi
    echo; echo '--- 资源 ---'
    free -h | head -2
    df -h / | tail -1
    echo; echo '--- pairs.json ---'
    if [ -f '$REMOTE_DIR/pairs.json' ]; then cat '$REMOTE_DIR/pairs.json'; else echo '(不存在)'; fi"
}

show_logs() {
  step "远程日志（最近 80 行）"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" "journalctl -u $SERVICE_NAME -n 80 --no-pager"
    return 0
  fi
  remote_exec "journalctl -u $SERVICE_NAME -n 80 --no-pager"
}

follow_logs() {
  step "实时日志（Ctrl-C 返回）"
  [ "$DRY_RUN" -eq 1 ] && { printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" "journalctl -u $SERVICE_NAME -f"; return 0; }
  remote_exec "journalctl -u $SERVICE_NAME -f"
}

backup_remote_state() {
  local stamp target_dir archive remote_archive remote_stage remote_checksum local_checksum
  stamp="$(date +%Y%m%d-%H%M%S)"
  target_dir="$SCRIPT_DIR/backups/oracle"
  archive="$target_dir/tg-forwarder-$stamp.tar.gz"
  remote_archive="/tmp/tg-forwarder-backup-$stamp-$$.tar.gz"
  remote_stage="/tmp/tg-forwarder-backup-stage-$stamp-$$"
  require_command scp
  require_command tar
  check_connection
  step "创建远程运行态快照"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '远程暂存: %s\n远程归档: %s\n本地文件: %s\n' "$remote_stage" "$remote_archive" "$archive"
    return 0
  fi

  mkdir -p "$target_dir"
  chmod 700 "$target_dir"
  if ! remote_checksum="$(remote_exec "cd '$REMOTE_DIR' || exit 1
    mkdir -p '$remote_stage' || exit 2
    chmod 700 '$remote_stage'
    copied=0
    for f in config.json pairs.json sources.json watermarks.json run_log.json message_map.json retry_queue.json tg_session.session tg_session.session-journal env; do
      if [ -f \"\$f\" ]; then cp -p \"\$f\" '$remote_stage/' || exit 3; copied=1; fi
    done
    if [ -f /etc/systemd/system/$SERVICE_NAME.service ]; then
      # 单元文件通常不可被运行用户直接读取，经 sudo 读取后再落暂存目录。
      sudo cat /etc/systemd/system/$SERVICE_NAME.service > '$remote_stage/systemd-tg-forwarder.service' || exit 3
      copied=1
    fi
    [ \"\$copied\" = 1 ] || { rm -rf '$remote_stage'; echo '没有可备份的运行态文件' >&2; exit 4; }
    tar -czf '$remote_archive' -C '$remote_stage' . || { rm -rf '$remote_stage'; exit 5; }
    rm -rf '$remote_stage'
    sha256sum '$remote_archive' | awk '{print \$1}'")"; then
    remote_exec "rm -rf '$remote_stage' '$remote_archive'" >/dev/null 2>&1 || true
    err "远程快照创建失败"
    return 1
  fi

  if ! scp "${SSH_ARGS[@]}" "$SSH_HOST:$remote_archive" "$archive" >/dev/null; then
    remote_exec "rm -f '$remote_archive'" >/dev/null 2>&1 || true
    err "快照下载失败"
    return 1
  fi
  remote_exec "rm -f '$remote_archive'" >/dev/null 2>&1 || true
  chmod 600 "$archive"

  if command -v shasum >/dev/null 2>&1; then
    local_checksum="$(shasum -a 256 "$archive" | awk '{print $1}')"
  elif command -v sha256sum >/dev/null 2>&1; then
    local_checksum="$(sha256sum "$archive" | awk '{print $1}')"
  else
    err "本机缺少 shasum/sha256sum，无法验证备份"
    return 1
  fi
  if [ "$local_checksum" != "$remote_checksum" ]; then
    err "备份校验失败：远程与本地 SHA-256 不一致"
    return 1
  fi
  if ! tar -tzf "$archive" >/dev/null; then
    err "备份归档损坏，无法读取"
    return 1
  fi
  log "备份完成并通过 SHA-256 校验：$archive"
  warn "归档包含 Telegram 会话和凭据，请妥善保管。"
}

open_shell() {
  require_command ssh
  step "连接到 ${SSH_HOST}（退出 shell 返回菜单）"
  [ "$DRY_RUN" -eq 1 ] && { printf 'ssh %s %s\n' "$SSH_OPTS" "$SSH_HOST"; return 0; }
  # shellcheck disable=SC2029 # 这里有意打开交互式远程 shell。
  ssh "${SSH_ARGS[@]}" "$SSH_HOST"
}

open_tunnel() {
  require_command ssh
  step "建立 SSH 隧道：本地 $LOCAL_PORT → $SSH_HOST 上的 127.0.0.1:$WEB_PORT（Ctrl-C 断开）"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh -N -o ExitOnForwardFailure=yes %s -L %d:127.0.0.1:%d %s\n' \
      "$SSH_OPTS" "$LOCAL_PORT" "$WEB_PORT" "$SSH_HOST"
    return 0
  fi
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    die "本机端口 $LOCAL_PORT 已被占用，请用 --local-port 换一个端口。"
  fi
  log "隧道已就绪后，在浏览器打开: http://127.0.0.1:$LOCAL_PORT"
  log "Web 账号是远程 $REMOTE_DIR/env 里的 DASH_USER / DASH_PASS"
  # shellcheck disable=SC2029 # 这里有意保持前台隧道。
  ssh -N -o ExitOnForwardFailure=yes "${SSH_ARGS[@]}" -L "$LOCAL_PORT:127.0.0.1:$WEB_PORT" "$SSH_HOST"
}

run_menu_action() {
  "$@" || warn "操作失败，可以修正配置或网络后重试。"
}

check_all() {
  check_local_tools
  check_connection
}

menu() {
  while true; do
    printf '\n%stelegram-forwarder / Oracle Cloud 管理器%s\n' "$C" "$N"
    printf '%s目标：%s%s:%s（服务 %s）%s\n' "$D" "$N" "$SSH_HOST" "$REMOTE_DIR" "$SERVICE_NAME" "$N"
    cat <<'EOF'

  1) 查看服务状态与健康检查
  2) 查看最近日志
  3) 实时跟踪日志
  4) 部署：同步代码并重启
  5) 只同步代码（不重启）
  6) 首次部署向导
  7) 重启服务
  8) 启动服务
  9) 停止服务
 10) 备份远程配置/会话/运行态
 11) 通过 SSH 隧道访问 Web UI
 12) 打开 SSH shell
 13) 检查本机工具和 SSH
 14) 设置/更新 Web 管理密码
 15) Telegram 扫码登录（自动停服务并备份旧 session）
 16) Telegram 手机号登录（自动停服务并备份旧 session）
  0) 退出
EOF
    printf '请选择 [0-16]: '
    read -r choice || exit 0
    case "$choice" in
      1) run_menu_action show_status; pause_screen ;;
      2) run_menu_action show_logs; pause_screen ;;
      3) run_menu_action follow_logs; pause_screen ;;
      4) run_menu_action run_deploy deploy; pause_screen ;;
      5) run_menu_action run_deploy sync; pause_screen ;;
      6) run_menu_action initial_setup; pause_screen ;;
      7) run_menu_action service_action restart; pause_screen ;;
      8) run_menu_action service_action start; pause_screen ;;
      9) run_menu_action service_action stop; pause_screen ;;
      10) run_menu_action backup_remote_state; pause_screen ;;
      11) run_menu_action open_tunnel ;;
      12) run_menu_action open_shell ;;
      13) run_menu_action check_all; pause_screen ;;
      14) run_menu_action configure_auth; pause_screen ;;
      15) run_menu_action telegram_login qr; pause_screen ;;
      16) run_menu_action telegram_login phone; pause_screen ;;
      0) return 0 ;;
      *) warn "无效选项：$choice" ;;
    esac
  done
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --status|--logs|--follow-logs|--deploy|--sync|--init|--auth|--login|--login-phone|--start|--stop|--restart|--backup|--tunnel|--shell|--check)
      ACTION="${1#--}"; shift ;;
    --install-unit) ACTION="install-unit"; shift ;;
    --print-unit) ACTION="print-unit"; shift ;;
    --menu) ACTION="menu"; shift ;;
    --host) [ "$#" -ge 2 ] || die "--host 需要参数"; SSH_HOST="$2"; HOST_OVERRIDE_SET=1; shift 2 ;;
    --remote-dir) [ "$#" -ge 2 ] || die "--remote-dir 需要参数"; REMOTE_DIR="$2"; REMOTE_OVERRIDE_SET=1; shift 2 ;;
    --service) [ "$#" -ge 2 ] || die "--service 需要参数"; SERVICE_NAME="$2"; shift 2 ;;
    --remote-user) [ "$#" -ge 2 ] || die "--remote-user 需要参数"; REMOTE_USER="$2"; shift 2 ;;
    --web-port) [ "$#" -ge 2 ] || die "--web-port 需要参数"; WEB_PORT="$2"; shift 2 ;;
    --local-port) [ "$#" -ge 2 ] || die "--local-port 需要参数"; LOCAL_PORT="$2"; shift 2 ;;
    --config) [ "$#" -ge 2 ] || die "--config 需要参数"; CONFIG_FILE="$2"; shift 2
      [ -f "$CONFIG_FILE" ] || die "配置文件不存在：$CONFIG_FILE"
      # shellcheck disable=SC1090
      . "$CONFIG_FILE"
      if [ "$HOST_OVERRIDE_SET" -eq 0 ]; then SSH_HOST="${TG_FORWARDER_SSH_HOST:-$SSH_HOST}"; fi
      if [ "$REMOTE_OVERRIDE_SET" -eq 0 ]; then REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-$REMOTE_DIR}"; fi
      SERVICE_NAME="${TG_FORWARDER_SERVICE:-$SERVICE_NAME}"
      REMOTE_USER="${TG_FORWARDER_REMOTE_USER:-$REMOTE_USER}"
      WEB_PORT="${TG_FORWARDER_WEB_PORT:-$WEB_PORT}"
      LOCAL_PORT="${TG_FORWARDER_LOCAL_PORT:-$LOCAL_PORT}"
      SSH_OPTS="${TG_FORWARDER_SSH_OPTS:-$SSH_OPTS}" ;;
    --yes) ASSUME_YES=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "未知参数：$1（使用 --help 查看帮助）" ;;
  esac
done

case "$REMOTE_DIR" in
  /*) ;;
  *) die "远程目录必须是绝对路径：$REMOTE_DIR" ;;
esac
case "$REMOTE_DIR" in
  *[!A-Za-z0-9_./-]*) die "远程目录只能包含字母、数字、下划线、点、斜杠和短横线：$REMOTE_DIR" ;;
esac
case "$REMOTE_DIR" in
  /|/.) die "远程目录不能是文件系统根目录：$REMOTE_DIR" ;;
esac
case "$REMOTE_DIR/" in
  *//*|*/./*|*/../*) die "远程目录不能包含空、. 或 .. 路径段：$REMOTE_DIR" ;;
esac

for port_var in WEB_PORT LOCAL_PORT; do
  case "${!port_var}" in
    ''|*[!0-9]*) die "$port_var 必须是数字端口：${!port_var}" ;;
  esac
done

# 将共享 SSH 参数解析成数组，避免未加引号的字符串触发通配符展开。
# ProxyCommand 等含空格的复杂参数应放进 ~/.ssh/config。
read -r -a SSH_ARGS <<< "$SSH_OPTS"

case "$ACTION" in
  menu) menu ;;
  status) show_status ;;
  logs) show_logs ;;
  follow-logs) follow_logs ;;
  deploy) run_deploy deploy ;;
  sync) run_deploy sync ;;
  init) initial_setup ;;
  install-unit) install_service_unit ;;
  auth) configure_auth ;;
  login) telegram_login qr ;;
  login-phone) telegram_login phone ;;
  start|stop|restart) service_action "$ACTION" ;;
  backup) backup_remote_state ;;
  tunnel) open_tunnel ;;
  shell) open_shell ;;
  check) check_local_tools; check_connection ;;
  print-unit) emit_systemd_unit ;;
  *) die "未知动作：$ACTION" ;;
esac
