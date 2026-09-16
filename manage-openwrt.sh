#!/usr/bin/env bash
# manage-openwrt.sh — telegram-forwarder 的单文件 OpenWrt 管理与部署入口
#
# 默认使用 ~/.ssh/config 中的 Host openwrt，远程目录为 /root/tg-forwarder。
# 本文件内置代码同步、procd 服务、远程常驻入口和 WAN/NTP 启动门禁，不依赖
# 仓库中的其他 Shell 脚本。
# 可通过环境变量或 ~/.config/telegram-forwarder/openwrt-manager.conf 覆盖：
#   TG_FORWARDER_SSH_HOST=openwrt
#   TG_FORWARDER_REMOTE_DIR=/root/tg-forwarder

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${TG_FORWARDER_MANAGER_CONFIG:-${HOME:-/tmp}/.config/telegram-forwarder/openwrt-manager.conf}"

# 先读用户配置，再让命令行参数覆盖配置。
SSH_HOST="${TG_FORWARDER_SSH_HOST:-openwrt}"
REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-/root/tg-forwarder}"
SSH_OPTS="${TG_FORWARDER_SSH_OPTS:--o ConnectTimeout=8}"
PY_DEPS="telethon quart hypercorn openpyxl qrcode"
TARGET_MARKER=".tg-forwarder-root"
RUNNER_SCRIPT="tg-forwarder-openwrt.sh"
DRY_RUN=0
ASSUME_YES=0
ACTION="menu"
HOST_OVERRIDE_SET=0
REMOTE_OVERRIDE_SET=0

if [ -f "$CONFIG_FILE" ]; then
  # 这是用户自己创建的本地 shell 配置文件，不会从远程设备执行。
  # shellcheck disable=SC1090
  . "$CONFIG_FILE"
  SSH_HOST="${TG_FORWARDER_SSH_HOST:-${SSH_HOST:-openwrt}}"
  REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-${REMOTE_DIR:-/root/tg-forwarder}}"
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
  --status              查看服务、进程、健康检查和 pairs.json
  --logs                查看最近日志
  --follow-logs         持续跟踪日志（Ctrl-C 返回）
  --deploy              同步代码并重启服务
  --sync                只同步代码，不重启
  --init                首次部署向导（依赖、认证、代码、运行态、启动）
  --auth                设置或更新持久化 Web 管理密码
  --login               在 OpenWrt 上扫码登录 Telegram（自动备份旧 session）
  --start / --stop      启动 / 停止服务
  --restart             重启服务
  --backup              备份远程配置、会话和运行态文件到 backups/openwrt/
  --shell               打开 OpenWrt SSH shell
  --check               检查本机工具和 SSH 连通性
  --print-runtime       输出部署时生成的 OpenWrt 常驻脚本
  --host HOST           覆盖 SSH 主机别名
  --remote-dir DIR      覆盖远程项目目录（默认 /root/tg-forwarder）
  --config FILE         指定本地配置文件
  --yes                 跳过部署、初始化、启停操作的确认
  --dry-run             只打印将执行的动作（不会连接设备）
  -h, --help            显示帮助

本地配置示例（${CONFIG_FILE}）：
  TG_FORWARDER_SSH_HOST=openwrt
  TG_FORWARDER_REMOTE_DIR=/root/tg-forwarder
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
  # shellcheck disable=SC2029 # 远程命令必须在 OpenWrt 端展开。
  ssh "${SSH_ARGS[@]}" "$SSH_HOST" "$1"
}

# 以下三个生成器是部署到 OpenWrt 的运行时资产。把内容保留在管理器内，确保
# 复制单个 manage-openwrt.sh 就能完成初始化、更新与服务修复。
emit_service_init() {
  cat <<'OPENWRT_SERVICE_INIT'
#!/bin/sh /etc/rc.common

# telegram-forwarder — procd service
USE_PROCD=1
START=99

RUNNER_NAME="tg-forwarder-openwrt.sh"

start_service() {
    config_load tg-forwarder
    config_get APP_DIR main app_dir "/root/tg-forwarder"
    config_get PORT main port "5000"
    config_get DASH_USER main dash_user "admin"
    config_get DASH_PASS main dash_pass ""
    config_get_bool ALLOW_INSECURE_OPEN_DASH main allow_insecure_open_dash "0"
    config_get TELEGRAM_PROXY_URL main telegram_proxy_url ""
    RUNNER="$APP_DIR/$RUNNER_NAME"

    if [ -z "$DASH_PASS" ] && [ "$ALLOW_INSECURE_OPEN_DASH" != "1" ]; then
        logger -t tg-forwarder "拒绝启动：/etc/config/tg-forwarder 未设置 dash_pass"
        return 1
    fi
    if [ ! -f "$RUNNER" ]; then
        logger -t tg-forwarder "拒绝启动：缺少 $RUNNER"
        return 1
    fi

    procd_open_instance
    procd_set_param command "$RUNNER"
    procd_set_param env PORT="$PORT"
    procd_set_param env DASH_USER="$DASH_USER"
    procd_set_param env DASH_PASS="$DASH_PASS"
    if [ -n "$TELEGRAM_PROXY_URL" ]; then
        procd_set_param env TELEGRAM_PROXY_URL="$TELEGRAM_PROXY_URL"
    fi
    if [ "$ALLOW_INSECURE_OPEN_DASH" = "1" ]; then
        procd_set_param env ALLOW_INSECURE_OPEN_DASH=1
    fi
    procd_set_param cwd "$APP_DIR"
    procd_set_param stdout 1
    procd_set_param stderr 1
    procd_set_param respawn 3600 5 10
    procd_set_param limits nofile=65535
    procd_close_instance
}

reload_service() {
    restart
}

service_triggers() {
    procd_add_reload_trigger "tg-forwarder"
}
OPENWRT_SERVICE_INIT
}

emit_service_config() {
  cat <<'OPENWRT_SERVICE_CONFIG'
config service 'main'
    option app_dir '/root/tg-forwarder'
    option port '5000'
    option dash_user 'admin'
    option dash_pass ''
    option allow_insecure_open_dash '0'
OPENWRT_SERVICE_CONFIG
}

emit_runtime_runner() {
  cat <<'OPENWRT_RUNTIME_RUNNER'
#!/bin/sh
# OpenWrt 常驻入口：先等待 WAN/NTP，再启动 Web UI 与转发调度器。

set -eu

APP_DIR="$(CDPATH='' cd -- "$(dirname "$0")" && pwd)"
cd "$APP_DIR"
umask 077

POLL_SECONDS="${OPENWRT_READY_POLL_SECONDS:-5}"
LOG_INTERVAL_SECONDS="${OPENWRT_READY_LOG_INTERVAL_SECONDS:-60}"
ROUTE_TARGET="${OPENWRT_READY_ROUTE_TARGET:-1.1.1.1}"
NTP_HOOK="${OPENWRT_NTP_VALID_HOOK:-/etc/hotplug.d/ntp/25-dnsmasqsec}"
TIME_VALID_MARKER="${OPENWRT_TIME_VALID_MARKER:-/var/state/dnsmasqsec}"
MIN_VALID_EPOCH="${OPENWRT_MIN_VALID_EPOCH:-1704067200}"
READY_LABEL="${OPENWRT_READY_LABEL:-tg-forwarder-ready}"

case "$POLL_SECONDS" in
    ''|0|*[!0-9]*) POLL_SECONDS=5 ;;
esac
case "$LOG_INTERVAL_SECONDS" in
    ''|0|*[!0-9]*) LOG_INTERVAL_SECONDS=60 ;;
esac
case "$MIN_VALID_EPOCH" in
    ''|*[!0-9]*) MIN_VALID_EPOCH=1704067200 ;;
esac

ready_log() {
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

    now="$(date +%s 2>/dev/null || printf '0')"
    case "$now" in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ "$now" -ge "$MIN_VALID_EPOCH" ]
}

if [ "${OPENWRT_SKIP_READY_CHECK:-0}" = "1" ]; then
    ready_log "已通过 OPENWRT_SKIP_READY_CHECK 跳过启动门禁"
else
    waited=0
    next_log=0
    while ! route_is_ready || ! clock_is_ready; do
        if [ "$waited" -ge "$next_log" ]; then
            route_state="等待"
            clock_state="等待"
            route_is_ready && route_state="就绪"
            clock_is_ready && clock_state="就绪"
            ready_log "WAN=${route_state} NTP=${clock_state}；服务暂不连接 Telegram"
            next_log=$((waited + LOG_INTERVAL_SECONDS))
        fi
        sleep "$POLL_SECONDS"
        waited=$((waited + POLL_SECONDS))
    done
    ready_log "WAN 与系统时钟已就绪（等待 ${waited}s）"
fi

# 供部署前诊断和自动化测试单独验证启动门禁，不启动业务进程。
if [ "${OPENWRT_READY_CHECK_ONLY:-0}" = "1" ]; then
    exit 0
fi

# copy-mode 的临时媒体必须落到 overlay 磁盘，不能使用 OpenWrt 的 /tmp tmpfs。
export TMPDIR="$APP_DIR/temp"
mkdir -p "$TMPDIR"
chmod 700 "$TMPDIR"
for stale_dir in "$TMPDIR"/*; do
    [ ! -d "$stale_dir" ] || rmdir "$stale_dir" 2>/dev/null || true
done

for runtime_file in \
    config.json pairs.json watermarks.json message_map.json run_log.json \
    retry_queue.json tg_session.session tg_session.session-journal; do
    [ ! -f "$APP_DIR/$runtime_file" ] || chmod 600 "$APP_DIR/$runtime_file"
done

exec /usr/bin/python3 -u "$APP_DIR/server.py"
OPENWRT_RUNTIME_RUNNER
}

upload_generated_file() {
  local remote_path="$1" generator="$2"
  # shellcheck disable=SC2029 # remote_path 来自固定路径或已校验的 REMOTE_DIR。
  if ! "$generator" | ssh "${SSH_ARGS[@]}" "$SSH_HOST" "umask 077; cat > '$remote_path'"; then
    err "写入远程文件失败：$remote_path"
    return 1
  fi
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
    err "无法连接 ${SSH_HOST}。请检查 ~/.ssh/config、密钥和 OpenWrt 网络。"
    return 1
  fi
}

check_local_tools() {
  require_command ssh
  case "$ACTION" in
    deploy|sync|init|menu|check) require_command rsync ;;
  esac
  case "$ACTION" in
    init|backup|menu|check) require_command scp ;;
  esac
  case "$ACTION" in
    backup|menu|check) require_command tar ;;
  esac
  log "本机依赖检查通过"
}

install_service_files() {
  step "安装内置 procd 服务与 OpenWrt 常驻入口"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '生成并写入 %s:%s/%s\n' "$SSH_HOST" "$REMOTE_DIR" "$RUNNER_SCRIPT"
    printf '生成并写入 %s:/etc/init.d/tg-forwarder\n' "$SSH_HOST"
    printf '如不存在则创建 %s:/etc/config/tg-forwarder\n' "$SSH_HOST"
    return 0
  fi
  remote_exec "mkdir -p '$REMOTE_DIR'" || return 1
  upload_generated_file "/etc/init.d/tg-forwarder" emit_service_init || return 1
  upload_generated_file "/tmp/tg-forwarder.config" emit_service_config || return 1
  upload_generated_file "$REMOTE_DIR/$RUNNER_SCRIPT" emit_runtime_runner || return 1
  remote_exec "
    chmod +x /etc/init.d/tg-forwarder '$REMOTE_DIR/$RUNNER_SCRIPT'
    if [ ! -f /etc/config/tg-forwarder ]; then
      cp /tmp/tg-forwarder.config /etc/config/tg-forwarder
      echo '已创建 /etc/config/tg-forwarder（启动前必须设置 dash_pass）'
    else
      echo '保留已有 /etc/config/tg-forwarder'
    fi
    rm -f /tmp/tg-forwarder.config
    uci -q get tg-forwarder.main >/dev/null 2>&1 || uci set tg-forwarder.main=service
    uci set 'tg-forwarder.main.app_dir=$REMOTE_DIR'
    uci commit tg-forwarder
    chmod 600 /etc/config/tg-forwarder
    /etc/init.d/tg-forwarder enable" || return 1
  log "procd 服务和常驻入口已更新"
}

install_system_dependencies() {
  step "安装 OpenWrt 系统与 Python 依赖"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" "安装 python3/python3-pip/rsync/openssh-sftp-server 和 $PY_DEPS"
    return 0
  fi
  remote_exec "
    mkdir -p '$REMOTE_DIR'
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
    python3 -c 'import telethon,quart,hypercorn,openpyxl,qrcode; print(\"deps OK\")' 2>&1" || return 1
}

ensure_qr_login_dependency() {
  step "检查远程二维码登录依赖"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '检查 %s 是否已安装 Python qrcode；缺失时先安装再停止服务\n' "$SSH_HOST"
    return 0
  fi
  remote_exec '
    if python3 -c "import qrcode" >/dev/null 2>&1; then
      echo "qrcode 已安装"
      exit 0
    fi
    echo "正在安装 qrcode..."
    if python3 -m pip install --break-system-packages "qrcode>=7.4.2,<9" >/tmp/tg-forwarder-qrcode.log 2>&1 ||
       python3 -m pip install "qrcode>=7.4.2,<9" >/tmp/tg-forwarder-qrcode.log 2>&1; then
      rm -f /tmp/tg-forwarder-qrcode.log
      python3 -c "import qrcode"
      echo "qrcode 安装完成"
    else
      tail -20 /tmp/tg-forwarder-qrcode.log
      rm -f /tmp/tg-forwarder-qrcode.log
      exit 1
    fi' || {
      err "无法安装二维码登录依赖；转发服务尚未停止"
      return 1
    }
}

sync_code() {
  local rsync_ssh
  step "同步代码到 $SSH_HOST:$REMOTE_DIR"
  log "保护远程会话、配置、水印、消息映射、日志与下载数据"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '校验部署目录标记：%s:%s/%s\n' "$SSH_HOST" "$REMOTE_DIR" "$TARGET_MARKER"
    printf 'rsync --delete（排除运行态文件）%s/ -> %s:%s/\n' "$SCRIPT_DIR" "$SSH_HOST" "$REMOTE_DIR"
    return 0
  fi
  step "校验远端部署目录"
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
  log "远端部署目录已确认"

  printf -v rsync_ssh '%q ' ssh "${SSH_ARGS[@]}"
  rsync_ssh="${rsync_ssh% }"
  if ! RSYNC_RSH="$rsync_ssh" rsync -avz --delete \
      --exclude='.venv' --exclude='venv' \
      --exclude='.git' --exclude='.claude' --exclude='.spec-workflow' \
      --exclude='__pycache__' --exclude='*.pyc' --exclude='*.pyo' \
      --exclude='tg_session.session' --exclude='*.session' --exclude='*.session-journal' \
      --exclude='config.json' --exclude='pairs.json' --exclude='sources.json' \
      --exclude='watermarks.json' --exclude='message_map.json' --exclude='run_log.json' \
      --exclude='retry_queue.json' --exclude='downloads' --exclude='temp' --exclude='data' \
      --exclude='.env' --exclude='backups' --exclude="$TARGET_MARKER" --exclude="$RUNNER_SCRIPT" \
      "$SCRIPT_DIR/" "$SSH_HOST:$REMOTE_DIR/" 2>&1 | awk '!/\/$/' | tail -20; then
    err "代码同步失败"
    return 1
  fi
  log "代码同步完成"
}

show_startup_logs() {
  [ "$DRY_RUN" -eq 1 ] && return 0
  step "最近启动日志"
  remote_exec 'logread | grep -iE " (tg-forwarder|tg-forwarder-openwrt\.sh)\[[0-9]+\]:|procd:.*tg-forwarder" | tail -20'
}

invoke_deploy() {
  local mode="$1"
  case "$mode" in
    init)
      step "首次初始化（${SSH_HOST}:${REMOTE_DIR}）"
      install_system_dependencies || return 1
      install_service_files || return 1
      log "系统初始化完成"
      ;;
    sync|deploy)
      if [ "$mode" = "deploy" ]; then
        step "同步代码并重启（${SSH_HOST}:${REMOTE_DIR}）"
      else
        step "只同步代码（${SSH_HOST}:${REMOTE_DIR}）"
      fi
      sync_code || return $?
      install_service_files || return 1
      if [ "$mode" = "deploy" ]; then
        service_action_internal restart || return 1
        show_startup_logs || return 1
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
  if [ "$1" = "deploy" ] || [ "$1" = "sync" ]; then
    ensure_remote_auth || return 1
  fi
  invoke_deploy "$1"
}

configure_auth_internal() {
  local dash_user dash_pass pass_confirm entered_user user_b64 pass_b64
  dash_user="${TG_FORWARDER_DASH_USER:-admin}"
  dash_pass="${TG_FORWARDER_DASH_PASS:-}"

  if [ "$DRY_RUN" -eq 1 ]; then
    printf '将为 %s 设置持久化 Web 认证（密码不会打印）\n' "$dash_user"
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
  # shellcheck disable=SC2029 # REMOTE_DIR 已经过字符白名单校验。
  if ! printf '%s\n%s\n' "$user_b64" "$pass_b64" | \
    ssh "${SSH_ARGS[@]}" "$SSH_HOST" "
      IFS= read -r user_b64
      IFS= read -r pass_b64
      dash_user=\$(printf '%s' \"\$user_b64\" | base64 -d)
      dash_pass=\$(printf '%s' \"\$pass_b64\" | base64 -d)
      uci -q get tg-forwarder.main >/dev/null 2>&1 || uci set tg-forwarder.main=service
      uci set \"tg-forwarder.main.app_dir=$REMOTE_DIR\"
      uci set \"tg-forwarder.main.dash_user=\$dash_user\"
      uci set \"tg-forwarder.main.dash_pass=\$dash_pass\"
      uci set tg-forwarder.main.allow_insecure_open_dash=0
      uci commit tg-forwarder
      chmod 600 /etc/config/tg-forwarder
    "; then
    err "持久化 Web 认证配置失败"
    return 1
  fi
  log "Web 认证已写入 /etc/config/tg-forwarder"
}

sync_service_files() {
  install_service_files
}

ensure_remote_auth() {
  if [ "$DRY_RUN" -eq 1 ]; then
    return 0
  fi
  # shellcheck disable=SC2016 # UCI 值应在 OpenWrt 端读取。
  if remote_exec '
    pass="$(uci -q get tg-forwarder.main.dash_pass 2>/dev/null || true)"
    allow="$(uci -q get tg-forwarder.main.allow_insecure_open_dash 2>/dev/null || true)"
    [ -n "$pass" ] || [ "$allow" = 1 ]' >/dev/null 2>&1; then
    return 0
  fi
  warn "远程尚未配置持久化 Web 管理密码，部署前必须先设置。"
  configure_auth_internal
}

configure_auth() {
  confirm || return 0
  check_connection || return 1
  configure_auth_internal || return 1
  sync_service_files || return 1
  if [ "$DRY_RUN" -eq 1 ] || remote_exec '/etc/init.d/tg-forwarder status >/dev/null 2>&1'; then
    service_action_internal restart || return 1
  else
    warn "服务当前未运行，密码将在下次启动时生效"
  fi
}

seed_runtime_files() {
  local file remote_exists missing_required=0
  step "补齐首次运行所需的本地配置和会话文件"
  for file in config.json pairs.json sources.json tg_session.session; do
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
  configure_auth_internal || return 1
  if ! seed_runtime_files; then
    runtime_ready=0
  fi
  invoke_deploy sync || return 1
  if [ "$runtime_ready" -eq 0 ]; then
    warn "代码已部署，但缺少 config.json 或 tg_session.session，暂不启动服务。"
    warn "补齐文件后执行：$(basename "$0") --restart --yes"
    return 1
  fi
  service_action_internal restart || return 1
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
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" "/etc/init.d/tg-forwarder $action"
    return 0
  fi
  if ! remote_exec "/etc/init.d/tg-forwarder $action"; then
    err "服务 $action 操作失败"
    return 1
  fi
  if [ "$action" = "stop" ]; then
    remote_exec '/etc/init.d/tg-forwarder status 2>&1 | head -3; true'
  else
    # shellcheck disable=SC2016 # 循环变量应由远程 ash 展开。
    remote_exec '
      ready=0; attempt=0
      while [ "$attempt" -lt 20 ]; do
        if curl -fsS --max-time 3 http://127.0.0.1:5000/healthz >/tmp/tg-forwarder-health 2>/dev/null; then
          ready=1; break
        fi
        attempt=$((attempt + 1)); sleep 3
      done
      /etc/init.d/tg-forwarder status 2>&1 | head -3
      if [ "$ready" = 1 ]; then
        cat /tmp/tg-forwarder-health
      else
        echo "60 秒内未通过健康检查" >&2
      fi
      rm -f /tmp/tg-forwarder-health
      [ "$ready" = 1 ]'
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
  # procd 状态丢失时 init stop 可能报告 inactive，但旧 Python 进程仍在。
  # 登录前必须确保它退出，防止旧进程继续持有正在替换的 session。
  remote_exec "
    pattern='^(/usr/bin/)?python3( -u)? $REMOTE_DIR/server\\.py\$'
    if pgrep -f \"\$pattern\" >/dev/null 2>&1; then
      echo '发现残留 telegram-forwarder 进程，正在停止...'
      pkill -TERM -f \"\$pattern\" || true
      attempt=0
      while pgrep -f \"\$pattern\" >/dev/null 2>&1 && [ \"\$attempt\" -lt 10 ]; do
        attempt=\$((attempt + 1))
        sleep 1
      done
    fi
    if pgrep -f \"\$pattern\" >/dev/null 2>&1; then
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
  # shellcheck disable=SC2016 # health 与循环变量应在 OpenWrt 端展开。
  remote_exec '
    attempt=0
    while [ "$attempt" -lt 20 ]; do
      health="$(curl -fsS --max-time 3 http://127.0.0.1:5000/healthz 2>/dev/null || true)"
      if printf "%s" "$health" | grep -q "telegram_connected.*true" &&
         printf "%s" "$health" | grep -q "telegram_session_error.*null"; then
        echo "$health"
        exit 0
      fi
      if [ -n "$health" ] && ! printf "%s" "$health" | grep -q "telegram_session_error.*null"; then
        echo "新 session 验证失败：$health" >&2
        exit 1
      fi
      attempt=$((attempt + 1))
      sleep 3
    done
    echo "60 秒内未确认 Telegram 登录成功：${health:-无健康响应}" >&2
    exit 1'
}

telegram_login() {
  local login_script login_script_b64
  require_command ssh
  require_command base64
  if [ "$DRY_RUN" -eq 0 ] && [ ! -t 0 ]; then
    err "Telegram 登录需要交互式终端，请直接运行：$(basename "$0") --login"
    return 1
  fi

  warn "登录期间会停止转发服务；成功后自动启动。"
  warn "新的 tg_session.session 只能供当前服务使用，不能复制给其他机器或服务。"
  confirm || return 0
  check_connection || return 1
  ensure_qr_login_dependency || return 1
  service_action_internal stop || return 1
  ensure_forwarder_stopped_for_login || return 1

  step "备份旧 Telegram session 并开始扫码登录"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh -t %s %s：备份 %s/tg_session.session，然后运行 python3 cli.py login-qr\n' \
      "$SSH_OPTS" "$SSH_HOST" "$REMOTE_DIR"
  else
    login_script="$(cat <<'REMOTE_LOGIN_SCRIPT'
set -eu

app_dir=$1
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

proxy_url=$(uci -q get tg-forwarder.main.telegram_proxy_url 2>/dev/null || true)
if [ -n "$proxy_url" ]; then
  export TELEGRAM_PROXY_URL="$proxy_url"
  echo '已沿用 OpenWrt 的 Telegram 代理配置'
fi
unset TELETHON_SESSION_STRING
export TELETHON_SESSION_FILE=tg_session

echo
echo '请使用已登录的 Telegram 手机客户端扫描终端二维码。'
echo '若账号启用了两步验证，扫码后仍需在终端输入密码。'
python3 cli.py login-qr
[ -f tg_session.session ] || {
  echo '登录命令结束，但没有生成 tg_session.session' >&2
  exit 3
}
chmod 600 tg_session.session
echo 'Telegram 登录成功，新 session 已保存。'
REMOTE_LOGIN_SCRIPT
)"
    login_script_b64="$(printf '%s' "$login_script" | base64 | tr -d '\n')"
    # 必须分配伪终端，以正确显示二维码并在需要时安全读取两步验证密码。
    # 脚本编码进命令行而不是占用 stdin，以便远程 Python 保持交互输入。
    # shellcheck disable=SC2029 # 登录命令必须在 OpenWrt 端展开并交互执行。
    if ! ssh "${SSH_ARGS[@]}" -t "$SSH_HOST" "
      set -eu
      remote_script=/tmp/tg-forwarder-login-\$\$.sh
      trap 'rm -f \"\$remote_script\"' EXIT HUP INT TERM
      python3 -c 'import base64, pathlib, sys; pathlib.Path(sys.argv[1]).write_bytes(base64.b64decode(sys.argv[2]))' \
        \"\$remote_script\" '$login_script_b64'
      chmod 700 \"\$remote_script\"
      \"\$remote_script\" '$REMOTE_DIR'
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
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" '服务状态、进程、健康检查、启动门禁、session 诊断和 pairs.json'
    return 0
  fi
  # shellcheck disable=SC2016 # health 和远程文件应在 OpenWrt 端展开。
  remote_exec '/etc/init.d/tg-forwarder status 2>&1 | head -3
    echo "--- 进程 ---"
    pgrep -af "^(/usr/bin/)?python3 .*/server\\.py$|^(/bin/)?(ash|sh) [^ ]*/tg-forwarder-openwrt\\.sh($| )" || echo "(无进程)"
    echo "--- 健康检查 ---"
    health="$(curl -fsS --max-time 5 http://127.0.0.1:5000/healthz 2>/dev/null || true)"
    if [ -n "$health" ]; then echo "$health"; else echo "(无响应)"; fi
    echo; echo "--- 启动门禁 ---"
    date "+time=%Y-%m-%dT%H:%M:%S%z"
    if [ -e /etc/hotplug.d/ntp/25-dnsmasqsec ]; then
      [ -e /var/state/dnsmasqsec ] && echo "ntp=ready" || echo "ntp=waiting"
    else
      echo "ntp=unknown(no marker hook)"
    fi
    echo; echo "--- session 诊断 ---"
    if echo "$health" | grep -q "telegram_session_error.*AuthKey"; then
      echo "session=invalid(需重新登录并重启服务)"
    elif echo "$health" | grep -q "telegram_connected.*true"; then
      echo "session=active"
    elif grep -qi "authorization key.*different IP" '"$REMOTE_DIR"'/run_log.json 2>/dev/null; then
      echo "session=needs-check(历史出现 AUTH_KEY_DUPLICATED，当前 Telegram 未连接)"
    else
      echo "session=当前未连接，未发现 AUTH_KEY_DUPLICATED"
    fi
    echo; echo "--- pairs.json ---"
    if [ -f '"$REMOTE_DIR"'/pairs.json ]; then cat '"$REMOTE_DIR"'/pairs.json; else echo "(不存在)"; fi'
}

show_logs() {
  step "远程日志（最近 80 行）"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" 'logread | 筛选 tg-forwarder | tail -80'
    return 0
  fi
  remote_exec 'logread | grep -iE " (tg-forwarder|tg-forwarder-openwrt\.sh)\[[0-9]+\]:|procd:.*tg-forwarder" | tail -80'
}

follow_logs() {
  step "实时日志（Ctrl-C 返回）"
  [ "$DRY_RUN" -eq 1 ] && { printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" 'logread -f | grep -iE " (tg-forwarder|tg-forwarder-openwrt\.sh)\[[0-9]+\]:|procd:.*tg-forwarder"'; return 0; }
  remote_exec 'logread -f | grep -iE " (tg-forwarder|tg-forwarder-openwrt\.sh)\[[0-9]+\]:|procd:.*tg-forwarder"'
}

backup_remote_state() {
  local stamp target_dir archive remote_archive remote_stage remote_checksum local_checksum
  stamp="$(date +%Y%m%d-%H%M%S)"
  target_dir="$SCRIPT_DIR/backups/openwrt"
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
    for f in config.json pairs.json sources.json watermarks.json run_log.json message_map.json retry_queue.json tg_session.session tg_session.session-journal .env; do
      if [ -f \"\$f\" ]; then cp -p \"\$f\" '$remote_stage/' || exit 3; copied=1; fi
    done
    if [ -f /etc/config/tg-forwarder ]; then
      cp -p /etc/config/tg-forwarder '$remote_stage/openwrt-tg-forwarder.config' || exit 3
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

run_menu_action() {
  "$@" || warn "操作失败，可以修正配置或网络后重试。"
}

check_all() {
  check_local_tools
  check_connection
}

menu() {
  while true; do
    printf '\n%stelegram-forwarder / OpenWrt 管理器%s\n' "$C" "$N"
    printf '%s目标：%s%s:%s%s\n' "$D" "$N" "$SSH_HOST" "$REMOTE_DIR" "$N"
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
 11) 打开 SSH shell
 12) 检查本机工具和 SSH
 13) 设置/更新 Web 管理密码
 14) Telegram 扫码登录（自动停服务并备份旧 session）
  0) 退出
EOF
    printf '请选择 [0-14]: '
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
      11) run_menu_action open_shell ;;
      12) run_menu_action check_all; pause_screen ;;
      13) run_menu_action configure_auth; pause_screen ;;
      14) run_menu_action telegram_login; pause_screen ;;
      0) return 0 ;;
      *) warn "无效选项：$choice" ;;
    esac
  done
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --status|--logs|--follow-logs|--deploy|--sync|--init|--auth|--login|--start|--stop|--restart|--backup|--shell|--check)
      ACTION="${1#--}"; shift ;;
    --print-runtime) ACTION="print-runtime"; shift ;;
    --menu) ACTION="menu"; shift ;;
    --host) [ "$#" -ge 2 ] || die "--host 需要参数"; SSH_HOST="$2"; HOST_OVERRIDE_SET=1; shift 2 ;;
    --remote-dir) [ "$#" -ge 2 ] || die "--remote-dir 需要参数"; REMOTE_DIR="$2"; REMOTE_OVERRIDE_SET=1; shift 2 ;;
    --config) [ "$#" -ge 2 ] || die "--config 需要参数"; CONFIG_FILE="$2"; shift 2
      [ -f "$CONFIG_FILE" ] || die "配置文件不存在：$CONFIG_FILE"
      # shellcheck disable=SC1090
      . "$CONFIG_FILE"
      if [ "$HOST_OVERRIDE_SET" -eq 0 ]; then SSH_HOST="${TG_FORWARDER_SSH_HOST:-$SSH_HOST}"; fi
      if [ "$REMOTE_OVERRIDE_SET" -eq 0 ]; then REMOTE_DIR="${TG_FORWARDER_REMOTE_DIR:-$REMOTE_DIR}"; fi
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
  auth) configure_auth ;;
  login) telegram_login ;;
  start|stop|restart) service_action "$ACTION" ;;
  backup) backup_remote_state ;;
  shell) open_shell ;;
  check) check_local_tools; check_connection ;;
  print-runtime) emit_runtime_runner ;;
  *) die "未知动作：$ACTION" ;;
esac
