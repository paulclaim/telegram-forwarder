#!/usr/bin/env bash
# manage-openwrt.sh — 在 macOS 终端交互式管理 OpenWrt 上的 telegram-forwarder
#
# 默认使用 ~/.ssh/config 中的 Host openwrt，远程目录为 /root/tg-forwarder。
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
  --start / --stop      启动 / 停止服务
  --restart             重启服务
  --backup              备份远程配置、会话和运行态文件到 backups/openwrt/
  --shell               打开 OpenWrt SSH shell
  --check               检查本机工具和 SSH 连通性
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
  require_command scp
  if [ "$ACTION" = "deploy" ] || [ "$ACTION" = "sync" ] || [ "$ACTION" = "menu" ]; then
    require_command rsync
    log "本机依赖检查通过（ssh/scp/rsync）"
  else
    log "本机依赖检查通过（ssh/scp）"
  fi
}

invoke_deploy() {
  local mode="$1"
  case "$mode" in
    deploy) mode="同步代码并重启" ;;
    sync) mode="只同步代码" ;;
    init) mode="首次初始化" ;;
    *) die "内部错误：未知部署模式 $1" ;;
  esac
  step "${mode}（${SSH_HOST}:${REMOTE_DIR}）"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'TG_FORWARDER_SSH_HOST=%q TG_FORWARDER_REMOTE_DIR=%q TG_FORWARDER_SSH_OPTS=%q %q' "$SSH_HOST" "$REMOTE_DIR" "$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh"
    case "$1" in
      deploy) printf '\n' ;;
      sync) printf ' --no-restart\n' ;;
      init) printf ' --init\n' ;;
    esac
    return 0
  fi
  case "$1" in
    deploy) TG_FORWARDER_SSH_HOST="$SSH_HOST" TG_FORWARDER_REMOTE_DIR="$REMOTE_DIR" TG_FORWARDER_SSH_OPTS="$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh" ;;
    sync) TG_FORWARDER_SSH_HOST="$SSH_HOST" TG_FORWARDER_REMOTE_DIR="$REMOTE_DIR" TG_FORWARDER_SSH_OPTS="$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh" --no-restart ;;
    init) TG_FORWARDER_SSH_HOST="$SSH_HOST" TG_FORWARDER_REMOTE_DIR="$REMOTE_DIR" TG_FORWARDER_SSH_OPTS="$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh" --init ;;
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
  step "同步安全版 procd 服务脚本"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'TG_FORWARDER_SSH_HOST=%q TG_FORWARDER_REMOTE_DIR=%q TG_FORWARDER_SSH_OPTS=%q %q --service\n' \
      "$SSH_HOST" "$REMOTE_DIR" "$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh"
    return 0
  fi
  TG_FORWARDER_SSH_HOST="$SSH_HOST" TG_FORWARDER_REMOTE_DIR="$REMOTE_DIR" TG_FORWARDER_SSH_OPTS="$SSH_OPTS" \
    "$SCRIPT_DIR/deploy-openwrt.sh" --service
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

show_status() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'TG_FORWARDER_SSH_HOST=%q TG_FORWARDER_REMOTE_DIR=%q TG_FORWARDER_SSH_OPTS=%q %q --status\n' "$SSH_HOST" "$REMOTE_DIR" "$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh"
    return 0
  fi
  TG_FORWARDER_SSH_HOST="$SSH_HOST" TG_FORWARDER_REMOTE_DIR="$REMOTE_DIR" TG_FORWARDER_SSH_OPTS="$SSH_OPTS" \
    "$SCRIPT_DIR/deploy-openwrt.sh" --status
}

show_logs() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'TG_FORWARDER_SSH_HOST=%q TG_FORWARDER_REMOTE_DIR=%q TG_FORWARDER_SSH_OPTS=%q %q --logs\n' "$SSH_HOST" "$REMOTE_DIR" "$SSH_OPTS" "$SCRIPT_DIR/deploy-openwrt.sh"
    return 0
  fi
  TG_FORWARDER_SSH_HOST="$SSH_HOST" TG_FORWARDER_REMOTE_DIR="$REMOTE_DIR" TG_FORWARDER_SSH_OPTS="$SSH_OPTS" \
    "$SCRIPT_DIR/deploy-openwrt.sh" --logs
}

follow_logs() {
  step "实时日志（Ctrl-C 返回）"
  [ "$DRY_RUN" -eq 1 ] && { printf 'ssh %s %s %q\n' "$SSH_OPTS" "$SSH_HOST" 'logread -f | grep -iE "tg-forwarder|scheduler|mode=|cycle|forwarded|error|exception|traceback"'; return 0; }
  remote_exec 'logread -f | grep -iE "tg-forwarder|scheduler|mode=|cycle|forwarded|error|exception|traceback"'
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
  0) 退出
EOF
    printf '请选择 [0-13]: '
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
      0) return 0 ;;
      *) warn "无效选项：$choice" ;;
    esac
  done
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --status|--logs|--follow-logs|--deploy|--sync|--init|--auth|--start|--stop|--restart|--backup|--shell|--check)
      ACTION="${1#--}"; shift ;;
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
  start|stop|restart) service_action "$ACTION" ;;
  backup) backup_remote_state ;;
  shell) open_shell ;;
  check) check_local_tools; check_connection ;;
  *) die "未知动作：$ACTION" ;;
esac
