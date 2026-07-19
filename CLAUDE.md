# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目

Telethon **用户客户端** 频道/话题镜像转发器：原子 watermark、原生批量 `forward_messages`（可选 `drop_author`）、受保护源自动 copy-mode、live edit/delete、`retry_queue`、Quart Web UI。不是 Bot API 转发器。

生产形态：Docker 单容器 / 每 Telegram 账号一个进程；OpenWrt 上也可 bare-metal 跑 `server.py`。

## 常用命令

```bash
# 安装（Python 3.11+）
pip install -r requirements.txt
# cryptg 装不上可从 requirements 删掉，功能仍可用，媒体更慢

cp config.example.json config.json   # api_id / api_hash
cp pairs.example.json pairs.json

# CLI 一次性操作（不碰 watermark）
python cli.py list-chats --limit 200
python cli.py list-topics --chat -100...
python cli.py forward --source -100... --dest -100... --type all --limit 100
python cli.py forward-topic --source -100... --topic 1234 --dest -100...
python cli.py download --chat -100... --type documents --limit 500
python cli.py export --chat -100... --output messages.json

# 无 UI 调度
python automate.py
RUN_ONCE_AND_EXIT=1 python automate.py

# 生产入口：Web UI + 同进程 scheduler + live handlers
DASH_USER=admin DASH_PASS='...' python server.py   # 默认 PORT=5000；无 DASH_PASS 则关鉴权
# GET /healthz 无鉴权

# Session → 容器用字符串
python convert_session.py tg_session

# 唯一“测试”：retry 分类器自检（无 pytest / ruff / mypy）
python retry_utils.py
```

### Docker

```bash
docker pull ghcr.io/apppurchasespro-hash/telegram-forwarder:latest
docker run -d --name tg-forwarder -p 5000:8080 \
  -v "$PWD/data:/app/data" \
  -e TELEGRAM_API_ID=... -e TELEGRAM_API_HASH=... \
  -e TELETHON_SESSION_STRING="$(cat session_string.txt)" \
  -e DASH_USER=admin -e DASH_PASS='...' \
  -e MSG_MAP_PATH=/app/data/message_map.json \
  ghcr.io/apppurchasespro-hash/telegram-forwarder:latest
```

镜像 `CMD`: `python server.py`。Dockerfile 默认 `STATE_PATH`/`PAIRS_PATH`/`RUN_LOG_PATH` 在 `/app/data/`，`PORT=8080`。CI：`.github/workflows/docker-publish.yml`（`main` / `v*.*.*` → GHCR）。

### OpenWrt

```bash
./deploy-openwrt.sh            # rsync 代码到 openwrt:/root/tg-forwarder + 重启
./deploy-openwrt.sh --init     # 首次：装依赖 + procd init
./deploy-openwrt.sh --no-restart
./deploy-openwrt.sh --status
./deploy-openwrt.sh --logs
```

- SSH 别名 `openwrt`，远端 `/root/tg-forwarder`，服务 `/etc/init.d/tg-forwarder` → `python3 server.py`（PORT=5000）
- **只同步代码，绝不覆盖** 远程 `watermarks.json` / `pairs.json` / `message_map.json` / `run_log.json` / `*.session`
- 依赖装：`telethon quart hypercorn openpyxl`（`cryptg` 在 OpenWrt 上常跳过）

## 架构

```
cli.py ──────────────► downloader.TelegramDownloader   # 一次性
automate.py ─────────► run_pair()  ◄── server.py       # 调度 / API / jobs / live
automate_multi.py ───► SQLite plan + native batch
retry_utils.py ──────► is_transient_error / ensure_connected / sleep_backoff
templates/index.html ► Quart SPA
```

| 模块 | 职责 |
|------|------|
| `downloader.py` | Telethon 客户端；list chats/topics；native `forward_batch`（含 forum `top_msg_id`）；copy 下载/上传；贴纸/视频 attributes；自适应超时 |
| `automate.py` | pairs / watermarks / message_map / retry_queue 路径与原子写；`run_pair`；replacements；pair 锁 |
| `server.py` | Quart UI/API；scheduler；全局 pause；bulk；clone-forum；`MessageEdited`/`MessageDeleted`；共享一个 `TelegramDownloader` |
| `retry_utils.py` | 瞬时网络错误分类与重连（OpenWrt WAN/DNS 闪断） |
| `cli.py` / `convert_session.py` | 薄 CLI；SQLite session → StringSession |

### 转发数据流

1. 读 `pairs.json` + `watermarks.json[pair].last_msg_id`
2. `is_source_protected` → `noforwards` 则 **copy**，否则 **native**（解析失败**假定 protected**）
3. `iter_messages(reverse=True, min_id=watermark[, reply_to=source_topic])` — 流式，不全量进内存
4. **Native**：最多 100 条/批；失败二进制拆批；成功 `record_mappings`；有 `replacements` 则 `edit_message(..., parse_mode=None)`（不强制 copy）
5. **Copy**：download-ahead（并发 1–3；>20MB 全局单槽）+ **顺序** upload；每成功一条才推进 watermark
6. 瞬时失败 → **不推进** watermark；同 msg 连续失败 ≥ `transient_skip_after`（默认 3）→ 入 `retry_queue` 并推进；永久 RPC 错误直接 skip+推进，**不入队**
7. Scheduler 周期末 `drain_retry_queue(max_items=3)`

### Live 镜像

- `message_map.json`: `{pair_name: {str(src_id): dest_id}}`，原子写 + asyncio lock
- 仅本进程转发过的消息有 map；历史消息无法 retro live-edit

## 状态文件

路径均可 env 覆盖（本地默认仓库根；Docker `/app/data/`）。

| 文件 | Env | 语义 |
|------|-----|------|
| `pairs.json` | `PAIRS_PATH`；首次可用 `PAIRS_JSON` | `interval_seconds` + `pairs[]` |
| `watermarks.json` | `STATE_PATH`；首次可用 `INITIAL_WATERMARKS_JSON` | `{name: {last_msg_id, last_scanned_id?, updated_at, ...}}` |
| `message_map.json` | `MSG_MAP_PATH` | live edit/delete |
| `retry_queue.json` | `RETRY_QUEUE_PATH` | park 的失败媒体：`pending` / `dead` |
| `run_log.json` | `RUN_LOG_PATH` | 近期事件环缓冲 |

### Watermark 不变量（改调度必读）

- **`save_pair_watermark`**：进程内锁 + 磁盘 RMW **只改一个 pair key**。禁止用“内存整份 state 再整文件写回”覆盖其他 key
- **默认拒绝回退**：新 `last_msg_id` < 磁盘值则丢弃；仅 `POST /api/pairs/<name>/watermark` 用 `allow_regression=True`
- **`last_scanned_id`**：iterator 扫过的最高源 msg id（含 type 不匹配）。`min_id = max(last_msg_id, last_scanned_id)`。改 `type` 后若需回扫历史，用 watermark repair 把两者一起回退
- **Per-pair asyncio lock**：同 pair 同时只能一个 `run_pair`（冲突 `skipped_locked`）；drain/repair/set-watermark 也拿同一把锁
- **`name` 是 watermark key**：改名 = 新 pair，会从空状态重跑
- 删除 pair **不删** watermark（重加时可避免重转历史）
- One-shot（`/api/forward-once`、CLI forward）**不碰** watermark
- 全局 pause 是**进程内存**，重启丢失
- **`message_map` 批量 flush**：内存立即更新；磁盘每 25 条或 run 结束 / shutdown 强制 flush

### Retry queue

- 卡死大文件：连续瞬时失败 → park + 主水位前进，避免整 pair 卡住
- 字段：`id=pair:src_id`、`attempts`、`retry_min_interval_seconds`（默认 900）、`retry_max_attempts`（默认 20，0=无限）→ 超限 `dead`
- Drain 成功只 `record_mappings` + 移除 item，不改主 watermark

## 配置

### 凭证

- Env：`TELEGRAM_API_ID` + `TELEGRAM_API_HASH`（可选 `DOWNLOAD_PATH`、`MAX_CONCURRENT_DOWNLOADS`）
- 或本地 `config.json`
- Session：`TELETHON_SESSION_STRING`（容器）或 `TELETHON_SESSION_FILE`（默认 `tg_session`）

### pairs.json 关键字段

| 字段 | 默认 | 说明 |
|------|------|------|
| `name` | 必填 | watermark / map / queue 键 |
| `source` / `dest` | 必填 | 通常 `-100…` |
| `source_topic` / `dest_topic` | 可选 | Forum；`1`=General。源 `>1` 才 `reply_to`；目的 `>1` 才 `top_msg_id` |
| `type` | `all` | `all`/`media`/`documents`/`messages`/`docs_and_text`；跳过 MessageService |
| `delay_seconds` | `1.0` | 条/批间延迟 |
| `max_per_run` | `0` 无限（API 新建 pair 默认 `200`） | 限制每轮 backlog |
| `drop_author` | `true` | 去转发头；需**发送号** Premium |
| `paused` | `false` | 调度跳过；手动 run 仍可 |
| `max_file_size_mb` | `0` | **仅 copy** 下载前跳过并**推进 watermark**（永久跳过该条；提高 cap 后需 repair/回退水位才能补传） |
| `copy_concurrency` | clamp 1–3 | copy 下载并发 |
| `transient_skip_after` | `3` | 同 msg 连续瞬时失败后 park；`0`=永不 |
| `retry_min_interval_seconds` | `900` | drain 冷却 |
| `retry_max_attempts` | `20` | 超限 → dead |
| `replacements` | `[]` | `{find,replace,regex?}`；native 路径 forward 后 edit |

Scheduler 间隔：`max(60, interval_seconds)`。

## Native vs Copy

| | Native | Copy |
|--|--------|------|
| 条件 | 源未保护 | `noforwards=True` |
| 路径 | 服务端 forward 100/批 | 本机 download → upload |
| replacements | forward 后 edit caption | 上传时 `text_override` |
| forum dest | `top_msg_id` | `reply_to=dest_topic` |

## 多账号

**一进程一 session**。VPS 上多账号 = 多容器（独立端口、`data-acctN/`、`.env-acctN`）。每个容器要独立 session + 独立 data 卷；建议显式 `MSG_MAP_PATH=/app/data/message_map.json`。完整 VPS runbook 在 gitignored 的 `SOP/`（克隆未必带）。

## 改代码时注意

- 转发主路径：`automate._run_pair_locked` + `downloader.forward_batch` / copy 上传
- 写 watermark / message_map / retry_queue 必须 **锁 + 原子 replace**；watermark **默认不可回退**
- 瞬时 vs 永久：`retry_utils.is_transient_error`；**FloodWait 不是 transient**（调用方 sleep 后重试）
- 不要为 replacements 强制走 copy
- 不要引入 `requirements.txt` 外的依赖，除非有明确必要
- 改 `is_transient_error` 后跑 `python retry_utils.py`

## 生产红线

- **禁止** 用空/本机 `watermarks.json` 覆盖生产卷 → 全量重转 + 目标重复
- **禁止** 同一 data 目录并行两个进程（锁只在进程内）
- **禁止** 提交 `config.json`、`*.session`、`.env`、真实 token（见 `.gitignore`）
- 部署：`deploy-openwrt.sh` 已刻意不覆盖状态；手写 rsync/scp 时不要带上 watermark/session
- 新环境若历史已同步：种子 `INITIAL_WATERMARKS_JSON` 或 watermark repair，不要从 0 跑
- `drop_author: true` 无 Premium 时实际无效；大 backlog 配 `max_per_run` + `delay_seconds`

## 已知限制

- 只能读已加入的会话
- 编辑后 Telegram 仍标 “(edited)”
- General topic（id=1）无法 `reply_to` 过滤；clone-forum 默认 `skip_general=True`
- Job cancel 在扫描/批间隙生效，**不能**打断进行中的单文件传输
- 无单元测试框架；`scripts/test_*.py` 是业务/冒烟脚本，不是 pytest
