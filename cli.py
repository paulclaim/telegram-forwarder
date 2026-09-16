#!/usr/bin/env python3
"""
Telegram Downloader CLI — Telethon version

Usage:
    py -3.11 cli.py login-qr
    py -3.11 cli.py list-chats
    py -3.11 cli.py list-topics --chat "-1002460585809"
    py -3.11 cli.py forward-topic --source "-1002460585809" --topic 1 --dest "-1003951264037"
    py -3.11 cli.py download --chat "-1002460585809" --type documents
    py -3.11 cli.py export --chat "-1002460585809" --output messages.json
    py -3.11 cli.py forward --source "-1002460585809" --dest "-1003951264037"
"""

import argparse
import asyncio
import getpass
import os
import sys
import time

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from downloader import TelegramDownloader, get_telegram_proxy, load_config


_CLEAR_TERMINAL = "\033[2J\033[3J\033[H"


def _qr_timeout(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须是整数秒数") from exc
    if not 60 <= seconds <= 900:
        raise argparse.ArgumentTypeError("必须在 60 到 900 秒之间")
    return seconds


def _render_login_qr(login_url: str) -> None:
    """Render a QR token without exposing its tg:// URL as terminal text."""
    import qrcode

    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=1,
        border=4,
    )
    qr.add_data(login_url)
    qr.make(fit=True)

    print(_CLEAR_TERMINAL, end="")
    print("请使用已登录的 Telegram 手机客户端扫码：")
    print("设置 → 设备 → 链接桌面设备\n")
    for row in qr.get_matrix():
        # Explicit backgrounds stay scannable with both dark and light themes.
        print(
            "".join("\033[40m  " if cell else "\033[47m  " for cell in row)
            + "\033[0m"
        )
    print("\033[0m\n二维码会在过期后自动刷新；按 Ctrl-C 取消。", flush=True)


def _qr_wait_seconds(qr_login, overall_remaining: float) -> float:
    expires_at = getattr(qr_login, "expires", None)
    token_remaining = expires_at.timestamp() - time.time() if expires_at else 60.0
    return max(1.0, min(overall_remaining, token_remaining))


async def cmd_login_qr(args):
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise RuntimeError("二维码登录需要交互式终端")
    if os.environ.get("TELETHON_SESSION_STRING"):
        raise RuntimeError(
            "二维码登录必须写入独立 session 文件；请先取消 TELETHON_SESSION_STRING"
        )

    config = load_config()
    session_name = os.environ.get("TELETHON_SESSION_FILE", "tg_session")
    session_path = str(os.path.join(os.path.dirname(__file__), session_name))
    client_kwargs = {}
    proxy = get_telegram_proxy()
    if proxy is not None:
        client_kwargs["proxy"] = proxy

    client = TelegramClient(
        session_path,
        config["api_id"],
        config["api_hash"],
        connection_retries=3,
        retry_delay=1,
        timeout=30,
        **client_kwargs,
    )
    await client.connect()
    try:
        if await client.is_user_authorized():
            print("当前 session 已授权，无需重新扫码。")
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + args.timeout
        qr_login = await client.qr_login()

        while True:
            overall_remaining = deadline - loop.time()
            if overall_remaining <= 0:
                raise RuntimeError(f"二维码登录在 {args.timeout} 秒内未完成")
            wait_task = asyncio.create_task(
                qr_login.wait(
                    timeout=_qr_wait_seconds(qr_login, overall_remaining)
                )
            )
            # QRLogin.wait() must register UpdateLoginToken before the QR can
            # be scanned, otherwise a very fast scan can miss the event.
            await asyncio.sleep(0)
            _render_login_qr(qr_login.url)
            try:
                await wait_task
                break
            except asyncio.TimeoutError:
                if deadline - loop.time() <= 0:
                    raise RuntimeError(f"二维码登录在 {args.timeout} 秒内未完成")
                await qr_login.recreate()
            except SessionPasswordNeededError:
                password = getpass.getpass("请输入 Telegram 两步验证密码（输入不显示）: ")
                if not password:
                    raise RuntimeError("两步验证密码不能为空")
                try:
                    await client.sign_in(password=password)
                finally:
                    password = ""
                break

        if not await client.is_user_authorized():
            raise RuntimeError("扫码完成，但 Telegram 未确认当前 session 已授权")
        print(_CLEAR_TERMINAL, end="")
        print("✓ Telegram 扫码登录成功。")
    finally:
        await client.disconnect()


def format_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def print_progress(progress):
    bar_width = 30
    filled = int(bar_width * progress.downloaded / max(progress.total_size, 1))
    bar = "█" * filled + "░" * (bar_width - filled)
    name = progress.filename[:35].ljust(35)
    speed = f"{format_size(int(progress.speed))}/s"
    if progress.status == "completed":
        print(f"\r  ✓ {name} [{bar}] Done!          ")
    elif progress.status == "failed":
        print(f"\r  ✗ {name} — Failed")
    else:
        print(f"\r  ⬇ {name} [{bar}] {progress.percent}% {speed}", end="", flush=True)


def print_message_progress(count, limit):
    if limit:
        print(f"\r  📋 Scanning: {count}/{limit}", end="", flush=True)
    else:
        print(f"\r  📋 Scanning: {count}", end="", flush=True)


async def cmd_list_chats(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chats = await dl.list_chats(limit=args.limit)
        if args.type:
            chats = [c for c in chats if c["type"] == args.type]

        print(f"\n{'─'*90}")
        print(f"  {'#':<4} {'ID':<16} {'Type':<12} {'Name':<30} {'Username':<20}")
        print(f"{'─'*90}")
        for i, chat in enumerate(chats, 1):
            username = f"@{chat['username']}" if chat["username"] else "—"
            print(f"  {i:<4} {str(chat['id']):<16} {chat['type']:<12} {chat['title'][:28]:<30} {username:<20}")
        print(f"{'─'*90}")
        print(f"  Total: {len(chats)} chats\n")


async def cmd_list_topics(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chat_id = args.chat
        try:
            chat_id = int(chat_id)
        except ValueError:
            pass

        topics = await dl.list_topics(chat_id)
        entity = await dl.client.get_entity(chat_id)
        chat_name = getattr(entity, "title", str(chat_id))

        print(f"\n📋 Topics in: {chat_name}")
        print(f"{'─'*60}")
        print(f"  {'#':<4} {'Topic ID':<12} {'Name':<30}")
        print(f"{'─'*60}")
        for i, topic in enumerate(topics, 1):
            print(f"  {i:<4} {str(topic['id']):<12} {topic['title'][:28]:<30}")
        print(f"{'─'*60}")
        print(f"  Total: {len(topics)} topics\n")


async def cmd_forward_topic(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        source = args.source
        dest = args.dest
        try:
            source = int(source)
        except ValueError:
            pass
        try:
            dest = int(dest)
        except ValueError:
            pass

        await dl.forward_topic(
            source_id=source,
            topic_id=args.topic,
            dest_id=dest,
            forward_type=args.type,
            limit=args.limit,
            delay=args.delay,
        )


async def cmd_download(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chat_id = args.chat
        try:
            chat_id = int(chat_id)
        except ValueError:
            pass

        await dl.download_chat(
            chat_id=chat_id,
            download_type=args.type,
            limit=args.limit,
            on_progress=print_progress,
            on_message_progress=print_message_progress,
        )


async def cmd_export(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chat_id = args.chat
        try:
            chat_id = int(chat_id)
        except ValueError:
            pass

        await dl.export_messages(
            chat_id=chat_id,
            output_file=args.output,
            limit=args.limit,
        )


async def cmd_forward(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        source = args.source
        dest = args.dest
        try:
            source = int(source)
        except ValueError:
            pass
        try:
            dest = int(dest)
        except ValueError:
            pass

        await dl.forward_chat(
            source_id=source,
            dest_id=dest,
            forward_type=args.type,
            limit=args.limit,
            delay=args.delay,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Telegram Downloader (Telethon)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # login-qr
    login_qr = sub.add_parser("login-qr", help="使用 Telegram 客户端扫码登录")
    login_qr.add_argument(
        "--timeout",
        type=_qr_timeout,
        default=300,
        metavar="SECONDS",
        help="等待扫码的总秒数（60-900，默认 300）",
    )

    # list-chats
    lc = sub.add_parser("list-chats")
    lc.add_argument("--limit", type=int, default=50)
    lc.add_argument("--type", choices=["private", "group", "supergroup", "channel"], default=None)

    # list-topics
    lt = sub.add_parser("list-topics")
    lt.add_argument("--chat", required=True)

    # forward-topic
    ft = sub.add_parser("forward-topic")
    ft.add_argument("--source", required=True)
    ft.add_argument("--topic", required=True, type=int)
    ft.add_argument("--dest", required=True)
    ft.add_argument("--type", choices=["all", "media", "documents", "messages", "docs_and_text"], default="all")
    ft.add_argument("--limit", type=int, default=0)
    ft.add_argument("--delay", type=float, default=0.5)

    # download
    dl = sub.add_parser("download")
    dl.add_argument("--chat", required=True)
    dl.add_argument("--type", choices=["all", "media", "documents", "messages"], default="all")
    dl.add_argument("--limit", type=int, default=0)

    # export
    ex = sub.add_parser("export")
    ex.add_argument("--chat", required=True)
    ex.add_argument("--output", default="messages.json")
    ex.add_argument("--limit", type=int, default=0)

    # forward
    fw = sub.add_parser("forward")
    fw.add_argument("--source", required=True)
    fw.add_argument("--dest", required=True)
    fw.add_argument("--type", choices=["all", "media", "documents", "messages"], default="all")
    fw.add_argument("--limit", type=int, default=0)
    fw.add_argument("--delay", type=float, default=1.0)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "login-qr": cmd_login_qr,
        "list-chats": cmd_list_chats,
        "list-topics": cmd_list_topics,
        "forward-topic": cmd_forward_topic,
        "download": cmd_download,
        "export": cmd_export,
        "forward": cmd_forward,
    }
    try:
        asyncio.run(commands[args.command](args))
    except RuntimeError as exc:
        if args.command != "login-qr":
            raise
        print(f"二维码登录失败：{exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        if args.command != "login-qr":
            raise
        print("\n二维码登录已取消。", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
