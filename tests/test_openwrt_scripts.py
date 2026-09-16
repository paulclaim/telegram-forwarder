from __future__ import annotations

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MANAGER_SCRIPT = REPO_ROOT / "manage-openwrt.sh"


class OpenWrtRuntimeReadyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.temp_root = Path(self.temporary_directory.name)
        self.fake_bin = self.temp_root / "bin"
        self.fake_bin.mkdir()
        self.ntp_hook = self.temp_root / "25-dnsmasqsec"
        self.time_marker = self.temp_root / "dnsmasqsec"
        self.runtime_script = self.temp_root / "tg-forwarder-openwrt.sh"
        self.ntp_hook.write_text("test hook\n", encoding="utf-8")
        self._write_executable(
            "ip",
            "#!/bin/sh\nprintf '%s\\n' '1.1.1.1 via 192.0.2.1 dev wan'\n",
        )
        render_environment = os.environ.copy()
        render_environment["TG_FORWARDER_MANAGER_CONFIG"] = str(
            self.temp_root / "missing-manager.conf"
        )
        rendered = subprocess.run(
            [str(MANAGER_SCRIPT), "--print-runtime"],
            cwd=REPO_ROOT,
            env=render_environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(rendered.returncode, 0, rendered.stdout + rendered.stderr)
        self.runtime_script.write_text(rendered.stdout, encoding="utf-8")
        self.runtime_script.chmod(
            self.runtime_script.stat().st_mode | stat.S_IXUSR
        )

    def _write_executable(self, name: str, content: str) -> None:
        path = self.fake_bin / name
        path.write_text(content, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    def _environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment["PATH"] = f"{self.fake_bin}{os.pathsep}{environment['PATH']}"
        environment["OPENWRT_NTP_VALID_HOOK"] = str(self.ntp_hook)
        environment["OPENWRT_TIME_VALID_MARKER"] = str(self.time_marker)
        environment["OPENWRT_READY_POLL_SECONDS"] = "1"
        environment["OPENWRT_READY_LOG_INTERVAL_SECONDS"] = "1"
        environment["OPENWRT_READY_CHECK_ONLY"] = "1"
        return environment

    def test_exits_immediately_when_route_and_ntp_are_ready(self) -> None:
        self.time_marker.write_text("ready\n", encoding="utf-8")

        result = subprocess.run(
            [str(self.runtime_script)],
            cwd=REPO_ROOT,
            env=self._environment(),
            capture_output=True,
            text=True,
            check=False,
            timeout=3,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("WAN 与系统时钟已就绪", result.stdout)
        self.assertEqual(result.stderr, "")

    def test_waits_until_ntp_marker_appears(self) -> None:
        self._write_executable(
            "sleep",
            "#!/bin/sh\nprintf '%s\\n' ready > \"$OPENWRT_TIME_VALID_MARKER\"\n",
        )

        result = subprocess.run(
            [str(self.runtime_script)],
            cwd=REPO_ROOT,
            env=self._environment(),
            capture_output=True,
            text=True,
            check=False,
            timeout=3,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("NTP=等待", result.stdout)
        self.assertIn("WAN 与系统时钟已就绪", result.stdout)
        self.assertEqual(result.stderr, "")


class OpenWrtDeployTargetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.temp_root = Path(self.temporary_directory.name)
        self.fake_bin = self.temp_root / "bin"
        self.fake_bin.mkdir()
        self._write_executable(
            "ssh",
            """#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift 2 ;;
    *) break ;;
  esac
done
[ "$#" -gt 0 ] || exit 2
shift
command="${1:-}"
case "$command" in
  *"marker="*) exec /bin/sh -c "$command" ;;
  *"cat >"*) cat >/dev/null; exit 0 ;;
  *) exit 0 ;;
esac
""",
        )
        self._write_executable("rsync", "#!/bin/sh\necho 'sent test files'\n")
        self._write_executable("scp", "#!/bin/sh\nexit 0\n")

    def _write_executable(self, name: str, content: str) -> None:
        path = self.fake_bin / name
        path.write_text(content, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    def _environment(self, remote_dir: Path) -> dict[str, str]:
        environment = os.environ.copy()
        environment["PATH"] = f"{self.fake_bin}{os.pathsep}{environment['PATH']}"
        environment["TG_FORWARDER_SSH_HOST"] = "test-openwrt"
        environment["TG_FORWARDER_REMOTE_DIR"] = str(remote_dir)
        environment["TG_FORWARDER_MANAGER_CONFIG"] = str(
            self.temp_root / "missing-manager.conf"
        )
        return environment

    def test_deploy_marks_empty_target_before_rsync(self) -> None:
        remote_dir = self.temp_root / "remote"

        result = subprocess.run(
            [str(MANAGER_SCRIPT), "--sync", "--yes"],
            cwd=REPO_ROOT,
            env=self._environment(remote_dir),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(
            (remote_dir / ".tg-forwarder-root").read_text(encoding="utf-8"),
            "telegram-forwarder-pro\n",
        )

    def test_deploy_rejects_unknown_nonempty_target(self) -> None:
        remote_dir = self.temp_root / "unknown"
        remote_dir.mkdir()
        (remote_dir / "unrelated.txt").write_text("keep\n", encoding="utf-8")

        result = subprocess.run(
            [str(MANAGER_SCRIPT), "--sync", "--yes"],
            cwd=REPO_ROOT,
            env=self._environment(remote_dir),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn("拒绝 rsync --delete", result.stderr)
        self.assertFalse((remote_dir / ".tg-forwarder-root").exists())

    def test_deploy_rejects_parent_path_segment(self) -> None:
        environment = self._environment(self.temp_root / "safe")
        environment["TG_FORWARDER_REMOTE_DIR"] = "/root/app/../other"

        result = subprocess.run(
            [str(MANAGER_SCRIPT), "--sync", "--yes"],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("不能包含空、. 或 .. 路径段", result.stderr)

    def test_sync_failure_is_not_hidden_by_followup_steps(self) -> None:
        remote_dir = self.temp_root / "remote-rsync-failure"
        self._write_executable("rsync", "#!/bin/sh\nexit 7\n")

        result = subprocess.run(
            [str(MANAGER_SCRIPT), "--sync", "--yes"],
            cwd=REPO_ROOT,
            env=self._environment(remote_dir),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("代码同步失败", result.stderr)

    def test_legacy_shell_entrypoints_are_fully_integrated(self) -> None:
        self.assertFalse((REPO_ROOT / "deploy-openwrt.sh").exists())
        self.assertFalse((REPO_ROOT / "tg-forwarder-openwrt.sh").exists())
        self.assertFalse((REPO_ROOT / "tg-forwarder.init").exists())
        self.assertFalse(
            (REPO_ROOT / "scripts" / "openwrt" / "wait-runtime-ready.sh").exists()
        )

    def test_login_dry_run_describes_safe_remote_login_flow(self) -> None:
        remote_dir = self.temp_root / "remote-login"

        result = subprocess.run(
            [str(MANAGER_SCRIPT), "--login", "--dry-run", "--yes"],
            cwd=REPO_ROOT,
            env=self._environment(remote_dir),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertLess(
            result.stdout.index("检查远程二维码登录依赖"),
            result.stdout.index("远程服务 stop"),
        )
        self.assertIn("远程服务 stop", result.stdout)
        self.assertIn("确认转发进程已停止", result.stdout)
        self.assertIn("备份", result.stdout)
        self.assertIn("python3 cli.py login-qr", result.stdout)
        self.assertIn("远程服务 start", result.stdout)
        self.assertIn("telegram_session_error=null", result.stdout)
        self.assertIn("Telegram 登录完成", result.stdout)

    def test_help_lists_telegram_login_action(self) -> None:
        result = subprocess.run(
            [str(MANAGER_SCRIPT), "--help"],
            cwd=REPO_ROOT,
            env=self._environment(self.temp_root / "remote-help"),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("--login", result.stdout)
        self.assertIn("扫码登录 Telegram", result.stdout)

    def test_login_transport_does_not_require_remote_base64_command(self) -> None:
        script = MANAGER_SCRIPT.read_text(encoding="utf-8")

        self.assertNotIn("'$login_script_b64' | base64 -d", script)
        self.assertIn("base64.b64decode", script)
        self.assertIn("set -eu\n      remote_script=", script)


if __name__ == "__main__":
    unittest.main()
