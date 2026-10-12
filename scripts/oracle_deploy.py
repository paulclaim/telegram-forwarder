#!/usr/bin/env python3
"""Deploy a GitHub Actions release while preserving production runtime state."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

# Exclusions protect both source and destination, including during --delete.
EXCLUDES = [
    '.git', '.venv*', 'venv', '__pycache__', '*.pyc', '*.pyo',
    '*.session*', 'tg_session*', 'session_string*', '*.db*', '*.sqlite*',
    'config.json', 'pairs.json', 'sources.json', 'watermarks.json',
    'message_map.json', 'run_log.json', 'retry_queue.json', '.pairs_from_env.json',
    'env', '.env*', 'env.*', 'data/', 'data_acct*/', 'data-acct*/', 'downloads', 'temp', 'backups*',
    'secrets', 'SOP', 'archive', 'bot', '.tg-forwarder-root',
    '.claude', '.spec-workflow', '.codeartsdoer', '.arts', '.DS_Store',
]


def run(*args: str, capture: bool = False) -> str:
    result = subprocess.run(args, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None,
                            timeout=600, env={**os.environ, 'GIT_TERMINAL_PROMPT': '0'})
    return result.stdout.strip() if capture else ''


def sync(source: Path, target: Path, excludes: list[str]) -> None:
    run('rsync', '-a', '--delete', '--delay-updates',
        *[f'--exclude={pattern}' for pattern in excludes], f'{source}/', f'{target}/')


def save(path: Path, value: dict) -> None:
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def read_record(state: Path) -> dict:
    record = state / 'version.json'
    return json.loads(record.read_text()) if record.exists() else {}


def service_state(service: str) -> str:
    return run('systemctl', 'show', service, '--property=ActiveState', '--value', capture=True)


def service_action(action: str, service: str) -> None:
    prefix = [] if os.geteuid() == 0 else ['sudo', '-n']
    run(*prefix, 'systemctl', action, service)


def wait_healthy(service: str, url: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                payload = json.load(response)
            if (service_state(service) == 'active' and payload.get('ok')
                    and payload.get('telegram_ready') and payload.get('telegram_connected')
                    and not payload.get('telegram_session_error')):
                return
        except (OSError, ValueError):
            pass
        time.sleep(2)
    raise RuntimeError(f'Health check failed after {timeout}s: {url}')


def export(archive: Path, target: Path) -> None:
    with tarfile.open(archive) as contents:
        for member in contents.getmembers():
            if (member.name.startswith('/') or '..' in Path(member.name).parts
                    or member.issym() or member.islnk() or not (member.isfile() or member.isdir())):
                raise RuntimeError(f'Unsafe archive entry: {member.name}')
        contents.extractall(target)
    for name in ('server.py', 'automate.py', 'requirements.txt'):
        if not (target / name).is_file():
            raise RuntimeError(f'Incomplete release: missing {name}')
    # No application imports or Telegram sessions during syntax validation.
    for path in target.rglob('*.py'):
        compile(path.read_bytes(), str(path), 'exec')


def deploy(config: dict, state: Path, archive: Path, sha: str) -> None:
    target = Path(config['target']).resolve()
    marker = target / '.tg-forwarder-root'
    if target == Path('/') or not marker.is_file() or marker.read_text().strip() != 'telegram-forwarder-pro':
        raise RuntimeError('Invalid deployment directory marker')
    service = config['service']
    before = service_state(service)
    if before not in ('active', 'inactive'):
        raise RuntimeError(f'Refusing deployment while service state is {before}')
    was_running = before == 'active'
    excludes = EXCLUDES + config.get('extra_excludes', [])
    venv = target / '.venv'
    if not (venv / 'bin/python').exists():
        raise RuntimeError('Production .venv is missing')
    backup = state / 'rollback-code'
    backup.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='stage-', dir=state) as work:
        stage = Path(work) / 'code'
        stage.mkdir()
        export(archive, stage)
        changed_requirements = ((stage / 'requirements.txt').read_bytes() !=
                                (target / 'requirements.txt').read_bytes())
        candidate = state / 'venvs' / sha
        old_link = os.readlink(venv) if venv.is_symlink() else None
        original_venv = state / 'original-venv'
        if changed_requirements:
            if candidate.exists():
                if candidate.resolve() == venv.resolve():
                    raise RuntimeError('Candidate venv is currently active')
                shutil.rmtree(candidate)
            candidate.parent.mkdir(exist_ok=True)
            run('python3', '-m', 'venv', str(candidate))
            run(str(candidate / 'bin/pip'), 'install', '-r', str(stage / 'requirements.txt'))
        python = candidate / 'bin/python' if changed_requirements else venv / 'bin/python'
        run(str(python), '-c', 'import telethon, quart, hypercorn, openpyxl, qrcode')
        sync(target, backup, excludes)
        save(state / 'rollback.json', {'commit': sha, 'previous': read_record(state),
                                     'was_running': was_running, 'time': time.time()})
        venv_swapped = False
        try:
            if was_running:
                service_action('stop', service)
            sync(stage, target, excludes)
            if changed_requirements:
                if old_link is None:
                    if original_venv.exists():
                        raise RuntimeError('original-venv already exists; refusing to overwrite it')
                    venv.rename(original_venv)
                else:
                    venv.unlink()
                venv_swapped = True
                venv.symlink_to(candidate, target_is_directory=True)
            if was_running:
                service_action('start', service)
                wait_healthy(service, config['health_url'], config.get('health_timeout', 90))
            save(state / 'version.json', {'commit': sha, 'mode': 'deployed',
                                         'time': time.time(), 'service_running': was_running})
        except Exception:
            print('Deployment failed; restoring previous code and environment', flush=True)
            service_action('stop', service)
            sync(backup, target, excludes)
            if venv_swapped:
                if venv.is_symlink():
                    venv.unlink()
                if old_link is None:
                    original_venv.rename(venv)
                else:
                    venv.symlink_to(old_link, target_is_directory=True)
            if was_running:
                service_action('start', service)
                wait_healthy(service, config['health_url'], config.get('health_timeout', 90))
            raise
    keep = {venv.resolve()}
    if old_link is not None:
        keep.add((target / old_link).resolve())
    for directory in (state / 'venvs').glob('*'):
        if directory.is_dir() and directory.resolve() not in keep:
            shutil.rmtree(directory)
    print(f'Deployed {sha}; service {"running and healthy" if was_running else "kept stopped"}', flush=True)


def apply_release(config: dict, archive: Path, sha: str) -> None:
    if len(sha) != 40 or any(c not in '0123456789abcdef' for c in sha):
        raise ValueError('Invalid commit SHA')
    state = Path(config['state_dir']).resolve()
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'deploy.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if read_record(state).get('commit') == sha:
            print(f'Already deployed: {sha}', flush=True)
            return
        deploy(config, state, archive, sha)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--commit', required=True)
    args = parser.parse_args()
    apply_release(json.loads(Path(args.config).read_text()), args.archive, args.commit)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'Auto-deploy failed: {error}', file=sys.stderr, flush=True)
        sys.exit(1)
