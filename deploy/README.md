# Oracle VPS 的 GitHub Actions 部署

工作流：`.github/workflows/deploy-oracle.yml`。推送到
`paulclaim/telegram-forwarder` 的 `main` 或在 Actions 页面手动运行，即触发部署。
PR、其他分支和 fork 不执行生产部署。部署使用 concurrency 串行执行，
避免中途取消正在替换代码的任务。

工作流先安装依赖、运行 unittest 和 retry_utils 自检，再用专用 SSH 密钥
上传该次提交的 Git archive、部署配置和临时部署脚本。VPS 不轮询 GitHub，
也不安装 self-hosted runner；临时文件在部署结束后清理。

## 一次性配置

在仓库 Actions secrets 中设置：

| Secret | 内容 |
| --- | --- |
| ORACLE_HOST | VPS 地址 |
| ORACLE_USER | SSH 用户，当前为 ubuntu |
| ORACLE_PORT | SSH 端口，当前为 22 |
| ORACLE_SSH_KEY | 专门为此仓库生成的 SSH 私钥 |
| ORACLE_KNOWN_HOSTS | 经可信连接确认的服务器 SSH 主机公钥行 |

对应公钥加入服务器用户的 `~/.ssh/authorized_keys`，禁用密钥的转发和 PTY。
用户需要对部署目录有写权限，能非交互执行 `sudo systemctl start/stop tg-forwarder.service`。
现有 ubuntu 管理用户已具备这些权限。GitHub runner 必须能连接 VPS 的 SSH 端口。

`deploy/oracle-deploy.json` 指定远程目录、服务、健康检查和部署备份目录。
部署状态目录 `/var/lib/tg-forwarder-deploy` 需预先创建并交给部署用户。
远程项目目录需有内容为 `telegram-forwarder-pro` 的 `.tg-forwarder-root` 标记，
以及现有 `.venv`、env、配置和 Telegram session。

## 运行态保护及回滚

同步和回滚排除配置、session、水位、消息映射、日志、重试队列、env、
数据目录和媒体临时目录，即使意外提交到 GitHub 也不会覆盖它们。
自定义状态路径须补充到 `extra_excludes` 中。

脚本先检查代码语法和依赖；requirements 改变时先创建独立 venv。
保存旧代码后，停止原进程，再同步代码和切换 venv，避免两个进程共用 session。
原服务运行时启动新代码并检查 Web、Telegram 连接；失败则恢复旧代码和 venv，
工作流显示失败。原服务停止时保留停止状态，此时不做运行时健康检查。
保留最近一次代码备份，回滚不撤销转发期间产生的水位或消息映射。

在 GitHub Actions 页面查看每次结果。部署失败后修复代码再推送，或使用 Re-run jobs。
已成功部署的相同提交会跳过，失败的提交允许重试。

```bash
# 查看服务器记录的部署版本
ssh sanjose 'cat /var/lib/tg-forwarder-deploy/version.json'
# 当前转发服务及健康检查
ssh sanjose 'systemctl status tg-forwarder.service --no-pager'
ssh sanjose 'curl -fsS http://127.0.0.1:5000/healthz'
```

手动同步前，确认 Actions 中没有部署任务正在执行。旧的
`tg-forwarder-autodeploy.timer` 应禁用，避免重复部署。
