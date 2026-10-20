# Repository Guidelines

## 项目概述
Telethon 用户客户端频道/话题镜像转发器：支持原子 watermark、原生批量转发、受保护源自动 copy-mode、live edit/delete、Quart Web UI。

## 常用命令
```bash
# 安装依赖
pip install -r requirements.txt

# 配置模板
cp config.example.json config.json
cp pairs.example.json pairs.json

# 启动 Web UI 与转发进程
python server.py
```

## 关键生产规范
- **禁止提交敏感文件**：严禁提交 `config.json`、`pairs.json`、`*.session`、`*.watermarks.json`、`.env` 等。
- **并发与锁机制**：写 watermark / message_map 必须锁 + 原子 replace，同一 data 目录禁止多进程并行。

## 文档归属与环境边界规范（强制）

- **通用工程资产定位**：本项目所有文档（`README.md`、`Dockerfile`、通用配置示例）必须保持高度通用性，便于向上游（upstream）同步或在任意独立环境部署。
- **严禁宿主机私有信息倒灌**：严禁将本台宿主机（San Jose VPS）的私有信息（如生产反代域名 `tgforward.wipoorg.com`、宿主机内部生产路径 `/opt/tg-forwarder`、生产 session 与环境变量）提交至本项目仓库。
- **宿主机专属运维分离**：若涉及本台 VPS 上的特化部署（如 Systemd 服务单元 `tg-forwarder.service`、Caddy 反代与 Basic Auth 认证、宿主机并发参数与回环端口调优记录），必须在根目录运维仓库 `github-sanjose/doc/11-service-tg-forwarder.md` 及故障记录中维护，严禁写入本项目仓库。
