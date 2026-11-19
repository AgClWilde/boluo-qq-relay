#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 - "$script_dir" "$@" <<'PY'
"""Install the standalone release without elevated permissions."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import urlsplit


def fail(message):
    raise SystemExit("安装失败：" + message)


def safe_path(value):
    path = Path(value).expanduser().absolute()
    # Systemd expands % and $ even in some quoted fields. Reject special paths
    # instead of allowing unit directives or environment expansions to enter it.
    if (str(path) != str(path).strip() or any(ord(c) < 32 for c in str(path))
            or any(c in str(path) for c in '\\"%$*?[]')):
        fail("安装路径不能包含首尾空白、控制字符、反斜杠、双引号、%、$ 或通配符。")
    return path


def quote(value):
    return '"' + str(value) + '"'


def render_unit(template, prefix):
    fields = {"WORKING_DIRECTORY": prefix, "PYTHON": prefix / ".venv/bin/python",
              "RELAY": prefix / "relay.py", "CONFIG": prefix / "config.json",
              "ENVIRONMENT_FILE": prefix / "token.env"}
    for key, value in fields.items():
        # ExecStart splits words; path directives take the entire raw value.
        rendered = quote(value) if key in {"PYTHON", "RELAY", "CONFIG"} else str(value)
        template = template.replace("@" + key + "@", rendered)
    if re.search(r"@[A-Z_]+@", template):
        fail("服务模板含未识别字段。")
    return template


def run(command):
    try:
        subprocess.run(command, check=True)
    except (OSError, subprocess.CalledProcessError):
        fail("命令执行失败：" + command[0] + "；修复环境后可重新运行安装器。")


def main(source, arguments):
    parser = argparse.ArgumentParser(description="安装菠萝频道提醒服务（Linux / systemd 用户服务）")
    parser.add_argument("--prefix", default=str(Path.home() / ".local/share/boluo-qq-relay"))
    parser.add_argument("--admin-qq", action="append", metavar="QQ", help="已弃用，保留旧版命令行兼容；所有 QQ 用户均可使用提醒")
    parser.add_argument("--onebot-url", default="ws://127.0.0.1:3001")
    parser.add_argument("--napcat-config", metavar="PATH", help="读取已有 NapCat 配置；不会修改该文件")
    parser.add_argument("--napcat-port", type=int, help="NapCat 正向 WebSocket 端口，默认取 onebot-url 的端口")
    parser.add_argument("--enable", action="store_true", help="安装后立即启用并启动用户服务")
    parser.add_argument("--dry-run", action="store_true", help="仅检查安装计划，不写文件、联网或启动服务")
    args = parser.parse_args(arguments)
    if sys.version_info < (3, 10):
        fail("需要 Python 3.10 或更高版本。")
    if not args.dry_run and not sys.platform.startswith("linux"):
        fail("自动安装仅支持 Linux；--dry-run 可在其他系统检查参数。")
    if args.admin_qq:
        print("提示：--admin-qq 已弃用并被忽略；所有 QQ 用户均可使用 .boluo提醒。")
    try:
        endpoint = urlsplit(args.onebot_url)
        port = endpoint.port if endpoint.port is not None else (443 if endpoint.scheme == "wss" else 80)
        if not 1 <= port <= 65535 or any(c.isspace() for c in args.onebot_url) or endpoint.scheme not in {"ws", "wss"} or not endpoint.hostname or endpoint.username is not None or endpoint.password is not None or endpoint.query or endpoint.fragment:
            raise ValueError
    except ValueError:
        fail("--onebot-url 须为 ws:// 或 wss:// 地址，不能含账号、密码、查询参数或片段。")
    if args.napcat_port is not None and (not args.napcat_config or not 1 <= args.napcat_port <= 65535):
        fail("--napcat-port 须为 1–65535，且必须同时提供 --napcat-config。")
    prefix = safe_path(args.prefix)
    unit_dir = safe_path(Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "systemd/user")
    unit_path = unit_dir / "boluo-qq-relay.service"
    if prefix.is_symlink():
        fail("安装目录不能是符号链接。")
    config = {
        "database": "state/relay.sqlite3",
        "onebot_url": args.onebot_url,
        "onebot_token_env": "BOLUO_ONEBOT_TOKEN",
        "rescan_seconds": 300,
        "send_interval": 1.2,
    }
    if args.napcat_config:
        napcat = Path(args.napcat_config).expanduser().resolve()
        try:
            document = json.loads(napcat.read_text())
            servers = document["network"]["websocketServers"]
            selected_port = args.napcat_port or port
            found = any(s.get("enable") is True and int(s.get("port", 0)) == selected_port for s in servers)
            if not found:
                fail("NapCat 配置没有匹配端口的已启用正向 WebSocket。")
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            fail("无法读取有效的 NapCat 配置。")
        config["napcat_config"] = str(napcat)
        config["napcat_ws_port"] = selected_port

    runtime = ["relay.py", "boluo.py", "onebot.py", "commands.py", "notifications.py", "requirements.txt", "config.example.json", "boluo-qq-relay.service.in"]
    for name in runtime:
        original, target = source / name, prefix / name
        if not original.is_file() or original.is_symlink():
            fail("发行包缺少正常文件：" + name)
        if target.is_symlink() or (target.exists() and (not target.is_file() or target.read_bytes() != original.read_bytes())):
            fail("安装目录已有不同内容：" + name + "；请选择新目录或先自行备份并移走旧版本。")
    for name in ("config.json", "token.env"):
        target = prefix / name
        if target.is_symlink() or (target.exists() and not target.is_file()):
            fail("已有配置不是普通文件：" + name)
    effective_config = config
    config_path = prefix / "config.json"
    if config_path.exists():
        try:
            effective_config = json.loads(config_path.read_text())
            if not isinstance(effective_config, dict):
                raise ValueError
        except (OSError, ValueError):
            fail("已有 config.json 不是有效的配置对象。")
    if args.enable and not effective_config.get("napcat_config"):
        token_file = prefix / "token.env"
        key = effective_config.get("onebot_token_env", "BOLUO_ONEBOT_TOKEN")
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            fail("配置中的 onebot_token_env 无效。")
        if not token_file.is_file() or token_file.stat().st_mode & 0o077 or not re.search(r"^\s*" + re.escape(key) + r"=", token_file.read_text(), re.MULTILINE):
            fail("启用环境令牌模式前，请先在安装目录创建权限为 600 的 token.env，包含 " + key + "=...；可先不带 --enable 安装。")
    if (prefix / "state").is_symlink() or ((prefix / "state").exists() and not (prefix / "state").is_dir()):
        fail("已有 state 不是普通目录。")
    if (prefix / ".venv").is_symlink() or ((prefix / ".venv").exists() and not (prefix / ".venv/pyvenv.cfg").is_file()):
        fail("已有 .venv 不是虚拟环境。")
    unit = render_unit((source / "boluo-qq-relay.service.in").read_text(), prefix)
    if unit_path.is_symlink() or (unit_path.exists() and (not unit_path.is_file() or unit_path.read_text() != unit)):
        fail("已有同名用户服务指向其他内容；请先检查 " + str(unit_path))
    if args.dry_run:
        print("安装计划检查通过；未写入文件、安装依赖或启动服务。")
        print("安装目录：" + str(prefix))
        print("用户服务：" + str(unit_path))
        return
    if shutil.which("systemctl") is None:
        fail("找不到 systemctl；此安装器需要 systemd 用户服务。")
    os.umask(0o077)
    prefix.mkdir(mode=0o700, parents=True, exist_ok=True)
    (prefix / "state").mkdir(mode=0o700, exist_ok=True)
    for name in runtime:
        if not (prefix / name).exists():
            shutil.copyfile(source / name, prefix / name)
    if not config_path.exists():
        with config_path.open("x") as handle:
            json.dump(config, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    else:
        print("保留已有 config.json 和订阅状态；本次参数不会改写已有配置。")
    if not (prefix / ".venv/bin/python").exists():
        run([sys.executable, "-m", "venv", str(prefix / ".venv")])
    run([str(prefix / ".venv/bin/python"), "-m", "pip", "--isolated", "install", "--index-url", "https://pypi.org/simple", "-r", str(prefix / "requirements.txt")])
    unit_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not unit_path.exists():
        with unit_path.open("x") as handle:
            handle.write(unit)
    run(["systemctl", "--user", "daemon-reload"])
    if args.enable:
        run(["systemctl", "--user", "enable", "--now", "boluo-qq-relay.service"])
        print("安装完成，用户服务已启用。")
    else:
        print("安装完成。确认连接配置后运行：systemctl --user enable --now boluo-qq-relay.service")
    if not effective_config.get("napcat_config"):
        print("使用环境令牌时，请手动创建安装目录下的 token.env，并将权限设为 600。")


if __name__ == "__main__":
    try:
        main(Path(sys.argv[1]), sys.argv[2:])
    except OSError:
        fail("文件操作失败；请检查路径和权限。")
PY
