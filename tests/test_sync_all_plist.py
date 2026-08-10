"""单元测试: sync-all launchd 配置从独立工作目录启动。"""

import plistlib
from pathlib import Path


def test_launchd_sync_all_changes_to_project_root_before_running():
    plist_path = (
        Path(__file__).parents[1]
        / "config"
        / "launchd"
        / "com.quantpylab.sync-all.plist"
    )
    config = plistlib.loads(plist_path.read_bytes())
    arguments = config["ProgramArguments"]

    assert arguments[:2] == ["/bin/sh", "-c"]
    assert config["WorkingDirectory"] == "/"
    command = arguments[2]
    assert "project volume is not mounted" in command
    assert "exit 1; fi" in command
    assert 'mkdir -p "$PROJECT_ROOT/logs"' in command
    assert 'cd "$PROJECT_ROOT" || exit 1' in command
    assert 'exec >> "$PROJECT_ROOT/logs/launchd_sync-all.out.log"' in command
    assert '2>> "$PROJECT_ROOT/logs/launchd_sync-all.err.log"' in command
    assert "project log redirection failed" in command
    assert command.index('cd "$PROJECT_ROOT"') < command.index("exec >>")
    assert command.index("exec >>") < command.index('exec "$PROJECT_ROOT')
    assert config["StandardOutPath"] == "/dev/null"
    assert config["StandardErrorPath"] == "/dev/null"
