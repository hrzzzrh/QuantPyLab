"""单元测试: sync-all launchd 配置从独立工作目录启动并按周六凌晨触发。"""

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
    # launchd Weekday: 0 和 7 均为周日, 1..6=周一..周六; 缺失该键才表示每天
    assert config["StartCalendarInterval"] == {
        "Weekday": 6,
        "Hour": 3,
        "Minute": 0,
    }
    command = arguments[2]
    assert "project volume is not mounted" in command
    assert "exit 1; fi" in command
    assert 'mkdir -p "$PROJECT_ROOT/logs"' in command
    assert 'cd "$PROJECT_ROOT" || exit 1' in command
    assert command.index('cd "$PROJECT_ROOT"') < command.index('exec "$PROJECT_ROOT')
    assert "launchd_sync-all" not in command
    assert config["StandardOutPath"] == "/dev/null"
    assert config["StandardErrorPath"] == "/dev/null"
