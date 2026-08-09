"""单元测试: 日志配置 (文件 handler 分流: app.log 全量, error.log 仅 Error)"""

import logging
from logging.handlers import TimedRotatingFileHandler

from utils.logger import setup_logger


class TestLoggerConfig:
    def _build(self, monkeypatch, tmp_path):
        monkeypatch.setattr("utils.logger.LOG_DIR", tmp_path)
        logger = setup_logger(f"test_logger_{tmp_path.name}")
        self.logger = logger
        return logger

    def test_file_handlers_split_app_and_error(self, monkeypatch, tmp_path):
        logger = self._build(monkeypatch, tmp_path)
        file_handlers = [
            h for h in logger.handlers if isinstance(h, TimedRotatingFileHandler)
        ]
        assert len(file_handlers) == 2

        err = next(h for h in file_handlers if h.baseFilename.endswith("error.log"))
        assert err.level == logging.ERROR

    def test_info_and_error_split_across_files(self, monkeypatch, tmp_path):
        logger = self._build(monkeypatch, tmp_path)
        logger.info("普通信息日志")
        logger.error("错误日志内容")

        app_content = (tmp_path / "app.log").read_text(encoding="utf-8")
        err_content = (tmp_path / "error.log").read_text(encoding="utf-8")
        assert "普通信息日志" in app_content
        assert "错误日志内容" in app_content
        assert "普通信息日志" not in err_content
        assert "错误日志内容" in err_content

    def teardown_method(self):
        logger = getattr(self, "logger", None)
        if logger is None:
            return
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
