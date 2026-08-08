"""单元测试: utils/retry.py 重试装饰器"""

import time

import pytest

from utils.retry import retry


class TestRetryDecorator:
    def test_success_on_first_try(self, monkeypatch):
        calls = []
        sleeps = []
        monkeypatch.setattr(time, "sleep", lambda d: sleeps.append(d))

        @retry(max_retries=2, delay=0.5)
        def func():
            calls.append(1)
            return "ok"

        assert func() == "ok"
        assert len(calls) == 1
        assert sleeps == []

    def test_success_after_retries(self, monkeypatch):
        calls = []
        sleeps = []
        monkeypatch.setattr(time, "sleep", lambda d: sleeps.append(d))

        @retry(max_retries=2, delay=0.5)
        def func():
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("transient")
            return "ok"

        assert func() == "ok"
        assert len(calls) == 3
        assert sleeps == [0.5, 0.5]

    def test_failure_after_max_retries(self, monkeypatch):
        calls = []
        monkeypatch.setattr(time, "sleep", lambda d: None)

        @retry(max_retries=2, delay=0.5)
        def func():
            calls.append(1)
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            func()
        assert len(calls) == 3

    def test_single_retry_attempts(self, monkeypatch):
        calls = []
        monkeypatch.setattr(time, "sleep", lambda d: None)

        @retry(max_retries=0, delay=0.5)
        def func():
            calls.append(1)
            raise RuntimeError("once")

        with pytest.raises(RuntimeError):
            func()
        assert len(calls) == 1

    def test_only_retries_specified_exceptions(self, monkeypatch):
        calls = []
        sleeps = []
        monkeypatch.setattr(time, "sleep", lambda d: sleeps.append(d))

        @retry(max_retries=2, delay=0.5, exceptions=(ValueError,))
        def func():
            calls.append(1)
            raise TypeError("not retried")

        with pytest.raises(TypeError):
            func()
        assert len(calls) == 1
        assert sleeps == []

    def test_preserves_function_metadata(self):
        @retry(max_retries=1, delay=0)
        def my_named_func():
            return 1

        assert my_named_func.__name__ == "my_named_func"
