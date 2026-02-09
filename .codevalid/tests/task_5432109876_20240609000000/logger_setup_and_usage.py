import pytest
import logging
import sys
from unittest import mock

from ekm_meter.utils.logger import setup_logger

def get_stream_handler(logger):
    """Helper to get StreamHandler attached to logger."""
    return [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]

def get_stdout_stream_handler(logger):
    """Helper to get StreamHandler attached to sys.stdout."""
    return [h for h in logger.handlers if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout]

def get_formatter(handler):
    """Helper to get formatter from handler."""
    return handler.formatter if hasattr(handler, 'formatter') else None

@pytest.mark.parametrize("logger_name", ["my_logger"])
def test_basic_logger_creation(logger_name):
    logger = setup_logger(logger_name)
    assert isinstance(logger, logging.Logger)
    assert logger.name == logger_name
    handlers = get_stream_handler(logger)
    assert handlers, "StreamHandler should be attached"

def test_logger_log_level_info():
    logger = setup_logger("info_logger")
    assert logger.level == logging.INFO

def test_stream_handler_attached():
    logger = setup_logger("stream_logger")
    handlers = get_stdout_stream_handler(logger)
    assert handlers, "StreamHandler to sys.stdout should be attached"
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)

def test_log_formatter(capsys):
    logger = setup_logger("format_logger")
    test_msg = "Test log message"
    logger.info(test_msg)
    out = capsys.readouterr().out
    # Check for timestamp and log level in output
    assert "INFO" in out
    # Check for timestamp (format: YYYY-MM-DD HH:MM:SS)
    import re
    assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", out), "Timestamp missing in log output"
    assert test_msg in out

def test_logger_same_name_multiple_calls():
    logger1 = setup_logger("repeat_logger")
    logger2 = setup_logger("repeat_logger")
    # Should be the same logger instance
    assert logger1 is logger2
    handlers = get_stream_handler(logger1)
    assert len(handlers) == 1, "Should only have one StreamHandler attached"

def test_empty_logger_name():
    logger = setup_logger("")
    assert isinstance(logger, logging.Logger)
    assert logger.name == ""
    logger.info("Empty name test")  # Should not raise

def test_none_logger_name():
    with pytest.raises(TypeError):
        setup_logger(None)

def test_non_string_logger_name():
    with pytest.raises(TypeError):
        setup_logger(123)

def test_very_long_logger_name():
    long_name = "a" * 1000
    logger = setup_logger(long_name)
    assert logger.name == long_name
    logger.info("Long name test")  # Should not raise

def test_handler_setup_failure(monkeypatch):
    # Simulate StreamHandler raising exception
    original_stream_handler = logging.StreamHandler

    def fail_stream_handler(*args, **kwargs):
        raise Exception("StreamHandler setup failed")

    monkeypatch.setattr(logging, "StreamHandler", fail_stream_handler)
    with pytest.raises(Exception) as excinfo:
        setup_logger("fail_logger")
    assert "StreamHandler setup failed" in str(excinfo.value)