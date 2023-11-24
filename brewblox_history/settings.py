import logging
from functools import lru_cache

from .models import ServiceConfig


@lru_cache
def get_config() -> ServiceConfig:
    cfg = ServiceConfig()
    return cfg


class DuplicateFilter(logging.Filter):
    """
    Logging filter to prevent long-running errors from flooding the log.
    When set, repeated log messages are blocked.
    This will not block alternating messages, and is module-specific.
    """

    def filter(self, record):
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, 'last_log', None):
            self.last_log = current_log
            return True
        return False


def brewblox_logger(name: str, dedupe=False):
    """
    Convenience function for creating a module-specific logger.
    """
    logger = logging.getLogger('...'+name[-27:] if len(name) > 30 else name)
    if dedupe:
        logger.addFilter(DuplicateFilter())
    return logger


def init_logging():
    config = get_config()
    level = logging.DEBUG if config.debug else logging.INFO
    format = '%(asctime)s %(levelname)-8s %(name)-30s  %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'

    logging.basicConfig(level=level, format=format, datefmt=datefmt)
    logging.captureWarnings(True)
