import logging

from src.config.settings import LOG_FORMAT, LOG_LEVEL


def setup_logger(name: str) -> logging.Logger:
    """Configure and return a logger instance with standardized format.

    Args:
        name: Logger name, typically __name__ from calling module

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, LOG_LEVEL))

    return logger
