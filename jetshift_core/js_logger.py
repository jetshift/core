import logging

logger = logging.getLogger('jetshift_core')
logger.setLevel(logging.WARNING)  # Default level

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_logger(name=None):
    return logger if name is None else logger.getChild(name)


def set_log_level(level):
    """
    Dynamically set the log level for the jetshift_core logger.
    Example: set_log_level(logging.DEBUG)
    """
    logger.setLevel(level)
