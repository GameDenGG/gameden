from logger_config import setup_logger

logger = setup_logger()

logger.info("Logger is working.")
logger.warning("This is a warning.")
logger.error("This is an error.")