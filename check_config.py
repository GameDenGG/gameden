from config import validate_settings
from logger_config import setup_logger

logger = setup_logger("config_check")


def main():
    logger.info("Checking configuration...")

    try:
        validate_settings()
        logger.info("Configuration is valid.")
    except Exception as e:
        logger.exception("Configuration validation failed.")
        print(e)


if __name__ == "__main__":
    main()