import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_file):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a rotating file handler with a maximum file size of 10MB and keeping 5 backup files
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    return logger
