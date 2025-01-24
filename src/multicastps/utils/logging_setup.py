#return a logger with desidred setup 
import datetime
import os
import logging 
from dotenv import load_dotenv

load_dotenv()

def setup_logging(mod_name, log_level):
    """returns a logger that logs everything to file and log_level to shell"""
    timestamp = datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
    log_file = os.path.join(os.environ.get('LOG_DIR'),
                            f"{mod_name}_{timestamp}.log")

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file,mode='w')

    # Set logging levels
    level = getattr(logging, log_level.upper())
    console_handler.setLevel(level)  # Logs desired level of information to the terminal
    file_handler.setLevel(logging.DEBUG)    # Logs everything to the file

    # Create formatters and add them to the handlers
    console_format = logging.Formatter('%(levelname)s - %(message)s')
    file_format = logging.Formatter('%(levelname)s - %(asctime)s - %(name)s - %(message)s')
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    logger = logging.getLogger(mod_name)
    logger.setLevel(logging.DEBUG)  # Set to the lowest level to capture all messages
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger