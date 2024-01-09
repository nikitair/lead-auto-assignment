import logging


# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Terminal output
th = logging.StreamHandler()
th.setLevel(logging.INFO)
th.setFormatter(formatter)
logger.addHandler(th)

# File output
fh = logging.FileHandler('logs.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)