import os
import logging


def setup_logger(name, log_file, flevel=logging.WARNING, clevel=logging.WARNING):
    """ Setup a logger for a specific scraper

    :param name: Name of the logger
    :param log_file: The log file to output to
    :param flevel: A specific logging level to use (defaults to WARNING)
    :param clevel: A specific logging level to use (defaults to WARNING)
    :return: the configured logger
    """
    # If the directory of the given filepath doesn't exist, go ahead and create it...
    if not os.path.exists(log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create the formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # create file handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(flevel)
    fh.setFormatter(formatter)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(clevel)

    logger = logging.getLogger(name)
    logging.getLogger().setLevel(logging.DEBUG)
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

def clean_unicode(source):
    """Clean unhelpful unicode characters out of scraped page content before saving

    :param content: Page source from a scraped url
    :return: cleaned up source
    """
    return source \
        .replace('\u201c', '"') \
        .replace('\u201d', '"') \
        .replace('\u2018', "'") \
        .replace('\u2019', "'") \
        .replace('\u00a0', " ") \
        .replace('\u2013', '-') \
        .replace('\u2014', '-')