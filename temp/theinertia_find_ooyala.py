import os
import sys
import atexit
import logging
import traceback
import pandas as pd
from pathlib import Path

from dogbeach import doglog
from dogbeach.dogdriver import DogDriver

ARTICLE_TEMPLATE = 'https://www.theinertia.com/{}'

_logger = None
_driver = None


def get_logger():
    """

    :return:
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "../log/theinertia_ooyala.log"
        _logger = doglog.setup_logger("theinertia_ooyala", logfile, clevel=logging.DEBUG)
    return _logger


def get_driver():
    """

    :return:
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger(), sleep=1)
    return _driver


def check_ooyala(slug):
    """ For the provided slug, load the article and find whatever data is available

    :param driver:
    :param url:
    :return:
    """
    # Load the article and wait for it to load
    url = ARTICLE_TEMPLATE.format(slug)
    get_logger().debug("Processing URL: {}".format(url))

    if not get_driver().get_url(url, sleep=0):
        # We'll just have to skip this slug, can't load it even with retries
        return

    if "oo-player-container" in get_driver().driver.page_source:
        print("**********Found Ooyala content page: {}".format(url))
        exit()


def check_articles():
    """ Load each article url and extract all useful info from it, saving each article separately to a json file

    :return:
    """
    # Read in the list of all articles from CSV
    articles_list_df = pd.read_csv('data/theinertia_articles.csv')
    articles_list_df = articles_list_df[pd.notnull(articles_list_df.url)]

    start_url = 'surf/do-you-want-north-shore-2'
    idx = articles_list_df[articles_list_df.url == start_url].index[0]
    print("Index of the most recently found Ooyala link: {}".format(idx))
    articles_list_df = articles_list_df[articles_list_df.index > idx]

    articles_list = articles_list_df.url.astype(str).unique()

    oo_articles = ['surf/imagine-what-kellys-wave-would-look-like-inside-this-12000-gallon-indoor-ocean',
                   'surf/check-out-this-inflatable-tent-that-can-actually-withstand-high-winds-without-poles']

    get_logger().debug("{} articles have been found: \n{}".format(len(articles_list), articles_list))

    for url in articles_list:
        get_logger().info("Checking url: {}".format(url))

        try:
            check_ooyala(url)
        except Exception as e:
            get_logger().error(getattr(e, 'message', repr(e)))
            get_logger().error(traceback.format_exc())
            exit()


def cleanup():
    get_driver().driver.quit()


if __name__ == "__main__":
    atexit.register(cleanup)

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    check_articles()
