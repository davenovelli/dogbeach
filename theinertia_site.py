import os
import sys
import time
import atexit
import logging
import pandas as pd

from pathlib import Path
from util import doglog
from util.dogdriver import DogDriver
from bs4 import BeautifulSoup


_logger = None
_driver = None
_filename = "{}/data/theinertia_articles.csv".format(os.path.dirname(os.path.realpath(__file__)))
_already_scraped = []


def get_logger():
    """ Initialize and/or return existing logger object

    :return:
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "log/theinertia_site.log"
        _logger = doglog.setup_logger('theinertia_site', logfile, clevel=logging.INFO)
    return _logger


def get_driver():
    """ Initialize and/or return existing webdriver object

    :return:
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger())
    return _driver


def cleanup():
    get_driver().driver.quit()


def get_articles_csv():
    """ If the file exists, just return the file handle. If it doesn't, create the file and add the header

    :return: the file handle to the the models csv file
    """
    global _filename

    headers = "url,thumb,ts\n"

    if os.path.isfile(_filename):
        return open(_filename, 'a+')
    f = open(_filename, 'w+')
    f.write(headers)

    return f


def write_posts_to_file(posts):
    """

    :param posts:
    :return:
    """
    f = get_articles_csv()

    for post in posts:
        line = ",".join([str(t) for t in list(post)])
        f.write("{}\n".format(line))


def extract_articles(post_source):
    """

    :param posts:
    :return: True if we encountered *any* urls that we've already scraped, False if not
    """
    global _already_scraped

    page_posts = []

    # current timestamp
    ts = time.time()

    soup = BeautifulSoup(post_source, "html.parser")
    articles = soup.find_all("div", class_="inertia-item")
    if len(articles) == 0:
        # Perhaps we're dealing with old html, the class switched in Nov 2018
        articles = soup.find_all("div", class_="item")
        if len(articles) == 0:
            get_logger().warn("No articles found to extract")
            return

    get_logger().info("Extracting {} articles starting with: {}".format(len(articles), articles[0].find('a').get('href')))
    for article in articles:
        # print(article.prettify())
        url = article.find('a').get('href').replace('https://www.theinertia.com/', '')[:-1]
        if url in _already_scraped:
            continue
        img = article.find('img').get('src')

        page_tup = (url, img, ts)
        get_logger().debug(page_tup)
        page_posts += [page_tup]

    if len(page_posts) > 0:
        write_posts_to_file(page_posts)
        return True
    return False


def load_scraped():
    """ Read in all the urls we've already scraped so we don't duplicate anything
    """
    global _filename, _already_scraped

    if not os.path.isfile(_filename):
        return []

    df = pd.read_csv(_filename)
    if len(df) == 0:
        return []

    _already_scraped = list(df.url.unique())


def scrape():
    """ This scraper uses an endpoint that controls the paging, but it doesn't exactly match the surf main page. It's
    close enough that I feel pretty good about it

    Categories: Films (broken), Surf, Mountain (skip), Enviro, Health, Photo, Arts, Travel,Women

    :return:
    """
    load_scraped()

    # We'll start with the Surf category:
    SURFCAT_URL = 'https://www.theinertia.com/wp-content/themes/theinertia-2014/quick-ajax.php' \
                  + '?action=recent_posts&category={}&curated_list=false&paged=1&num={}'
    CATEGORIES = {
        'art': 10,
        'surf': 20,
        'health': 21,
        'enviro': 22,
        'travel': 23,
        'photos': 494,
        'women': 32700
    }

    for cat in CATEGORIES.values():
        pagenum = 0
        while 1 == 1:
            get_driver().get_url(SURFCAT_URL.format(cat, pagenum))
            pagenum += 12

            source = get_driver().driver.page_source.replace('\u201c', '"').replace('\u201d', '"').replace('\u2019', "'")
            if not extract_articles(source):
                print("All articles on page {} have already been scraped, exiting...".format(int(pagenum/12)))
                break


if __name__ == "__main__":
    atexit.register(cleanup)

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    scrape()
