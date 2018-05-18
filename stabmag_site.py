import os
import sys
import time
import atexit
import logging

from pathlib import Path
from util import doglog
from util.dogdriver import DogDriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By


NEWS_URL = "https://stabmag.com/news/"

_logger = None
_driver = None


def get_logger():
    """ Initialize and/or return existing logger object

    :return:
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "log/stabmag_site.log"
        _logger = doglog.setup_logger('stabmag_site', logfile, clevel=logging.INFO)
    return _logger


def get_driver():
    """ Initialize and/or return existing webdriver object

    :return:
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger())
    return _driver


def get_articles_csv():
    """ If the file exists, just return the file handle. If it doesn't, create the file and add the header

    :return: the file handle to the the models csv file
    """
    headers = "url,thumb,fb_shares,comments,ts\n"

    filename = "{}/data/stabmag_articles.csv".format(os.path.dirname(os.path.realpath(__file__)))
    if os.path.isfile(filename):
        return open(filename, 'a+')
    f = open(filename, 'w+')
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


def extract_articles(posts):
    """ We're going to simply write all the data we find to file, and in a later process we'll de-dupe. This file only
    contains the url, image,

    :param posts:
    :return: an array of tuples representing the articles on this page (links/images/shares/comments)
    """
    page_posts = []

    # current timestamp
    ts = time.time()

    posts_html = posts.get_attribute('innerHTML')
    # print("posts_html: {}".format(posts_html))

    soup = BeautifulSoup(posts_html, "html.parser")
    # print("soup_text: {}".format(soup.prettify()))

    articles = soup.find_all("div", class_='grid-layout')
    get_logger().info("Extracting {} articles starting with: {}".format(len(articles), articles[0].find('a', class_='feed-hero').get('href')))
    for article in articles:
        # print(article.prettify())
        url = article.find('a', class_='feed-hero').get('href')
        img = article.find('img').get('src')

        fb_span = article.find('span', attrs={"class": "stab-social", "data-network": "facebook"})
        fb_shares = 0 if fb_span is None else fb_span.text

        comments_span = article.find('span', class_='disqus-comment-count')
        comments_count = 0 if comments_span is None else comments_span.text

        page_tup = (url, img, fb_shares, comments_count, ts)
        get_logger().debug(page_tup)
        page_posts += [page_tup]

    write_posts_to_file(page_posts)

    return page_posts


def scrape():
    """ Stab's site doesn't allow direct requests to paging, so we have to simulate usage of the site to get
    all the article URLs

    :return:
    """
    get_logger().info("Starting scrape of 15 pages of latest Stab Mag news...")
    # Load the news page and wait for the posts to load
    get_driver().get(NEWS_URL)
    time.sleep(5)

    # Click the "load more" button so we have all of the first 20 results (only for first page)
    more_button = get_driver().find_element_by_class_name('pagination-load-more')
    more_button.click()
    time.sleep(5)
    get_logger().debug("Got the news page")

    i = 0
    while i < 15:
        posts = get_driver().find_element_by_id('blog-list')
        extract_articles(posts)
        try:
            next_button = get_driver().find_element(By.XPATH, '//a[text()="Next Page"]')
        except:
            get_driver().get_screenshot_as_file("error_{}.png".format(time.time()))
            get_logger().error('Failed to find "Next Page" link', exc_info=True)
            get_logger().info('page source...\n{}'.format(get_driver().page_source))
            exit()

        next_button.click()
        time.sleep(5)
        i += 1
    get_logger().info("Successfully completed scrape of latest Stab Mag news.")


def cleanup():
    get_driver().quit()


if __name__ == "__main__":
    atexit.register(cleanup)

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    scrape()
