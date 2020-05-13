import os
import sys
import json
import pytz
import time
import atexit
import logging
import numpy as np
import pandas as pd

from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine

from dogbeach import doglog
from dogbeach.dogdriver import DogDriver


pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 50)
pd.set_option('display.width', 500)


_logger = None
_engine = None
_drivers = {}


SITE = "https://stabmag.com"
NEWS_URL = "https://stabmag.com/news/"

DEFAULT_SLEEP_SECS = 10

ALREADY_SCRAPED = set()
PAGES_TO_SCRAPE = 1

# We want all times to be in westcoast time
WESTCOAST = pytz.timezone('US/Pacific')


def get_logger():
    """ Initialize and/or return existing logger object

    :return:
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "log/stabmag_site.log"
        _logger = doglog.setup_logger('stabmag_site', logfile, clevel=logging.INFO)
    return _logger


def get_driver(name='default'):
    """ Initialize and/or return existing webdriver object

    :return:
    """
    global _drivers
    if name not in _drivers:
        _drivers[name] = DogDriver(get_logger())
    return _drivers[name]


def get_rds_engine():
    """ Get the sqlalchemy engine object to read from RDS

    :return: SqlAlchemy engine object
    """
    try:
        user = os.environ['YEWREVIEW_RDS_USER']
        pw = os.environ['YEWREVIEW_RDS_PASS']
        host = os.environ['YEWREVIEW_RDS_HOST']
        port = os.environ['YEWREVIEW_RDS_PORT']
    except:
        get_logger().debug("Required database connection environment variable missing")
        raise

    global _engine
    if _engine is None:
        _engine = create_engine('mysql+pymysql://{}:{}@{}:{}/yewreview'.format(user, pw, host, port), encoding='utf8')

    return _engine


def write_articles_to_rds(articles):
    """

    :param articles:
    :return:
    """
    cols = ['uri', 'publisher', 'publish_date', 'scrape_date', 'category', 'title', 'subtitle', 'thumb', 'content',
            'article_type', 'article_photo', 'article_caption', 'article_video', 'article_insta',
            'author_name', 'author_type', 'author_url',
            'fblikes', 'twlikes']

    articles_df = pd.DataFrame(articles)

    missing_cols = set(cols) - set(articles_df.columns.values)
    for col in missing_cols:
        articles_df[col] = np.NaN
    articles_df['publisher'] = 'stabmag'
    articles_df['scrape_date'] = datetime.now(WESTCOAST)
    articles_df = articles_df[cols]

    print("Writing {} articles to RDS...\n".format(articles_df.shape[0]))
    print(articles_df)

    articles_df.to_sql(name='articles', con=get_rds_engine(), if_exists='append', index=False)


def load_already_scraped_articles():
    """ Query the database for all articles that have already been scraped

    :return: a list of urls of articles that have already been scraped
    """
    global ALREADY_SCRAPED

    query = """
     SELECT uri
       FROM articles
      WHERE publisher = 'stabmag'
    """
    articles_df = pd.read_sql(query, get_rds_engine())
    ALREADY_SCRAPED = set(articles_df.uri)

    get_logger().debug("Found {} articles already scraped".format(len(ALREADY_SCRAPED)))


def get_author(post_meta, content_div):
    """ Attempt to extract a Person object from this article and return it if found, otherwise return None

    :param post_meta:
    :param content_div:
    :return:
    """
    author_json = {}

    if len(post_meta.find_all("div")) > 1:
        post_author_divs = post_meta.find_all("div")
        post_author_div = post_author_divs[1]
        post_author_spans = post_author_div.find_all("span")
        post_author_span = post_author_spans[1]

        # Sometimes the author is "STAB" or has no link
        if len(post_author_span.find_all("a")) > 0:
            author_link = post_author_span.find("a")
            author_url = 'https://stabmag.com/' + author_link.get('href')
            author_name = author_link.get_text().title()
            author_json['uri'] = author_url
        elif len(content_div.find_all(text="Story by")) > 0:
            author_name = content_div.find_all(text="Story by")[0].get_text().lower().replace('story by', '').title()
        elif 'class' in post_author_span.attrs and 'post-meta-last' in post_author_span.get("class"):
            author_name = post_author_span.get_text().lower().replace('words by', '').title()
        else:
            return None

        # If we made it here, there should be an author name
        if author_name is not None:
            author_json['name'] = author_name
    # else:
    #     print("No Author found")

    return author_json


def scrape_article(article):
    """ Using a second driver instance, load the specific URL and scrape the remainder of the data

    :param article: A dictionary containing the url and thumbnail image, to be populated with the rest of the properties
    :return: the the populated dictionary, to be written to file as json - or None if we can't load the page
    """
    # Load the article and wait for it to load
    url = article['uri']

    if not get_driver('article').get_url(url):
        # We'll just have to skip this slug, can't load it even with retries
        print("failed to get url: {}".format(url))
        return None

    source = get_driver('article').clean_unicode(get_driver('article').driver.page_source)
    soup = BeautifulSoup(source, "html.parser")
    article_soup = soup.find("article", class_="container")
    if article_soup is None:
        get_logger().warning("Can't find the article. Skipping. {}".format(url))
        return None

    # Get the title and subtitle
    title_h1 = article_soup.find("h1")
    if title_h1 is None:
        get_logger().warning("Can't find a title. Skipping. {}".format(url))
        return None
    title = article_soup.find("h1").string.strip()
    article['title'] = title
    subtitle_div = article_soup.find("div", class_="featured-summary")
    subtitle = '' if subtitle_div is None else subtitle_div.get_text().strip()
    article['subtitle'] = subtitle

    # The post date should always be available
    post_meta_div = article_soup.find("div", class_="blog-post-meta")
    post_date_div = post_meta_div.find_all("div")[0]
    post_date = post_date_div.find_all("span")[1].find("a").get('href').replace("/news/archive/", "").replace("/", "-")
    article['publish_date'] = post_date

    # Get the content
    content_div = article_soup.find("div", {"class": "content editable"})
    content = content_div.get_text().strip()
    content = content.replace(u'\xa0', u' ')
    article['content'] = content

    author_json = get_author(post_meta_div, content_div)
    if author_json:
        if 'uri' in author_json:
            article['author_url'] = author_json['uri']
        else:
            article['author_url'] = ''
        if 'name' in author_json:
            article['author_name'] = author_json['name']
        else:
            article['author_name'] = ''

    return article


def extract_articles(posts):
    """ We're going to simply write all the data we find to file, and in a later process we'll de-dupe. This file only
    contains the url, image,

    :param posts:
    :return: an array of tuples representing the articles on this page (links/images/shares/comments)
    """
    global ALREADY_SCRAPED

    articles = []

    posts_html = posts.get_attribute('innerHTML')
    # print("posts_html: {}".format(posts_html))

    soup = BeautifulSoup(posts_html, "html.parser")
    # print("soup_text: {}".format(soup.prettify()))

    article_divs = soup.find_all("div", class_='grid-layout')
    first_url = article_divs[0].find('a', class_='feed-hero').get('href')
    get_logger().info("Extracting {} articles starting with: {}".format(len(article_divs), first_url))
    for article_div in article_divs:
        # print(article_div.prettify())
        url = SITE + article_div.find('a', class_='feed-hero').get('href')
        if url in ALREADY_SCRAPED:
            print("already scraped {}, skipping...".format(url))
            continue

        article_json = {
            'uri': url,
            'thumb': article_div.find('img').get('src')
        }
        article_json = scrape_article(article_json)

        if article_json is not None:
            articles += [article_json]
            print(json.dumps(article_json))
            # get_logger().debug("extracted article: {}".format(json.dumps(article_json)))
        else:
            print("Couldn't scrape {}".format(url))

    return articles


def scrape_pages():
    """ Stab's site doesn't allow direct requests to paging, so we have to simulate usage of the site to get
    all the article URLs

    :return:
    """
    get_logger().info("Starting scrape of latest Stab Mag news...")

    # Load the news page and wait for the posts to load
    get_driver('site').get_url(NEWS_URL)
    time.sleep(DEFAULT_SLEEP_SECS)

    # Click the "load more" button so we have all of the first 20 results (only for first page)
    more_button = get_driver('site').driver.find_element_by_class_name('pagination-load-more')
    more_button.click()
    time.sleep(DEFAULT_SLEEP_SECS)
    get_logger().debug("Got the news page")

    # Scrape the first PAGES_TO_SCRAPE pages, even if there isn't a single new article on a page
    for _ in range(PAGES_TO_SCRAPE):
        posts = get_driver('site').driver.find_element_by_id('blog-list')

        articles_json = extract_articles(posts)
        if len(articles_json) == 0:
            get_logger().debug("We've already scraped all the articles found on this page")
        else:
            write_articles_to_rds(articles_json)

        time.sleep(DEFAULT_SLEEP_SECS)
        try:
            next_button = get_driver('site').driver.find_element(By.XPATH, '//a[text()="Next Page"]')
        except:
            get_driver('site').driver.get_screenshot_as_file("log/error_images/stabmag/error_{}.png".format(time.time()))
            get_logger().error('Failed to find "Next Page" link', exc_info=True)
            get_logger().info('page source...\n{}'.format(get_driver('site').driver.page_source))
            exit()

        next_button.click()

    get_logger().info("Successfully completed scrape of latest Stab Mag news.")


@atexit.register
def cleanup():
    get_driver('site').driver.quit()
    get_driver('article').driver.quit()


if __name__ == "__main__":
    kickoff_time = datetime.now(WESTCOAST).strftime('%Y-%m-%d %H:%M:%S')
    get_logger().info("Kicking off the Stabmag scraper at {}...".format(kickoff_time))

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    load_already_scraped_articles()

    scrape_pages()
