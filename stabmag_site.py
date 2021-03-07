import os
import sys
import json
import pytz
import time
import yaml
import atexit
import logging
import requests
import numpy as np
import pandas as pd

from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver.common.by import By

from dogbeach import doglog
from dogbeach.dogdriver import DogDriver

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 50)
pd.set_option('display.width', 500)

_logger = None
_engine = None
_drivers = {}

PUBLISHER = 'stabmag'

##################################### Config
os.chdir(os.path.dirname(sys.argv[0]))
with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.FullLoader)

# Log level
levels = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARN': logging.WARN,
    'ERROR': logging.ERROR
}
clevel_key = config[PUBLISHER]['log_clevel'] if 'log_clevel' in config[PUBLISHER] else 'WARN'
CLEVEL = levels[clevel_key] if clevel_key in levels else levels['WARN']

# What is the API endpoint
REST_API_PROTOCOL = config['common']['rest_api']['protocol']
REST_API_HOST = config['common']['rest_api']['host']
REST_API_PORT = config['common']['rest_api']['port']
REST_API_URL = f"{REST_API_PROTOCOL}://{REST_API_HOST}:{REST_API_PORT}"
CREATE_ENDPOINT = f"{REST_API_URL}/article"
PUBLISHER_ARTICLES_ENDPOINT = f"{REST_API_URL}/articleUrlsByPublisher?publisher={PUBLISHER}"

# UserID and BrowswerID are required fields for creating articles, this User is the ID tied to the system account
SYSTEM_USER_ID = config['common']['system_user_id']

# This is the "blank" UUID
SCRAPER_BROWSER_ID = config['common']['browser_id']

# How long in between requests, in seconds
SLEEP = config[PUBLISHER]['sleep'] if 'sleep' in config[PUBLISHER] else None

# How many times should we attempt to load a page before going to next one?
RETRIES = config[PUBLISHER]['retries'] if 'retries' in config[PUBLISHER] else None

# How long to wait before giving up on a page load
PAGELOAD_TIMEOUT = config[PUBLISHER]['page_load_timeout'] if 'page_load_timeout' in config[PUBLISHER] else None

# How many pages of articles that we've already scraped fully should we try before quitting?
MAX_SCRAPED_PAGES_BEFORE_QUIT = config[PUBLISHER]['max_empty_pages']

SITE = "https://stabmag.com"

NEWS_URL = "https://stabmag.com/news/"

# We want all times to be in westcoast time
WESTCOAST = pytz.timezone('US/Pacific')

# Track the list of article urls that have already been scraped
already_scraped = set()

def get_logger():
    """ Initialize and/or return existing logger object

    :return: a DogLog logger object
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath("__file__"))) / f"log/{PUBLISHER}_site.log"
        _logger = doglog.setup_logger(f'{PUBLISHER}_site', logfile, clevel=CLEVEL)
    return _logger


def get_driver(name='default'):
    """ Initialize and/or return existing webdriver object

    :return: a DogDriver object
    """
    global _drivers
    if name not in _drivers:
        _drivers[name] = DogDriver(get_logger())
        if SLEEP:
            _drivers[name].sleep = SLEEP
        if RETRIES:
            _drivers[name].tries = RETRIES
        if PAGELOAD_TIMEOUT:
            _drivers[name].set_pageload_timeout(PAGELOAD_TIMEOUT)
    
    return _drivers[name]


def load_already_scraped_articles():
    """ Query the database for all articles that have already been scraped

    :return: a list of urls of articles that have already been scraped
    """
    global already_scraped

    r = requests.get(PUBLISHER_ARTICLES_ENDPOINT)
    urls_json = r.json()
    already_scraped = set([x['url'].rstrip('/').split("/")[-1] for x in urls_json])
    get_logger().debug("Found {} articles already scraped".format(len(already_scraped)))

    return


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
            author_json['url'] = author_url
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
    url = article['url']

    if not get_driver('article').get_url(url):
        # We'll just have to skip this slug, can't load it even with retries
        get_logger().warning(f"failed to get url: {url}")
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
    article['publishedAt'] = post_date

    # Get the content
    content_div = article_soup.find("div", {"class": "content editable"})
    content = content_div.get_text().strip()
    content = content.replace(u'\xa0', u' ')
    article['text_content'] = content

    author_json = get_author(post_meta_div, content_div)
    if author_json:
        if 'url' in author_json:
            article['author_url'] = author_json['url']
        if 'name' in author_json:
            article['author_name'] = author_json['name']

    return article


def extract_articles(posts):
    """ We're going to simply write all the data we find to file, and in a later process we'll de-dupe. This file only
    contains the url, image,

    :param posts:
    :return: an array of tuples representing the articles on this page (links/images/shares/comments)
    """
    global already_scraped

    articles = []

    posts_html = posts.get_attribute('innerHTML')
    # print("posts_html: {}".format(posts_html))

    soup = BeautifulSoup(posts_html, "html.parser")
    # print("soup_text: {}".format(soup.prettify()))

    article_divs = soup.find_all("div", class_='grid-layout')
    first_url = article_divs[0].find('a', class_='feed-hero').get('href').rstrip('/')
    get_logger().info("Extracting {} articles starting with: {}".format(len(article_divs), first_url))
    for article_div in article_divs:
        # print(article_div.prettify())
        url = SITE + article_div.find('a', class_='feed-hero').get('href').rstrip('/')
        if url.split('/')[-1] in already_scraped:
            get_logger().info("already scraped {}, skipping...".format(url))
            continue
        else:
            get_logger().info("new article found: {}".format(url))
            # Just in case there are duplicates
            already_scraped.add(url.split('/')[-1])

        article_json = {
            'url': url,
            'thumb': article_div.find('img').get('src')
        }
        article_json = scrape_article(article_json)

        if article_json is not None:
            articles += [article_json]
            # get_logger().debug("extracted article: {}".format(json.dumps(article_json)))
        else:
            get_logger().warn("Couldn't scrape {}".format(url))

    return articles


def create_articles(articles):
    """ Create articles using the REST API

    :param articles:
    :return:
    """
    for article in articles:
      get_logger().info(f"creating article: {article['url']}")

      # Add some common fields
      article['userId'] = SYSTEM_USER_ID
      article['browserId'] = SCRAPER_BROWSER_ID
      article['publisher'] = 'stabmag'
      get_logger().debug("Writing article to RDS...\n{}".format(article))

      header = { "Content-Type": "application/json" }
      r = requests.post(CREATE_ENDPOINT, headers=header, data=json.dumps(article, default=str))
      try:
        r.raise_for_status()
      except Exception as ex:
        get_logger().error(f"There was a {type(ex)} error while creating article {article['url']}:...\n{r}")


def scrape_pages():
    """ Stab's site doesn't allow direct requests to paging, so we have to simulate usage of the site to get
    all the article URLs

    :return:
    """
    get_logger().info("Starting scrape of latest Stab Mag news...")

    # Load the news page and wait for the posts to load
    get_driver('site').get_url(NEWS_URL)
    time.sleep(SLEEP)

    # Click the "load more" button so we have all of the first 20 results (only for first page)
    more_button = get_driver('site').driver.find_element_by_class_name('pagination-load-more')
    more_button.click()
    time.sleep(SLEEP)
    get_logger().debug("Got the news page")

    # Scrape the first MAX_SCRAPED_PAGES_BEFORE_QUIT pages, even if there isn't a single new article on a page
    articles = []
    for _ in range(MAX_SCRAPED_PAGES_BEFORE_QUIT):
        posts = get_driver('site').driver.find_element_by_id('blog-list')

        post_articles = extract_articles(posts)
        if len(post_articles) == 0:
            get_logger().debug("We've already scraped all the articles found on this page")
        else:
            articles += post_articles
            
        time.sleep(SLEEP)
        try:
            next_button = get_driver('site').driver.find_element(By.XPATH, '//a[text()="Next Page"]')
        except:
            get_driver('site').driver.get_screenshot_as_file("log/error_images/stabmag/error_{}.png".format(time.time()))
            get_logger().error('Failed to find "Next Page" link', exc_info=True)
            get_logger().info('page source...\n{}'.format(get_driver('site').driver.page_source))
            exit()

        next_button.click()
    
    # Now, write all the articles we found to RDS
    ordered = list(reversed(articles))
    create_articles(ordered)

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
