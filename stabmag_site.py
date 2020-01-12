import os
import sys
import json
import time
import atexit
import logging

from dogbeach import doglog
from dogbeach.dogdriver import DogDriver

from pathlib import Path
from bs4 import BeautifulSoup 
from selenium.webdriver.common.by import By


SITE = "https://stabmag.com"
NEWS_URL = "https://stabmag.com/news/"

DEFAULT_SLEEP_SECS = 10

_logger = None
_drivers = {}

ALREADY_SCRAPED = set()


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


def get_json_filename():
    """

    :return:
    """
    return "{}/data/stabmag_articles.json".format(os.path.dirname(os.path.realpath(__file__)))


def get_articles_file():
    """ If the file exists, just return the file handle. If it doesn't, create the file and add the header

    :return: the file handle to the the models csv file
    """
    if os.path.isfile(get_json_filename()):
        return open(get_json_filename(), 'a+')

    return open(get_json_filename(), 'w+')


def write_articles_to_file(articles):
    """

    :param articles:
    :return:
    """
    f = get_articles_file()

    for article in articles:
        f.write("{}\n".format(json.dumps(article)))


def load_already_scraped_articles():
    """ We don't want to waste time scraping pages we've already scraped before. So read in all the data and capture
    the existing URLs

    :return: a list of urls of articles that have already been scraped
    """
    global ALREADY_SCRAPED

    if not os.path.exists(get_json_filename()):
        get_logger().warn("The articles file does not yet exist")
        return

    with open(get_json_filename(), 'r') as f:
        print("Found Stabmag articles file: {}".format(f.name))
        for line in f:
            print(line)
            article = json.loads(line)
            ALREADY_SCRAPED.add(article['uri'])

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
    article['text_content'] = content

    author_json = get_author(post_meta_div, content_div)
    if author_json:
        article['author'] = author_json

    return article


def extract_articles(posts):
    """ We're going to simply write all the data we find to file, and in a later process we'll de-dupe. This file only
    contains the url, image,

    :param posts:
    :return: an array of tuples representing the articles on this page (links/images/shares/comments)
    """
    global ALREADY_SCRAPED

    new_article_count = 0
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
            continue
        else:
            new_article_count += 1
        thumb = article_div.find('img').get('src')

        article_json = {'uri': url, 'thumb': thumb}
        article_json = scrape_article(article_json)

        if article_json is not None:
            articles += [article_json]
            print(json.dumps(article_json))
            # get_logger().debug("extracted article: {}".format(json.dumps(article_json)))
        else:
            print("Couldn't scrape {}".format(url))

    # We want to save each page as we crawl so we'll never lose too much...
    write_articles_to_file(articles)

    return new_article_count


def scrape():
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

    for _ in range(20):
        posts = get_driver('site').driver.find_element_by_id('blog-list')
        new_articles_count = extract_articles(posts)
        if new_articles_count == 0:
            get_logger().debug("We've already scraped all the articles found on this page")
        time.sleep(DEFAULT_SLEEP_SECS)

        try:
            next_button = get_driver('site').driver.find_element(By.XPATH, '//a[text()="Next Page"]')
        except:
            get_driver('site').driver.get_screenshot_as_file("error_{}.png".format(time.time()))
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
    get_logger().info("Kicking off Stabmag scraper...")

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    load_already_scraped_articles()

    scrape()
