"""
Surfer.com Scraper

This site is able to be scraped through a wordpress endpoint that handles paging, filtering, and sort order.

The scraper can run in two modes:
1. "Full" - start from the oldest article and scrape to the newest
2. "Updates" - start from the newest article and scrape until all articles on the page has already been scraped

Other parameters that can be used to control this scraper:
* "COUNT" - how many articles to retrieve for each "page"
* "SLEEP" - how many seconds to wait in-between requests

"""
import os
import re
import sys
import json
import pytz
import yaml
import atexit
import logging
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from xml.sax.saxutils import escape, unescape

from dogbeach import doglog
from dogbeach.dogdriver import DogDriver

import pprint
pp = pprint.PrettyPrinter(indent=2, width=200)


_logger = None
_engine = None
_driver = None

# What is the identifier for this scraper?
PUBLISHER = 'surfer.com'

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

# Mode: full or new-only
MODE_FULL = config[PUBLISHER]['mode_full']

# Article count (how many to retrieve with each API call)
COUNT = config[PUBLISHER]['articles_per_page']

# Approximately how many articles do we want to check for something missing before giving up?
MAX_SCRAPED_ARTICLES_BEFORE_QUIT = 50

# How many pages of articles that we've entirely scraped should we try before quitting?
MAX_SCRAPED_PAGES_BEFORE_QUIT = int(MAX_SCRAPED_ARTICLES_BEFORE_QUIT / COUNT) + 1

# Which direction to retrieve
SORT = 'asc' if MODE_FULL else 'desc'

# The url template to query a specific page of results
SURFCAT_URL = 'https://www.surfer.com/wp-json/ami/v1/lazy-load' \
  '?paged={}'\
  '&count={}'\
  '&sort={{"date": "{}"}}'

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


def get_driver():
    """ Initialize and/or return existing webdriver object

    :return: a DogDriver object
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger())
        if SLEEP:
            _driver.sleep = SLEEP
        if RETRIES:
            _driver.tries = RETRIES
        if PAGELOAD_TIMEOUT:
            _driver.set_pageload_timeout(PAGELOAD_TIMEOUT)
    
    return _driver


def load_already_scraped_articles():
    """ Query the database for all articles that have already been scraped

    :return: a list of urls of articles that have already been scraped
    """
    global already_scraped

    r = requests.get(PUBLISHER_ARTICLES_ENDPOINT)
    urls_json = r.json()
    already_scraped = set([x['url'] for x in urls_json])

    with open(f'data/{PUBLISHER}/skips.txt', 'r') as skips_file:
      SKIPS = list(map(str.strip, skips_file.readlines()))
      # print(SKIPS)
    
    already_scraped.update(SKIPS)

    get_logger().debug("Found {} articles already scraped".format(len(already_scraped)))
    

def get_page_source(endpoint):
    """ Retireve the page source from the endpoint, and do any necessary cleanup
    """
    get_logger().debug("Retrieving page from endpoint: {}".format(endpoint))
    
    get_driver().get_url(endpoint)
    raw_source = get_driver().driver.page_source
    
    # The html returned is html encoded for '<' and '>' which obviously causes problems
    html_escape_table = {'<': "&lt;", ">": "&gt;"}
    html_unescape_table = {v:k for k, v in html_escape_table.items()}
    raw_source = unescape(raw_source, html_unescape_table)
    
    # Handle know unicode character problems
    source = get_driver().clean_unicode(raw_source)

    return source


def extract_article_image(element):
  """ Each image usually comes with multiple resolutions. Not sure if the available resolutions ever vary, but 
  this has been written to get whichever resolution is closest to 600px width (should be exactly 600px)
  """
  images = element.select('img')
  if len(images) == 0:
    img = ''
  else:
    img_element = images[0]
    # Most images have multiple resolution options, if so, find the one closest to 600px width
    if img_element.has_attr('srcset'):
      img_resolutions_list = list(map(str.split, map(str.strip, img_element['srcset'].split(','))))
      img_resolutions_dict = {int(item[1][:-1]): item[0] for item in img_resolutions_list}
      resolutions = img_resolutions_dict.keys()
      resolution_diffs_dict = {abs(item-600): item for item in resolutions}
      min_resolution_diff_key = min(resolution_diffs_dict.keys())
      img = img_resolutions_dict[resolution_diffs_dict[min_resolution_diff_key]]
    elif img_element.has_attr('src'):
      img = img_element['src']
    else:
      # There is an image without a src? seems unlikely but we'll capture it
      get_logger().warn(f"Article found with src-less image element: {img_element}")
      img = ''
  # print(f"Thumbnail: {img}")

  return img

def extract_article_category(element):
  """
  """
  category_element = element.select('div.article__text a.post-flag')
  if len(category_element) == 0:
    category = ""
  else:
    category = category_element[0].string.strip()
  # print(f"Category: {category}")

  return category


def extract_article_title(element):
  """
  """
  title_element = element.select('h2.article__title a')
  if len(title_element) == 0:
    title = ""
  else:
    title = title_element[0].string.strip()
  # print(f"Title: {title}")

  return title


def extract_article_subtitle(element):
  """
  """
  subtitle_element = element.select('p.article__subtitle')
  if len(subtitle_element) == 0:
    subtitle = ""
  else:
    subtitle_string = subtitle_element[0].string
    if subtitle_string is not None:
      subtitle = " ".join(subtitle_element[0].string.split())
    else:
      subtitle = ""
  # print(f"Subtitle: {title}")
  
  return subtitle

def extract_author_url(element):
  """
  """
  author_url_element = element.select('div.article__meta a')
  if len(author_url_element) == 0:
    url = ''
  else:
    url = author_url_element[0].get('href')
  # print(f"Author URL: {url}")

  return url

def extract_author_name(element):
  """
  """
  author_name_element = element.select('div.article__meta a')
  if len(author_name_element) == 0:
    name = ''
  else:
    name = author_name_element[0].string.strip()
  # print(f"Author Name: {name}")

  return name


def extract_article_list(post_source):
  """ This method will find all article links on the page that haven't already been scraped

  :param category: The category we're currently scraping
  :param post_source: The html for an entire page of results
  :return: A list of dictionaries of article data scraped from the page, in the order they were scraped
  """
  global already_scraped

  articles = []
  
  soup = BeautifulSoup(post_source, "html.parser")
  # print(soup.prettify())

  article_elements = soup.find_all("article")
  if len(article_elements) == 0:
    get_logger().warn("No articles found to extract")
  else:
    get_logger().info("Extracting {} articles starting with: {}".format(len(article_elements), article_elements[0].find('a').get('href')))
  
  # From each article div, extract the partial content (url, thumbnail, category) from the card
  for article_element in article_elements:
    url = article_element.find('a').get('href')[:-1]
    if url in already_scraped:
      continue

    if '30-days-giveaways' in url:
      print(f"All of these are broken for some reason: {url}")
      with open('data/surfer.com/skips.txt', "a") as skips:
        skips.write(f"{url}\n")
      continue

    surfer_dot_com_regex = r"^https?:\/\/(www\.)?surfer.com"
    if not re.search(surfer_dot_com_regex, url, re.M|re.I):
      get_logger().warn("This link is to a different domain, skip it")
      continue
    
    img = extract_article_image(article_element)
    category = extract_article_category(article_element)
    title = extract_article_title(article_element)
    subtitle = extract_article_subtitle(article_element)
    author_url = extract_author_url(article_element)
    author_name = extract_author_name(article_element)

    article_json = {
      "url": url, 
      "category": category, 
      "thumb": img, 
      "title": title, 
      "subtitle": subtitle, 
      "author_url": author_url, 
      "author_name": author_name
    }
    get_logger().debug(f"\nArticle card found: \n{pp.pformat(article_json)}")
    
    articles += [article_json]
  
  return articles


def remove_html_markup(s):
    """ https://stackoverflow.com/questions/753052/strip-html-from-strings-in-python """
    tag = False
    quote = False
    out = ""

    for c in s:
        if c == '<' and not quote:
            tag = True
        elif c == '>' and not quote:
            tag = False
        elif (c == '"' or c == "'") and tag:
            quote = not quote
        elif not tag:
            out = out + c

    return out


def cleanup_text(s):
    """ """
    cleaned = s.replace("- Enlarge image", "").strip()

    return cleaned


def scrape_article(article):
    """ For the provided article url, load the article and find whatever data is available

    :param article: The initial fields of the article in a dictionary
    :return:
    """
    # Load the article and wait for it to load
    print("\n\n=================================================================================\n")
    get_logger().debug(f"Processing URL: {article['url']}")

    if not get_driver().get_url(article['url'], tries=5):
        # We'll just have to skip this url, can't load it even with retries
        get_logger().error("Failed to load URL: {}".format(article['url']))
        return
    
    # There are some URLs that get redirected to non-article pages, avoid them...
    current_url = get_driver().driver.current_url.rstrip('/')
    current_slug = current_url.split('/')[-1]
    article_slug = article['url'].rstrip('/').split('/')[-1]
    if current_slug != article_slug :
      get_logger().error(f"This url redirected to something other than the expected URL ({current_url}), so it's probably a dead page\n")

      # Save this bad url so we don't try to scrape it again
      with open('data/surfer.com/skips.txt', "a") as skips:
        skips.write(f"{article['url']}\n")
      
      return
    
    # Cleanup the article source
    source = get_driver().clean_unicode(get_driver().driver.page_source)
    
    # There are different formats/html structure so figure out which we're dealing with
    article_soup = BeautifulSoup(source, "html.parser")
    try:
      content = article_soup.select('article.post-content')[0]
    except:
      get_logger().error(f"Broken content found at {article['url']}, adding to the skip list...")
      get_logger().info(f"Broken content:\n{article_soup}")
      
      with open('data/surfer.com/skips.txt', "a") as skips:
        skips.write(f"{article['url']}\n")
      
      return

    # Publication Date
    try:
      post_date = article_soup.select('span.post-byline__date')[0].string
      post_date = datetime.strptime(post_date, '%B %d, %Y')
      if post_date.year < 1970:
          post_date = post_date.replace(year=1970)
      article['publishedAt'] = post_date.strftime('%Y-%m-%d')
    except IndexError:
      post_date_content = article_soup.find(property="article:published_time").get("content")
      if len(post_date_content) > 0:
        if ":" == post_date_content[-3]:
          post_date_content = post_date_content[:-3] + post_date_content[-2:]
        post_date = datetime.strptime(post_date_content, '%Y-%m-%dT%H:%M:%S%z')
        article['publishedAt'] = post_date.strftime('%Y-%m-%d')
      else:
        get_logger().error(f"Could not find a published date on url: {article['url']}")
        return
    # print(f"Post date: {post_date.strftime('%Y-%m-%d')}")

    # Article Content
    content = article_soup.select('article.post-content div.post-body')[0].text.strip()
    content = remove_html_markup(content) # This shouldn't be necessary, but there are some broken articles with html tags
    content = cleanup_text(content)
    content = " ".join(content.split())
    article['text_content'] = content
    
    # Tags
    tags = []
    tags_element = article_soup.select('article.post-content div[data-article-tags]')
    if len(tags_element) > 0:
      tags = tags_element[0]['data-article-tags']
    else:
      tags = ''
    # print(f"Tags: {tags}")
    article['tags'] = tags
    
    # print(pp.pformat(article))
    return article


def create_article(article):
    """
    Push this article to the database through the REST API

    :param articles:
    :return:
    """
    global already_scraped
    
    # Add some common fields
    article['userId'] = SYSTEM_USER_ID
    article['browserId'] = SCRAPER_BROWSER_ID
    article['publisher'] = PUBLISHER
    get_logger().debug("Writing article to RDS...\n{}".format(article))

    header = { "Content-Type": "application/json" }
    json_data = json.dumps(article, default=str)
    r = requests.post(CREATE_ENDPOINT, headers=header, data=json_data)
    
    try:
      r.raise_for_status()
    except Exception as ex:
      exception_type = ex.__class__.__name__
      get_logger().error(f"There was a {exception_type} error while creating article {article['url']}:...\n{r.json()}")


def extract_articles(articles, create=True):
  """
  Given a list of article objects (contains: URL, category, thumbnail, etc), extract the remainder of the data 
  for each and submit the data to the API endpoint

  :param category: The category we're currently scraping
  :param post_source: The html for an entire page of results
  :return: True if we encountered *any* urls that we've already scraped, False if not
  """
  # Load each article and extract the rest of the data
  for article in articles:
    article = scrape_article(article)
    
    # If there was no scraping error, then send the article data to the REST API...
    if article and create:
      create_article(article)


def scrape():
    """ 

    :return:
    """
    get_logger().debug("Starting scrape...")

    pagenum, empty_pages = 1, 0
    
    while 1 == 1:
        # Extract and clean the html source for the current page
        page_endpoint = SURFCAT_URL.format(pagenum, COUNT, SORT)
        source = get_page_source(page_endpoint)
        
        # build a list of all articles on this page that haven't been scraped yet
        page_articles = extract_article_list(source)

        # If there are any new articles on this page, extract all their contents and push to the database
        article_urls_string = "\n".join([x['url'] for x in page_articles])
        print(f"Found {len(page_articles)} articles to scrape on page {pagenum}:\n{ article_urls_string }\n")
        if len(page_articles) > 0:
            # Scrape each of these pages and load them into the database
            extract_articles(page_articles)

            # Reset the empty page counter
            empty_pages = 0
        else:
            # Incrememnt the empty page counter
            empty_pages += 1

            # If we have gone past the maximum number of pages without a new article, then quit
            if empty_pages == MAX_SCRAPED_PAGES_BEFORE_QUIT:
                get_logger().info("All articles on page {} have already been scraped, exiting...".format(int(pagenum)))
                break
        
        # Increment the page counter
        pagenum += 1

    get_logger().info("Successfully completed scrape of latest Surfer.com news.")


@atexit.register
def cleanup():
    get_driver().driver.quit()


def test_urls(urls):
    """ This function will bypass the scheduled scraping logic and jump straight into extracting a particular URL """
    articles = []
    for url in urls:
        articles += [{'url': url}]

    extract_articles(articles, False)


if __name__ == "__main__":
    # test_articles = [
    #     "https://www.surfer.com/features/gravity",
    #     "https://www.surfer.com/magazine/drew-brees-is-on-a-mission-to-give-back-to-those-in-need",
    #     "https://www.surfer.com/blogs/industry-news/2010-katin-pro-am-team-challenge-draws-near",
    #     "https://www.surfer.com/surfing-magazine-archive/surfing-video/mick-fanning-2009-wct-champion-video"
    # ]
    # test_urls(test_articles)
    # exit()

    kickoff_time = datetime.now(WESTCOAST).strftime('%Y-%m-%d %H:%M:%S')
    get_logger().info("Kicking off Surfer.com scraper at {}...".format(kickoff_time))

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    # Query all the urls already scraped for this publisher
    load_already_scraped_articles()

    scrape()
