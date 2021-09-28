import os
import re
import sys
import json
import yaml
import atexit
import logging
import requests
import pandas as pd
import pprint as pp

from time import sleep
from pathlib import Path
from datetime import datetime
from scrapy.selector import Selector
from selenium.webdriver.common.keys import Keys

# Config
os.chdir(os.path.dirname(sys.argv[0]))
with open("../config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.FullLoader)

# Import Doglog
sys.path.append('..')
from dogbeach import doglog
from dogbeach.dogdriver import DogDriver

_logger = None
_driver = None

PUBLISHER = 'surfd.com'

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
SLEEP = config[PUBLISHER]['sleep']

# How many times should we attempt to load a page before going to next one?
RETRIES = config[PUBLISHER]['retries']

# How long to wait before giving up on a page load
PAGE_LOAD_TIMEOUT = config[PUBLISHER]['page_load_timeout']

# The category page url template
CAT_URL_TEMPLATE = 'https://surfd.com/category/{}/'

# The list of all category slugs
CATEGORIES = {
  'product-reviews',
  'surfboards',
  'inspiration',
  'surf-photographs',
  'surf-art',
  'surf-books',
  'surf-videos',
  'environment',
  'surf-health',
  'style',
  'travel',
  'improve-your-surfing'
}

# A list of all the articles that have been scraped already, so we don't duplicate our efforts
already_scraped = set()


def get_logger():
    """ Initialize and/or return existing logger object

    :return: a DogLog logger object
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath("__file__"))) / f"../log/{PUBLISHER}_site.log"
        _logger = doglog.setup_logger(f'{PUBLISHER}_site', logfile, clevel=CLEVEL)
    return _logger


def get_driver():
    """ Initialize and/or return existing webdriver object

    :return: a DogDriver object
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger(), sleep=SLEEP, tries=RETRIES, pageload_timeout=PAGE_LOAD_TIMEOUT)
    return _driver


def load_already_scraped_articles():
    """ Query the database for all articles that have already been scraped

    :return: a list of urls of articles that have already been scraped
    """
    global already_scraped

    r = requests.get(PUBLISHER_ARTICLES_ENDPOINT)
    urls_json = r.json()
    already_scraped = set([x['url'] for x in urls_json])

    skip_filename = Path(f'data/{PUBLISHER}/skips.txt')
    directory = os.path.dirname(skip_filename)
    if not os.path.exists(directory):
        os.makedirs(directory)
    skip_filename.touch(exist_ok=True)  # will create file, if it exists will do nothing
    with open(skip_filename, 'r') as skips_file:
      SKIPS = list(map(str.strip, skips_file.readlines()))
      # print(SKIPS)
    
    already_scraped.update(SKIPS)

    get_logger().debug("Found {} articles already scraped".format(len(already_scraped)))


def cleanup_youtube_link(link):
    """ Youtube links extracted from embedded iframes need to be converted to the simple URL
    
    Example: 
        https://www.youtube.com/embed/PUYsRnyYSWY?version=3&rel=1&showsearch=0&showinfo=1&iv_load_policy=1&fs=1&hl=en-AU&autohide=2&wmode=transparent
    """
    if 'embed' in link:
        questionmark_split = link.split('?')
        # Was there a question mark? If so, strip it and everything after it, otherwise this is maybe a clean link already
        if len(questionmark_split) > 1:
            short = questionmark_split[0]
        else:
            return link
        
        return f"https://www.youtube.com/watch?v={short.split('/')[-1]}"
    else:
        return link


def extract_article_video(sel):
    """ There can be a mix of youtube and vimeo videos on a page. Extract them all, and fix up the urls for the youtube vids
    which are super long embed links rather than simple video pages

    Return an empty list if there are no videos found
    """
    article_video_youtube = sel.xpath("*//iframe[@class='youtube-player']/@src").extract()
    if article_video_youtube is None: 
        article_video_youtube = ['']
    else:
        article_video_youtube = list(map(cleanup_youtube_link, article_video_youtube))

    article_video_vimeo = sel.xpath("*//div[@class='embed-vimeo']//iframe/@src").extract()
    if article_video_vimeo is None: 
        article_video_vimeo = ['']

    article_video = article_video_youtube + article_video_vimeo

    return article_video


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
    """ Perform know cleanup for the text content of the article
    """
    # There is a ton of whitespace that can be compressed to a single space
    s = re.sub(r'\s+', ' ', s)
    
    # Remove any remaining html tags encountered
    s = remove_html_markup(s)

    # Remove this social footer text...
    s = s.replace("Share Pin Tweet WhatsApp Email", '')

    # Strip whitespace
    s = s.strip()

    return s


def extract_link_data(link):
    """ For a given url, load the page and extract all available data

    Extract the following fields:
        url, publish date, post category, tItle, subtitle, tags, thumbnail image, text content, article video (if the content contains a video), author name, and author url
    
    :return: A dictionary of attributes extracted from the page
    """
    
    get_driver().get_url(link)
    sleep(4)

    get_logger().debug("getting page source from driver")
    source = get_driver().clean_unicode(get_driver().driver.page_source)
    sleep(1)

    sel = Selector(text=source)
    sleep(0.4)

    publish_date = sel.xpath("*//meta[@property='article:published_time']").xpath('@content').extract_first()
    if ":" == publish_date[-3]:
        publish_date = publish_date[:-3] + publish_date[-2:]
        publish_date = datetime.strptime(publish_date, '%Y-%m-%dT%H:%M:%S%z')
        publish_date  = publish_date.strftime('%Y-%m-%d')

    post_category = sel.xpath("*//div[@class='byline-part cats']//a/text()").extract_first()

    title = sel.xpath("*//div[@class='title-wrap title-with-sub']/h1/text()").extract_first()
    if title is None:
        title = sel.xpath("*//div[@class='title-wrap']/h1/text()").extract_first()
    
    subtitle = sel.xpath("*//div[@class='title-wrap title-with-sub']/p/text()").extract_first()
    subtitle = subtitle if subtitle is not None else ''

    thumbnail_image = sel.xpath("*//div[@class='hero']/img/@src").extract_first()
    if thumbnail_image is None or len(thumbnail_image) == 0:
        thumbnail_image = ''

    text_content = sel.xpath("*//div[contains(@class,'entry-content')]//text()").extract()
    text_content = '\n'.join(text_content)
    text_content = cleanup_text(text_content)

    article_video = extract_article_video(sel)
    
    author_name = sel.xpath("*//span[@class='byline-part author']//a/text()").extract_first()

    author_url = sel.xpath("*//span[@class='byline-part author']//a/@href").extract_first()
    
    article_dict = {
        'publishedAt' : publish_date,
        'url' : link, 
        'category' : post_category, 
        'thumb' : thumbnail_image, 
        'title' : title,
        'subtitle' : subtitle, 
        'text_content' : text_content, 
        'article_video': article_video,
        'author_name' : author_name, 
        'author_url' : author_url,           
    }

    return article_dict


def create_article(article):
    """
    Push this article to the database through the REST API

    :param article: A dictionary of attributes extracted for a single link
    :return: None
    """
    # Add some common fields
    article['userId'] = SYSTEM_USER_ID
    article['browserId'] = SCRAPER_BROWSER_ID
    article['publisher'] = PUBLISHER
    get_logger().info(f"Writing article to RDS...\n{article}")

    header = { "Content-Type": "application/json" }
    json_data = json.dumps(article, default=str)
    r = requests.post(CREATE_ENDPOINT, headers=header, data=json_data)
    
    try:
      r.raise_for_status()
    except Exception as ex:
      exception_type = ex.__class__.__name__
      get_logger().error(f"There was a {exception_type} error while creating article {article['url']}:...\n{r.json()}")


def extract_new_links():
    """ Get all the unique, new links from all categories

    :return: new links to scrape
    """
    all_links = []

    get_logger().debug("Loop through all categories...")
    for category in sorted(list(CATEGORIES)):
        get_logger().debug(f"\nExtracting category: {category}")
        get_logger().debug(f"-------------------")

        get_driver().get_url(CAT_URL_TEMPLATE.format(category))

        # If other screen appear, close
        try:
            get_driver().driver.find_element_by_xpath("*//i[@class='tipi-i-close'])[2]").click()
        except:
            pass

        # Click the "more" button until there are no more links in the category
        while True:
            try:
                button_load_more = get_driver().driver.find_element_by_xpath("*//a[@class='block-loader tipi-button inf-load-more custom-button__fill-1 custom-button__size-1 custom-button__rounded-1']")
                sleep(0.4)
                
                button_load_more.click()
                sleep(3)

                #if other screen appear,close
                try:
                    get_driver().driver.find_element_by_xpath("*//i[@class='tipi-i-close'])[2]").click()
                except:
                    pass
            except:
                get_logger().debug("Button Load More not found")
                break

        # Get all links
        source = get_driver().clean_unicode(get_driver().driver.page_source)
        sel = Selector(text=source)
        cat_links = sel.xpath("*//div[@class='block block-72 tipi-flex']//div[@class='title-wrap']/h3/a/@href").extract()
        if len(cat_links) == 0:
            # Try a different format
            cat_links = sel.xpath("*//article//descendant::div[@class='mask'][1]//descendant::a[1]/@href").extract()

        # Filter out links we've already scraped, and/or duplicates (which we've seen before even in the same category)
        start_link_count = len(cat_links)
        cat_links = list(set([x for x in cat_links if x not in already_scraped]))
        get_logger().info(f"{len(cat_links)} of {start_link_count} links in this category are new")
        get_logger().info("\n".join(sorted(cat_links)))
        if len(cat_links) > 0:
            all_links += cat_links
    
    return sorted(list(set(all_links)))


def scrape():
    """ High level class to manage the iteration of articles being scraped

    This site is very, very small. The approach is to find all links in all categories, de-dupe (links can appear in more than one category), remove
    links that have already been extracted, and then go through all new links - adding in chronological order

    So, there is no "Full" vs. "Update" mode - the site is so small it doesn't warrant it
    """
    new_links = extract_new_links()
    get_logger().info(f"There are {len(new_links)} new links to scrape...")
    for link in new_links:
        get_logger().info(f"\nprocessing link: {link}")
        article_dict = extract_link_data(link)
        create_article(article_dict)


@atexit.register
def cleanup():
    get_driver().driver.quit()


def test_urls(urls):
    """ """
    for url in urls:
        test_article = extract_link_data(url)
        if test_article:
            print(test_article['text_content'])
        else:
            print("there was a problem")


if __name__ == '__main__':
    # urls = [
    #     "https://surfd.com/2021/06/company-profile-globe-international/"
    #     # "https://surfd.com/2021/06/company-profile-channel-islands-surfboards/"
    # ]
    # test_urls(urls)
    # exit()

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    # Query all the urls already scraped for this publisher
    load_already_scraped_articles()

    # Extract and save any new articles
    scrape()

    get_logger().info("\nDone.")
