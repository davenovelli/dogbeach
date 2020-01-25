import os
import sys
import pytz
import atexit
import urllib
import logging
import numpy as np
import pandas as pd

from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine

from dogbeach import doglog
from dogbeach.dogdriver import DogDriver


pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 75)
pd.set_option('display.width', 500)


_logger = None
_engine = None
_driver = None

# We want all times to be in westcoast time
WESTCOAST = pytz.timezone('US/Pacific')

# Track the list of article urls that have already been scraped
ALREADY_SCRAPED = set()

# The url for a specific page of a specific category
SURFCAT_URL = 'https://www.theinertia.com/wp-content/themes/theinertia-2014/quick-ajax.php' \
              + '?action=recent_posts&category={}&curated_list=false&paged=1&num={}'

# The list of categories and codes we're interested in scraping
CATEGORIES = {
    'art': 10,
    'surf': 20,
    'health': 21,
    'enviro': 22,
    'travel': 23,
    'photos': 494,
    'women': 32700
}


def get_logger():
    """ Initialize and/or return existing logger object

    :return: a DogLog logger object
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "log/theinertia_site.log"
        _logger = doglog.setup_logger('theinertia_site', logfile, clevel=logging.DEBUG)
    return _logger


def get_driver():
    """ Initialize and/or return existing webdriver object

    :return: a DogDriver object
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger())
    return _driver


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
    articles_df.rename(columns={
        'type': 'article_type',
        'photo': 'article_photo',
        'caption': 'article_caption',
        'video': 'article_video',
        'insta': 'article_insta',
        'text_content': 'content'
    }, inplace=True)

    missing_cols = set(cols) - set(articles_df.columns.values)
    for col in missing_cols:
        articles_df[col] = np.NaN
    articles_df['publisher'] = 'theinertia'
    articles_df['scrape_date'] = datetime.now(WESTCOAST)
    articles_df = articles_df[cols]

    get_logger().debug("\nWriting {} articles to RDS...".format(articles_df.shape[0]))

    articles_df.to_sql(name='articles', con=get_rds_engine(), if_exists='append', index=False)


def get_article_content(soup, article):
    """ Extract the information from the html element containing the article, and add it to the article's json

    This is a bit tricky, because the format and location of the images and/or videos is different depending on the
    content type. There's no clear indication of what's what, so we'll just have to do lots of tests...

    :param soup: The html element containing the specific article info
    :param article: The json/dictionary of data already extracted from this article
    :return:
    """
    article_element = soup.find("article", {"itemprop": "articleBody"})

    # Remove sections of the page that don't have article-specific data
    decompose_divs = ['page-numbers', 'social-share', 'trc_related_container', 'inertia-comments-container',
                      'inertia-endless-articles']
    for decompose_div in decompose_divs:
        if article_element.find("div", class_=decompose_div) is not None:
            article_element.find("div", class_=decompose_div).decompose()

    # Default values
    article_insta = None
    article_photo = None
    article_caption = None
    article_video = None
    article_type = 'blog'

    # Test if this is a photo carousel...
    carousel_div = soup.find("div", class_="carousel-inner")
    if carousel_div is not None:
        # This is an image carousel - we would ideally pull ALL the images, but for now we'll just grab the first
        article_div = carousel_div.find("div", class_="active")

        # Have encountered an error on the site where the carousel is empty:
        # https://www.theinertia.com/surf/the-california-collection-photos-from-an-amazing-stretch-of-swell/
        if article_div is not None:
            article_img = article_div.find("img")
            article_photo = article_img.get('src')
            article_caption = '' if article_img.get('alt') is None else article_img.get('alt')
        article_type = 'photos'

    # If this is a video post (no matter what the origin) it will be in an iframe...
    content_iframe = article_element.find("iframe")
    if content_iframe is not None:
        # If the article includes an embedded ooyala player (which is very rare) it's broken, and there's no src on
        # the iframes -- do cleanup so we can get the text out of the article without the garbage.
        oo_div = article_element.find("div", class_="oo-player-container")
        if oo_div is not None:
            # Remove the oo content so that the error message doesn't show up in the article text
            article_element.find("div", class_="oo-player-container").decompose()
            scripts = article_element.find_all("script")
            for script in scripts:
                script.decompose()
        else:
            article_content = content_iframe.get('src')
            if article_content is None:
                # This could be an embedded video hosted on theinertia that has no external URL
                article_type = 'video'
                content_iframe.decompose()
            elif 'youtube' in article_content:
                article_video = "https://youtu.be/{}".format(article_content.split('/')[-1])
                article_type = 'video'
            elif 'vimeo' in article_content:
                if 'cloudfront' in article_content:
                    vid_id = urllib.parse.unquote(article_content.split('?')[1]).split('/')[-1].split('"')[0]
                    article_video = 'https://vimeo.com/{}'.format(vid_id)
                else:
                    article_video = "https://vimeo.com/{}".format(article_content.split('?')[0].split('/')[-1])
                article_type = 'video'
            elif 'instagram' in article_content:
                # There can be two different formats for instagram links: /p/ for plain posts and /tv/ for insta tv
                if 'instagram.com/p/' in article_content:
                    article_insta = "https://www.instagram.com/p/{}".format(article_content.split('/p/')[1].split('/')[0])
                elif 'instagram.com/tv/' in article_content:
                    article_insta = "https://www.instagram.com/tv/{}".format(article_content.split('/tv/')[1].split('/')[0])
                article_type = 'social'

        # Get rid of any and all iframes in the article part of the page
        iframes = article_element.find_all("iframe")
        for iframe in iframes:
            iframe.decompose()

    article_text = article_element.get_text().strip().rstrip('Advertisement')

    # Populate the dictionary and return
    article['text_content'] = article_text
    article['type'] = article_type
    if article_photo:
        article['photo'] = article_photo
    if article_caption:
        article['caption'] = article_caption
    if article_video:
        article['video'] = article_video
    if article_insta:
        article['insta'] = article_insta

    return article


def scrape_article(article):
    """ For the provided article uri, load the article and find whatever data is available

    :param article: The initial fields of the article in a dictionary
    :return:
    """
    # Load the article and wait for it to load
    get_logger().debug("Processing URL: {}".format(article['uri']))

    if not get_driver().get_url(article['uri']):
        # We'll just have to skip this url, can't load it even with retries
        return

    source = get_driver().clean_unicode(get_driver().driver.page_source)
    if 'ERROR 404' in source:
        get_logger().debug("Skipping (url is a 404) - {}".format(article['uri']))
        return

    # There are different formats/html structure so figure out which we're dealing with
    soup = BeautifulSoup(source, "html.parser")
    article_soup = soup.find("div", class_="inertia-article")
    if article_soup is None:
        article_soup = soup.find("main", class_="inertia-article")
    if article_soup is None:
        get_logger().error("Can't find the article container element in: {}".format(article['uri']))
        return()

    # Category
    category_soup = article_soup.find("small", {"itemprop": "articleSection"})
    if category_soup is None:
        category_soup = article_soup.find("span", class_='inertia-category-tag')
    category = category_soup.get_text().strip()
    # print("category: {}".format(category))

    # Title
    title = article_soup.find("h1", {"itemprop": "name"}).get_text().strip().lstrip(category).lstrip()
    # print("title: {}".format(title))

    # Publication Date
    post_date = article_soup.find("time", {"itemprop": "datePublished"}).get("datetime")[:10]
    # print("Post date: {}".format(post_date))

    article['category'] = category
    article['title'] = title
    article['publish_date'] = post_date

    # Author
    author_a = article_soup.find("a", {"rel": "author"})
    if author_a is not None:
        article['author_name'] = author_a.get_text().strip()
        article['author_url'] = author_a.get("href")
        article['author_type'] = article_soup.find(class_="inertia-author-type").get_text().strip()
        # print("{} {} {}".format(author_name, author_url, author_type))
    else:
        get_logger().debug("No author found for this article")
        article['author_name'] = np.NaN
        article['author_url'] = np.NaN
        article['author_type'] = np.NaN

    # Article Content
    article = get_article_content(article_soup, article)

    print(article)
    return article


def extract_articles(category, post_source):
    """

    :param category: The category we're currently scraping
    :param post_source: The html for an entire page of results
    :return: True if we encountered *any* urls that we've already scraped, False if not
    """
    global ALREADY_SCRAPED

    # Extract all the divs containing article cards. There are two possible html layouts
    soup = BeautifulSoup(post_source, "html.parser")
    article_divs = soup.find_all("div", class_="inertia-item")
    if len(article_divs) == 0:
        # Perhaps we're dealing with old html, the class switched in Nov 2018
        article_divs = soup.find_all("div", class_="item")
        if len(article_divs) == 0:
            get_logger().warn("No articles found to extract")
            return

    # From each article div, extract the partial content (url, thumbnail, category) from the card
    articles = []
    get_logger().info("Extracting {} articles starting with: {}".format(len(article_divs), article_divs[0].find('a').get('href')))
    for article_div in article_divs:
        # print(article.prettify())
        url = article_div.find('a').get('href')[:-1]
        if url in ALREADY_SCRAPED:
            continue
        img = article_div.find('img').get('src')
        if img is None:
            img = article_div.find('img').get('data-src')
        if img is not None:
            img = img.replace('https://www', 'cdn1')

        article_json = {"uri": url, "category": category, "thumb": img}
        get_logger().debug("Article card found: {}".format(article_json))
        articles += [article_json]

    article_count = len(articles)

    if article_count > 0:
        articles_completed = []

        # Load each article and extract the rest of the data
        for article in articles:
            article = scrape_article(article)
            articles_completed += [article]

        # Save to file...
        write_articles_to_rds(articles_completed)

    return len(articles)


def load_already_scraped_articles():
    """ Query the database for all articles that have already been scraped

    :return: a list of urls of articles that have already been scraped
    """
    global ALREADY_SCRAPED

    query = """
     SELECT uri
       FROM articles
      WHERE publisher = 'theinertia'
    """
    articles_df = pd.read_sql(query, get_rds_engine())
    ALREADY_SCRAPED = set(articles_df.uri)

    get_logger().debug("Found {} articles already scraped".format(len(ALREADY_SCRAPED)))


def scrape():
    """ This scraper uses an endpoint that controls the paging, but it doesn't exactly match the surf main page. It's
    close enough that I feel pretty good about it

    Categories: Films (broken), Surf, Mountain (skip), Enviro, Health, Photo, Arts, Travel, Women

    :return:
    """
    get_logger().debug("Starting scrape...")
    for cat, catnum in CATEGORIES.items():
        get_logger().debug("Processing category: {}".format(cat))
        pagenum = 0
        empty_pages = 0
        while 1 == 1:
            cat_page_url = SURFCAT_URL.format(catnum, pagenum)
            get_logger().debug("Scraping category page: {}".format(cat_page_url))
            get_driver().get_url(cat_page_url)

            source = get_driver().clean_unicode(get_driver().driver.page_source)

            pagenum += 12

            extracted_count = extract_articles(cat, source)
            if extracted_count == 0:
                if empty_pages < 2:
                    empty_pages += 1
                    continue
                else:
                    get_logger().info("All articles on page {} have already been scraped, exiting...".format(int(pagenum/12)))
                    break

    get_logger().info("Successfully completed scrape of latest The Inertia news.")


@atexit.register
def cleanup():
    get_driver().driver.quit()


if __name__ == "__main__":
    kickoff_time = datetime.now(WESTCOAST).strftime('%Y-%m-%d %H:%M:%S')
    get_logger().info("Kicking off The Inertia scraper at {}...".format(kickoff_time))

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    load_already_scraped_articles()

    scrape()
