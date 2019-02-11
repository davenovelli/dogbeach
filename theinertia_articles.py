import os
import sys
import json
import atexit
import urllib
import logging
import traceback
import pandas as pd
from pathlib import Path
from util import doglog
from util.dogdriver import DogDriver
from bs4 import BeautifulSoup

ARTICLE_TEMPLATE = 'https://www.theinertia.com/{}'

_logger = None
_driver = None


def get_logger():
    """

    :return:
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "log/theinertia_articles.log"
        _logger = doglog.setup_logger("theinertia_article", logfile, clevel=logging.DEBUG)
    return _logger


def get_driver():
    """

    :return:
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger())
    return _driver


def save_article_to_json(tup):
    """ Take a tuple containing all the useful attributes from an article and save it to it's own json file

    (slug, category, title, fblikes, twlikes, post_date, author_name, author_url, author_type)
    + (article_photo, article_caption, article_video, article_insta, article_text, article_type)

    :param tup:
    :return:
    """
    # First we'll store the tuple as a dictionary:
    article = {
        'slug': tup[0],
        'category': tup[1],
        'title': tup[2],
        'fblikes': tup[3],
        'twlikes': tup[4],
        'published': tup[5],
        'author': tup[6],
        'author_url': tup[7],
        'author_type': tup[8],
        'article_photo': tup[9],
        'article_caption': tup[10],
        'article_video': tup[11],
        'article_insta': tup[12],
        'content': tup[13],
        'article_type': tup[14]
    }

    with open("data/article_json/theinertia/{}.json".format(tup[0].replace('/', '--')), 'w') as f:
        json.dump(article, f)


def get_article_content(soup):
    """ This is a bit tricky, because the format and location of the images and/or videos is different depending on the
    content type. There's no clear indication of what's what, so we'll just have to do lots of tests...

    :param soup:
    :return:
    """
    article = soup.find("article", {"itemprop": "articleBody"})

    decompose_divs = ['page-numbers', 'social-share', 'trc_related_container', 'inertia-comments-container',
                      'inertia-endless-articles']
    for decompose_div in decompose_divs:
        if article.find("div", class_=decompose_div) is not None:
            article.find("div", class_=decompose_div).decompose()

    article_photo = ''
    article_caption = ''
    article_video = ''
    article_insta = ''
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
    content_iframe = article.find("iframe")
    if content_iframe is not None:
        # If the article includes an embedded ooyala player (which is very rare) it's broken, and there's no src on
        # the iframes -- do cleanup so we can get the text out of the article without the garbage.
        oo_div = article.find("div", class_="oo-player-container")
        if oo_div is not None:
            # Remove the oo content so that the error message doesn't show up in the article text
            article.find("div", class_="oo-player-container").decompose()
            scripts = article.find_all("script")
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
                article_type = 'photos'
            elif 'instagram' in article_content:
                article_insta = "https://www.instagram.com/p/{}".format(article_content.split('/p/')[1].split('/')[0])
                article_type = 'social'

        # Get rid of any and all iframes in the article part of the page
        iframes = article.find_all("iframe")
        for iframe in iframes:
            iframe.decompose()

    article_text = article.get_text().strip()
    article_text = article_text.rstrip('Advertisement')

    return (article_photo, article_caption, article_video, article_insta, article_text, article_type)


def scrape_article(slug):
    """ For the provided slug, load the article and find whatever data is available

    :param driver:
    :param url:
    :return:
    """
    d = get_driver()
    # Load the article and wait for it to load
    url = ARTICLE_TEMPLATE.format(slug)
    get_logger().debug("Processing URL: {}".format(url))

    if not d.get_url(url):
        # We'll just have to skip this slug, can't load it even with retries
        return

    source = d.clean_unicode(d.driver.page_source)
    if 'ERROR 404' in source:
        get_logger().debug("Skipping (url is a 404) - {}".format(url))
        return

    soup = BeautifulSoup(source, "html.parser")
    article_soup = soup.find("div", class_="inertia-article")
    if article_soup is None:
        article_soup = soup.find("main", class_="inertia-article")
    if article_soup is None:
        get_logger().error("Can't find the article container element in: {}".format(url))
        return()

    # Category
    category_soup = article_soup.find("small", {"itemprop": "articleSection"})
    if category_soup is None:
        category_soup = article_soup.find("span", class_='inertia-category-tag')
    category = category_soup.get_text().strip()
    print("category: {}".format(category))

    # Title
    title = article_soup.find("h1", {"itemprop": "name"}).get_text().strip().lstrip(category).lstrip()
    print("title: {}".format(title))

    # Social Likes
    fblikes = 0
    twlikes = 0
    social_ul = article_soup.find("ul", class_="inertia-share-section")
    if social_ul is not None:
        fblikes = social_ul.find("li", {"data-network": "FB"}).get_text().strip()
        twlikes = social_ul.find("li", {"data-network": "TW"}).get_text().strip()
        if twlikes == 'tweet':
            twlikes = '0'
    print("FB likes: {}, Twitter likes: {}".format(fblikes, twlikes))

    # Publication Date
    post_date = article_soup.find("time", {"itemprop": "datePublished"}).get("datetime")
    print("Post date: {}".format(post_date))

    # Author
    author_a = article_soup.find("a", {"rel": "author"})
    if author_a is not None:
        author_name = author_a.get_text().strip()
        author_url = author_a.get("href")
        author_type = article_soup.find(class_="inertia-author-type").get_text().strip()
        print("{} {} {}".format(author_name, author_url, author_type))
    else:
        author_name = ''
        author_url = ''
        author_type = ''
        get_logger().debug("No author found for this article")

    # Article Content
    content_tup = get_article_content(article_soup)

    tup = (slug, category, title, fblikes, twlikes, post_date, author_name, author_url, author_type) + content_tup

    print(tup)

    save_article_to_json(tup)


def scrape_articles():
    """ Load each article url and extract all useful info from it, saving each article separately to a json file

    :return:
    """
    # Read in the list of all articles from CSV
    articles_list_df = pd.read_csv('data/theinertia_articles.csv')
    articles_list_df = articles_list_df[pd.notnull(articles_list_df.url)]
    articles_list = articles_list_df.url.astype(str).unique()

    get_logger().debug("{} articles have been found: \n{}".format(len(articles_list), articles_list))

    # Then read in the list of article slugs that have been scraped already
    slugs = []
    for filename in os.listdir("data/article_json/theinertia"):
        slug = filename.split('.')[0]
        slugs += [slug.replace('--', '/')]
    get_logger().debug("{} articles have been scraped: \n{}".format(len(slugs), "\n".join(slugs)))

    # Find any articles whose content hasn't been scraped yet and scrape it, adding it to the csv
    slugs = list(set(articles_list) - set(slugs))
    get_logger().info("{} slugs haven't been crawled yet: \n{}".format(len(slugs), slugs))

    for slug in slugs:
        get_logger().info("Scraping slug: {}".format(slug))

        # scrape_article(slug)
        try:
            scrape_article(slug)
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

    scrape_articles()
