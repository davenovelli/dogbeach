import os
import time
from dogbeach import doglog
from sys import platform
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException


class DogDriver:
    """ This class will support scraping activities through ChromeDriver """

    def __init__(self, logger=None, sleep=5, tries=10, backoff=.4, pageload_timeout=15):
        self.driver = self.init_driver()
        self.driver.set_page_load_timeout(pageload_timeout)
        self.sleep = sleep
        self.tries = tries
        self.backoff = 1 + backoff
        self.logger = logger

        if self.logger is not None:
            self.logger.info("Initialized DogDriver with: sleep={}, tries={}, backoff={} and {} logger"
                             .format(sleep, tries, backoff, "no" if logger is None else "a"))

    @staticmethod
    def init_driver():
        """ Create a driver object based on default settings and set for this instance

        :return:
        """
        # instantiate a chrome options object so you can set the size and headless preference
        options = Options()
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--no-proxy-server')

        # download the chrome driver from https://sites.google.com/a/chromium.org/chromedriver/downloads and put it in
        # the current directory
        currdir = os.path.dirname(os.path.realpath(__file__)) + "/{}"
        if "linux" in platform:
            return webdriver.Chrome(chrome_options=options, executable_path=currdir.format("chromedriver_linux"))
        else:
            return webdriver.Chrome(chrome_options=options, executable_path=currdir.format("chromedriver"))

    def get_url(self, url, sleep=None, tries=None):
        """ Recursive method to retrieve the

        :param url: The URL to load
        :param sleep: The number of seconds (int) to wait after request
        :param tries: The number of times to retry before giving up
        :return: True if successful, False otherwise
        """
        s = self.sleep if sleep is None else sleep
        t = self.tries if tries is None else tries

        # Attempt to load the page, catch and log any exceptions
        try:
            self.driver.get(url)
            time.sleep(s)
            return True
        except TimeoutException:
            if self.logger is not None:
                self.logger.error("TimeoutException on: {}".format(url), exc_info=True)
        except WebDriverException:
            if self.logger is not None:
                self.logger.warn('Error retrieving page after waiting {} seconds: {}'.format(s, url), exc_info=True)
        
        # If this is the last attempt, log an error and return False
        if t == 1:
            if self.logger is not None:
                self.logger.error("Failed to retrieve the page before running out of retries")
            return False

        # Calculate the new duration to sleep, backoff AT LEAST 1 second
        newsleep = s + max(int(self.backoff * s), 1)

        return self.get_url(url, newsleep, t - 1)

    @staticmethod
    def clean_unicode(source):
        """Clean unhelpful unicode characters out of scraped page content before saving

        :param content: Page source from a scraped url
        :return: cleaned up source
        """
        return source \
            .replace('\u201c', '"') \
            .replace('\u201d', '"') \
            .replace('\u2018', "'") \
            .replace('\u2019', "'") \
            .replace('\u00a0', " ") \
            .replace('\u2013', '-') \
            .replace('\u2014', '-')


if __name__ == "__main__":
    logfile = Path(os.path.dirname(os.path.realpath(__file__))).parent / "log/test.log"
    _logger = doglog.setup_logger("test", logfile)

    d = DogDriver(_logger)
    if d.get_url("http://www.google.com", 2, 2):
        print("Succeeded!")
        print(d.driver.page_source)
    else:
        print("Failed")
