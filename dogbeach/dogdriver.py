import os
import time
from dogbeach import doglog
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException


# Use the same current, pinned Chrome release for Selenium and Playwright.
CHROME_BINARY = os.environ.get('YEWREVIEW_CHROME_BINARY', '/opt/yewreview-browsers/156.0.8078.47/chrome-linux64/chrome')
CHROMEDRIVER_BINARY = os.environ.get('YEWREVIEW_CHROMEDRIVER', '/opt/yewreview-browsers/156.0.8078.47/chromedriver-linux64/chromedriver')
os.environ.setdefault('PLAYWRIGHT_NODEJS_PATH', '/opt/node-v24.22.1-linux-x64/bin/node')


def browser_test_args():
    if os.environ.get('YEWREVIEW_UPGRADE_TEST') == '1':
        return ['--host-resolver-rules=MAP yewreview.com 127.0.0.1, MAP www.yewreview.com 127.0.0.1']
    return []


def launch_playwright(playwright):
    return playwright.chromium.launch(
        executable_path=CHROME_BINARY, channel='chrome',
        headless=True, chromium_sandbox=True, args=browser_test_args(),
    )


class DogDriver:
    """ This class will support scraping activities through ChromeDriver """

    def __init__(self, logger=None, sleep=5, tries=10, backoff=.4, pageload_timeout=15):
        self.driver = self.init_driver()
        self.pageload_timeout = pageload_timeout
        self.set_pageload_timeout(pageload_timeout)
        self.sleep = sleep
        self.tries = tries
        self.backoff = 1 + backoff
        self.logger = logger

        if self.logger is not None:
            self.logger.info("Initialized DogDriver with: sleep={}, tries={}, backoff={} and {} logger"
                             .format(sleep, tries, backoff, "no" if logger is None else "a"))
    
    def set_pageload_timeout(self, pageload_timeout):
        self.pageload_timeout = pageload_timeout
        self.driver.set_page_load_timeout(pageload_timeout)
    
    @staticmethod
    def init_driver():
        """ Create a driver object based on default settings and set for this instance

        :return:
        """
        # instantiate a chrome options object so you can set the size and headless preference
        options = Options()
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920x1080")
        options.add_argument('--disable-extensions')
        options.add_argument('--no-proxy-server')
        for argument in browser_test_args():
            options.add_argument(argument)
        options.binary_location = CHROME_BINARY
        return webdriver.Chrome(service=Service(CHROMEDRIVER_BINARY), options=options)

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


if __name__ == "__main__":
    logfile = Path(os.path.dirname(os.path.realpath(__file__))).parent / "log/test.log"
    _logger = doglog.setup_logger("test", logfile)

    d = DogDriver(_logger)
    if d.get_url("http://www.google.com", 2, 2):
        print("Succeeded!")
        print(d.driver.page_source)
    else:
        print("Failed")
