"""Authenticated, origin-bound API requests. Never attach service tokens to publisher requests."""
import os
from urllib.parse import urlsplit
import requests


class ApiClient:
    def __init__(self, base_url, config):
        self.origin = self._origin(base_url)
        if os.environ.get('YEWREVIEW_ENVIRONMENT') == 'local' and self.origin[1] not in ('api', 'localhost', '127.0.0.1'):
            raise ValueError('Local scraper configuration must target the local API')
        token = os.environ.get('SCRAPER_API_TOKEN') or config['common'].get('api_token')
        if not token:
            raise ValueError('Configure a scraper API token before publishing')
        if self.origin[0] != 'https' and self.origin[1] not in ('api', 'localhost', '127.0.0.1'):
            raise ValueError('Remote API connections must use HTTPS')
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers['Authorization'] = 'Bearer ' + token

    @staticmethod
    def _origin(url):
        value = urlsplit(url)
        if value.scheme not in ('http', 'https') or not value.hostname or value.username or value.password:
            raise ValueError('Invalid API URL')
        return value.scheme, value.hostname.lower(), value.port or (443 if value.scheme == 'https' else 80)

    def _request(self, method, url, **kwargs):
        if self._origin(url) != self.origin:
            raise ValueError('Refusing to send API credentials to another origin')
        kwargs['timeout'] = (5, 20)
        kwargs['allow_redirects'] = False
        response = self.session.request(method, url, **kwargs)
        if 300 <= response.status_code < 400:
            raise requests.RequestException('API redirects are not allowed')
        return response

    def get(self, url, **kwargs):
        return self._request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self._request('POST', url, **kwargs)
