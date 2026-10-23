import os
import unittest
from unittest.mock import Mock, patch
from dogbeach.api import ApiClient


class ApiClientTests(unittest.TestCase):
    def make_client(self):
        return ApiClient('http://api:31169', {'common': {'api_token': 'local-test-service-token'}})

    def test_service_token_and_deadlines_only_for_exact_api_origin(self):
        with patch.dict(os.environ, {'YEWREVIEW_ENVIRONMENT': 'local'}):
            client = self.make_client()
            self.assertEqual(client.session.headers['Authorization'], 'Bearer local-test-service-token')
            response = Mock(status_code=200)
            client.session.request = Mock(return_value=response)
            self.assertIs(client.post('http://api:31169/article', json={'title': 'fixture'}), response)
            args = client.session.request.call_args.kwargs
            self.assertEqual(args['timeout'], (5, 20))
            self.assertFalse(args['allow_redirects'])
            for target in ['https://example.com/article', 'http://api:22/', 'http://user:password@api:31169/']:
                with self.assertRaises(ValueError): client.get(target)
            self.assertEqual(client.session.request.call_count, 1)

    def test_local_config_cannot_publish_to_production(self):
        with patch.dict(os.environ, {'YEWREVIEW_ENVIRONMENT': 'local'}):
            with self.assertRaises(ValueError): ApiClient('https://yewreview.com:8081', {'common': {'api_token': 'fixture'}})

    def test_missing_token_and_redirects_fail_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError): ApiClient('http://api:31169', {'common': {}})
        client = self.make_client()
        client.session.request = Mock(return_value=Mock(status_code=302))
        import requests
        with self.assertRaises(requests.RequestException): client.get('http://api:31169/article')


if __name__ == '__main__': unittest.main()
