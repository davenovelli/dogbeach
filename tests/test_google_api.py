import traceback
import unittest
from unittest.mock import Mock

from googleapiclient.errors import HttpError
from dogbeach.google_api import safe_google_call


class GoogleApiTests(unittest.TestCase):
    def test_success_preserves_response(self):
        response = object()
        self.assertIs(safe_google_call(lambda: response), response)

    def test_http_failure_does_not_log_uri_body_or_credential(self):
        secret = 'synthetic-test-key'
        failure = HttpError(Mock(status=403, reason='Forbidden'),
                            ('{"error":{"message":"' + secret + '"}}').encode(),
                            uri='https://example.invalid/?key=' + secret)
        try:
            safe_google_call(Mock(side_effect=failure))
        except RuntimeError:
            output = traceback.format_exc()
        else:
            self.fail('Expected safe Google failure')
        self.assertIn('Google API request failed (HTTP 403)', output)
        self.assertNotIn(secret, output)
        self.assertNotIn('example.invalid', output)
        self.assertNotIn('HttpError', output)


if __name__ == '__main__':
    unittest.main()
