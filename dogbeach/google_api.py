"""Keep Google request URLs and their API keys out of routine error logs."""
from googleapiclient.errors import HttpError


def safe_google_call(operation):
    try:
        return operation()
    except HttpError as error:
        # HttpError stringifies the full request URI, including developerKey.
        status = getattr(error.resp, 'status', None)
        suffix = f" (HTTP {status})" if type(status) is int and 100 <= status <= 599 else ""
        raise RuntimeError(f"Google API request failed{suffix}") from None
