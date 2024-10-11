import base64

def base64url_encode(b: bytes) -> str:
    """Base64 encoding without padding.
    - It is like what JavaScript s.toString('base64url') does.
    """
    return base64.urlsafe_b64encode(b).rstrip(b'=').decode()

def base64url_decode(s: str) -> bytes:
    """Base64 decoding for a string without padding."""
    # Nothing to do if it is already padded
    n = len(s) & 3
    if n:
        s += '=' * (4-n)
    return base64.urlsafe_b64decode(s)
