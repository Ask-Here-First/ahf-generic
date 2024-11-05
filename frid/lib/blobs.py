import base64

def base64url_encode(b: bytes) -> bytes:
    """Base64 encoding without padding.
    - It is like what JavaScript s.toString('base64url') does.
    - It returns bytes as all base64 functions.
    """
    return base64.urlsafe_b64encode(b).rstrip(b'=')

def base64url_decode(s: str|bytes) -> bytes:
    """Base64 decoding for a string without padding."""
    # Nothing to do if it is already padded
    if isinstance(s, str):
        s = s.encode('ascii')
    n = len(s) & 3
    if n:
        s = bytes(s) + b'=' * (4-n)
    return base64.urlsafe_b64decode(s)
