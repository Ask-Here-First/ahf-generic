"""
# A minimal web servr routing framework.

## The routing mechanism

# URL path fragmentation

This framework does not use decorators for routes; instead it breaks up the
URL path into three parts: a prefix, a medial, a suffix, with the following
rules:
- The path prefix, if non-empty, must start with `/`; it may or may not end
  with a `/`.
- The path medial must not contain a `/`, but can be empty, except for the
  case that an empty prefix is matched to the root path `/`.
  (This is to accommondate the case that, although URL path can be empty but
  the server is still get a single `/` string.)
- The path suffix, if non-empty, may start with a `/` but must not ends with
  a `/`, nor it can contain contiguous `/`s.
The three fragments concatenate into the full URL path without the query string.

### How to split URL path into fragments

#### The path prefix

The framework maintains a set of routers. Each router object is associated
with a prefix string. There are two kinds of routers:
- Method routers - the prefix of a method router must end with a `/`.
- Functor routers - the prefix of an functor router must not end with a `/`,
  but a matching prefix, if not the entire URL path, must be followed by a '/'
  in the URL path.

The shortest possible router prefix is an empty string which is for a functor
router. In the implementation, if enabled, it is always mapped to the root
file router to access static files (assets) on disk.

Since there are multiple routes with different routing prefix, the router
with the longest matching prefix wins.

#### The path medial

A functor router has a single action: the router itself, which must be a
callable. The path medial is always empty unless the URL path is the root
`"/"` and the matching prefix is empty; in this case the path medial is `/`
(and the path suffix is empty).

A method router is a class object with many methods, and some methods
represents actions. We first extract the path medial string following
the path prefix to before the next `/` (or to the end).

Let `MEDIAL` be the value of the path medial, then we try to find the following
callable attributes in the router object, depending on the HTTP method:

- For HTTP GET: `get_MEDIAL`,
- For HTTP POST: `set_MEDIAL` or `post_MEDIAL`, in this order,
- For HTTP GET and POST: `add_MEDIAL`, if none of above attributes are found,
- for HTTP PUT: `put_MEDIAL`,
- For HTTP DELETE, `del_MEDIAL` or `delete_MEDIAL`, in this order,
- For HTTP PATCH, `add_MEDIAL` or `patch_MEDIAL`, in this order,
- For all HTTP methods, if none of the above attributes is found, try to
  find `use_MEDIAL`.

If none of the above attributes are found in the method router, we set
path medial to be an empty string (i.e., with `MEDIAL=''`), then repeat the
search for methods as above to find the callable attributes `get_`, `post_`,
`use_` and alike. If this results in a match, then the path medial is an empty.

The found attribute that matches to the medial and HTTP method is the action.

#### The path suffix

The remaining string after the path medial is the path suffix.
By the way how prefix and medial are matched, the path suffix must either
starts with a `/` and starts right after a `/`. The latter happens only
for a method router with an empty medial.

When parsing the suffix, the leading `/`, if any, is removed first.
The suffix is then split into multiple items with `/`.
Each item is processed this way:
- The item is percentage decoded;
- Then it is parsed into a `FridValue`.
The result is an array of `FridValue`s.

Since FridValue string representation does not allow empty string,
there will be no continguous `/`s or `/` at the end. If this is
the case, a http 307 is issued to remove extra `/`s.

## How actions are called

How actions are called depends if the router is a class type object or
a class instance object, and if the router is a method router or a functor
router.

Note that for all cases, the URL provides the following arguments:

- Variable positional arguyments (`vpargs`) which comes from the path suffix,
  and
- Keywoard arguments (`kwargs`) which comes from the query.

Also, whenever `kwargs` is used a keyword argument named `_http` with a `dict`
value is first added to the keyword arguments as an additional keyword argument
along with the other arguments of `kwargs`. If the action does not accept
this additional argument, (i.e., a `TypeError` is raised when calling), then
`_http` keyword argument is removed from `kwargs` and the same call is tried
again.

### For a method router which is a class instance object

The action is called with fixed positional arguments (`fpargs`), followed by
`vpargs` coming from the path suffix, then keyword arguments `kwargs` coming
from URL query string; in other words, the action is called in this way:
`action(*fpargs, *vpargs, **kwargs)`.

For a method router, the number of fix positional arguments depends on the name
of object methods:
- For `get_*`, `del_*`, `delete_*` methods, there is no fixed positional
  argument, i.e., `fpvargs=()`.
- For `set_*`, `post_*`, `put_*`, `add_*`, and `patch_*`, the request body
  parsed as enhanced JSON to `FridValue` is passed as one and only fixed
  positional argument, i.e., `fpargs=(HTTP_DATA,)`.
- For `use_*`, two fixed positional arguments are passed; the first is a
  the processed request body as a `FridValue`, or `None` if the body is
  missing; and the second is a string of one of the following:
  `get` for HTTP GET, `set` for HTTP POST, `put` for HTTP PUT,
  `add` for HTTP PATCH, and `del` for HTTP DELETE; in other words,
  `fpargs=(HTTP_DATA, 'get'|'set'|'put'|'add'|'del')`.

### For a method router which is a class type object

The constructor of the class type is first called to instantiate an class
instance, with arguments `(*vpargs, **kwargs)`. In other words, all arguments
coming from the URL is passed to the constructor instead of methods.
Then a method of the instance is called in the same way as described with above,
but only with the arguments `(*fpargs)` (so either zero, one or two arguments).

### For a functor router which is a class instance object

For functor router that is an class object, only GET is supported.
The functor is called with `(*vpargs, **kwargs)`.

### For a method router which is a class instance object

The constructor of the class type is first called to instantiate an class
instance, with arguments `(*vpargs, **kwargs)`. Then the instance is called
as a functor as follows:

- For `GET`, the functor is called with zero argument.
- For `POST`, the functor is called with only one argument, the post data.
- For other HTTP method, the functor is called with two arguments:
  the first is the data (or None without HTTP body), and the second
  is an operation type string: `put` for HTTP PUT, `add` for HTTP PATCH, and
  `del` for HTTP DELETE.

## How to generate HTTP response

- If the action returns `None`, HTTP 204 is returned with no content body.
- If the action returns a string, it is UTF-8 encoded with with MIME-type
  `text/plain`.
- If the action returns a blob, it is used as is with a binary MIME-type
  and a HTTP 200 code.
- If other frid-compatible values are returned, they are converted to
  enhanced JSON format with MIME-type `application/json` and 200 code.
- If the action returns a tuple with two or three elements:
    + the first element is handled as the returned data as above;
    + The second element, is either of
        + An integer for status code (in which case MIME-type is determined
          by data as above), or
        + A string for MIME-type (in which case the status code is 200 or 204
          depending if data is None).  One can use short-hand for MIME-types:
          `text` for `text/plain`, `html` for `text/html`, `blob` for binary,
          `json` for `application/json`.
    + The third element, if present, is of `Mapping[str,str]` for additional
      route-specific headers.
- If a HttpError is returned or raised, its `ht_status` code is used as the
  HTTP status, and headers therein are also used.
- If other exceptions are raised, an 500 status code is returned with generated
  headers.
"""

from .mixin import parse_url_query, parse_http_body, build_http_body, HttpMixin, HttpError
from .route import HttpInput, EchoRouter
from .files import FileRouter

__all__ = [
    'parse_url_query', 'parse_http_body', 'build_http_body',
    'HttpMixin', 'HttpError', 'HttpInput', 'EchoRouter', 'FileRouter'
]
