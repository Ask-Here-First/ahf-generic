# TODO: DELETE this file at 0.5.0.

from frid import load_module_data

# This is for backward compatibility to be removed at 0.5.0
from .lib import path_to_url_path, url_path_to_path  # noqa: F401

# TODO: move this single function to be defined directly in __init__.py

# For backward compatibility
load_data_in_module = load_module_data
os_path_to_url_path = path_to_url_path
url_path_to_os_path = url_path_to_path
