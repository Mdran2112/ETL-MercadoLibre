import argparse
from functools import reduce
from typing import List, Dict, Any

from requests import Response, RequestException


def check_status_response(response: Response) -> None:
    if response.status_code != 200:
        raise RequestException(f"status code: {response.status_code}. Detail: {response.json()}")


flat_map = lambda f, xs: reduce(lambda a, b: a + b, map(f, xs))


def drop_repeated_dicts(object: List[Dict[str, Any]]):
    return [dict(s) for s in set(tuple(x.items()) for x in object)]


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
