import logging
import time
from datetime import datetime
from functools import wraps
from typing import List, Type, Callable, Any

from requests import RequestException, Response

from utils.exceptions import ResultsNotFoundException
from utils.time_profilers import TimeProfilerBase, TimeProfilerMetrics


def handle_error(f):
    @wraps(f)
    def decorated_function(*args, **kws):
        try:
            resp = f(*args, **kws)
            return resp
        except RequestException as ex:
            print("Request Exception: " + str(ex))
        except ResultsNotFoundException as ex:
            print("Exception: No items were found in ML Search API.")
        except Exception as ex:
            import traceback
            traceback = ''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__))
            print(str(ex))
            logging.error(traceback)

    return decorated_function


def _time_profiling(time_profiler: Type[TimeProfilerBase]):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kws):
            t0 = time.time()
            resp = f(*args, **kws)
            request_time = time.time() - t0
            if isinstance(resp, Response) and resp.status_code != 200:
                logging.warning("Couldn't measure api request time")
                return resp
                #raise RequestException
            metrics = TimeProfilerMetrics(request_time=request_time, date=datetime.now())
            time_profiler.metrics.append(metrics)
            return resp

        return wrapper

    return decorator
