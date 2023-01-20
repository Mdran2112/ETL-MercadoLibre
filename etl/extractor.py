import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional

from requests import Response

from meli.api_client import MLApiClient
from etl.filter.filter import Filter
from utils.decorators import _time_profiling
from utils.exceptions import ResultsNotFoundException
from utils.time_profilers import ExtractTimeProfiler
from utils.useful import check_status_response


class Extractor:

    """
    Used for data extraction of products.
    Attributes
    ----------
    ml_api_client: MLApiClient
        client for requesting data to MeLi API.
    _filter: Optional[Filter]
        results filter. To every item object found, a filter would be applied according to a list of Condition.
    """

    def __init__(self, ml_api_client: MLApiClient, _filter: Optional[Filter]) -> None:
        self.ml_api_client = ml_api_client
        self._filter = _filter

    def _apply_filter(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._filter.apply_to_all(results)

    def _get_warranty_for_all(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        The warranty is not included in the response of the endpoint https://api.mercadolibre.com/sites/<country_ml>/search/...
        So this method uses the ml_api_client for requesting items attributes. Then, for each item grabs the 'warranty'
        attribute and inserts it in the item dictionary.
        :param results: List of items.
        :return: List of items with warranty
        """

        executor = ThreadPoolExecutor(max_workers=100)

        def get_attributes(item: Dict[str, Any]):
            resp = self.ml_api_client.request_to_item_attributes_api(item_id=item["id"])
            check_status_response(resp)
            item["warranty"] = resp.json()["warranty"]

        _ = [executor.submit(get_attributes, item) for item in results]
        executor.shutdown()

        return results

    @staticmethod
    def _check_if_items(len_items_list: int):
        if len_items_list == 0:
            raise ResultsNotFoundException("No new items were found in ML Search API.")

    @_time_profiling(ExtractTimeProfiler)
    def search(self, query: str, exclude_seller_id: int, max_items: int = 200) -> List[Dict[str, Any]]:
        """
        Uses the ml_api_client for requesting products information, according to the input params. Also uses the _filter
         (if it was defined in the object constructor) for discarding useless products (for example, 'used' products or
          products that were already stored in database.):
        :param query: query param related to item title. For example: 'Samsung Galaxy'
        :param exclude_seller_id: exclude results of items that belongs to that seller.
        :param max_items: max amount of items that will be returned.
        :return: List of items with useful attributes (item_id, item_title, seller, warranty, etc.)
        """
        item_list = []
        offset = 0
        step = 50
        limit = 50
        len_item_list = 0
        #received = 0
        while len_item_list < max_items:
            resp: Response = self.ml_api_client.request_to_search_api(query, exclude_seller_id, offset, limit)
            if resp.status_code != 200:
                logging.warning(f"status code: {resp.status_code}. Detail: {resp.json()}")
                break
            results = resp.json()["results"]
            batch_len = len(results)
            if batch_len == 0:
                break

            if self._filter:
                results = self._apply_filter(results)

            after_filter_batch_len = len(results)
            item_list += results
            len_item_list = len(item_list)
            logging.info(f"{len_item_list}/{max_items} items. {batch_len} items were received and {batch_len - after_filter_batch_len} were filtered.")
            offset = offset + step

            #received += batch_len
            #if received >= max_items:
            #    logging.warning(f"{received} items were received and {len_item_list} will be processed.")
            #    break

        self._check_if_items(len_item_list)

        if len_item_list > max_items:
            item_list = item_list[:max_items]
            logging.warning(f"{len_item_list - max_items} were left out.")

        item_list = self._get_warranty_for_all(item_list)

        return item_list
