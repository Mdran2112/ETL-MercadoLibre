import logging
from typing import List, Dict, Any

from database.client import DatabaseClient
from utils.decorators import _time_profiling
from utils.time_profilers import ConverterApiTimeProfiler, SearchApiTimeProfiler, AttributesItemApiTimeProfiler, \
    InsertItemsTimeProfiler, InsertItemsShippingTimeProfiler, InsertSellersTimeProfiler, LoadTimeProfiler, \
    ExtractTimeProfiler, TransformTimeProfiler
from utils.useful import flat_map

API_REQUEST_PROFILERS = [ConverterApiTimeProfiler,
                         SearchApiTimeProfiler,
                         AttributesItemApiTimeProfiler]

DB_INSERT_PROFILERS = [InsertItemsTimeProfiler,
                       InsertItemsShippingTimeProfiler,
                       InsertSellersTimeProfiler]

PROCESS_PROFILERS = [ExtractTimeProfiler, TransformTimeProfiler, LoadTimeProfiler]


class Loader:
    """
    Pipeline for storing into MySQL database products, sellers, metrics related to MeLi API requests and metrics related
    to database inserts.
    Attributes
    ----------
    database_client: DatabaseClient
       client used for interacting with database that stores products, sellers and metrics data.
    """

    def __init__(self, database_client: DatabaseClient):
        self.database_client = database_client

    def _insert_items(self, items: List[Dict[str, Any]]) -> None:
        len_items = len(items)
        if len_items == 0:
            logging.warning("No new item_shipping data retrieved.")
            return
        logging.info(f"Inserting {len_items} products.")
        self.database_client.insert_items(items)

    def _insert_sellers(self, sellers: List[Dict[str, Any]]) -> None:
        len_sellers = len(sellers)
        if len_sellers == 0:
            logging.warning("No new sellers data retrieved.")
            return
        logging.info(f"Inserting {len_sellers} sellers.")
        self.database_client.insert_sellers(sellers)

    def _insert_item_shipping(self, item_shipping: List[Dict[str, Any]]) -> None:
        len_item_shipping = len(item_shipping)
        if len_item_shipping == 0:
            logging.warning("No new item_shipping data retrieved.")
            return
        logging.info(f"Inserting {len_item_shipping} items & shipping methods.")
        self.database_client.insert_item_shipping(item_shipping)

    def _insert_request_metrics(self):
        mappers = []
        for profiler in API_REQUEST_PROFILERS:
            mappers.append(profiler.to_json())

        mappers = list(flat_map(lambda x: x, mappers))
        logging.info(f"Inserting {len(mappers)} registries of request metrics.")
        self.database_client.insert_request_metrics(mappers)

    def _insert_database_metrics(self):
        mappers = []
        for profiler in DB_INSERT_PROFILERS:
            mappers.append(profiler.to_json())

        mappers = list(flat_map(lambda x: x, mappers))
        logging.info(f"Inserting {len(mappers)} registries of database metrics.")
        self.database_client.insert_database_metrics(mappers)

    def _insert_process_metrics(self):
        mappers = []
        for profiler in PROCESS_PROFILERS:
            mappers.append(profiler.to_json())

        mappers = list(flat_map(lambda x: x, mappers))
        logging.info(f"Inserting {len(mappers)} registries of process metrics.")
        self.database_client.insert_process_metrics(mappers)

    @_time_profiling(LoadTimeProfiler)
    def _load_items_and_sellers(self, items: List[Dict[str, Any]],
                                sellers: List[Dict[str, Any]],
                                item_shipping: List[Dict[str, Any]]) -> None:
        self._insert_items(items)
        self._insert_item_shipping(item_shipping)
        self._insert_sellers(sellers)

    def load(self, items: List[Dict[str, Any]],
             sellers: List[Dict[str, Any]],
             item_shipping: List[Dict[str, Any]]) -> None:

        self._load_items_and_sellers(items, sellers, item_shipping)

        self._insert_request_metrics()
        self._insert_database_metrics()
        self._insert_process_metrics()

