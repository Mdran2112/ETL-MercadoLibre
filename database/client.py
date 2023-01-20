from datetime import date
from typing import List, Dict, Any, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from database.models import Item, ItemShipping, Seller, RequestMetrics, DatabaseMetrics, ProcessMetrics
from utils.decorators import _time_profiling
from utils.time_profilers import InsertItemsTimeProfiler, InsertItemsShippingTimeProfiler, InsertSellersTimeProfiler


class DatabaseClient:
    """
    client used for interacting with database that stores products, sellers and metrics data.
    Attributes
    ----------
    session: Session
       SQLAlchemy session.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    @_time_profiling(InsertItemsTimeProfiler)
    def insert_items(self, objects: List[Dict[str, Any]]) -> None:
        self._session.bulk_insert_mappings(mapper=Item, mappings=objects)
        self.session_commit()

    @_time_profiling(InsertItemsShippingTimeProfiler)
    def insert_item_shipping(self, objects: List[Dict[str, Any]]) -> None:
        self._session.bulk_insert_mappings(mapper=ItemShipping, mappings=objects)
        self.session_commit()

    @_time_profiling(InsertSellersTimeProfiler)
    def insert_sellers(self, objects: List[Dict[str, Any]]) -> None:
        self._session.bulk_insert_mappings(mapper=Seller, mappings=objects)
        self.session_commit()

    def insert_request_metrics(self, objects: List[Dict[str, Any]]) -> None:
        self._session.bulk_insert_mappings(mapper=RequestMetrics, mappings=objects)
        self.session_commit()

    def insert_database_metrics(self, objects: List[Dict[str, Any]]) -> None:
        self._session.bulk_insert_mappings(mapper=DatabaseMetrics, mappings=objects)
        self.session_commit()

    def insert_process_metrics(self, objects: List[Dict[str, Any]]) -> None:
        self._session.bulk_insert_mappings(mapper=ProcessMetrics, mappings=objects)
        self.session_commit()

    def session_commit(self) -> None:
        self._session.commit()

    def get_current_day_item_ids(self) -> List[str]:
        current_day_items = self._session.query(Item).filter(func.date(Item.created_at) == date.today()).all()
        current_day_item_ids = list(map(lambda i: i.id, current_day_items))
        return current_day_item_ids

    def get_sellers_data(self) -> List[Tuple[int, int]]:
        sellers = self._session.query(Seller).all()
        sellers = list(map(lambda i: (i.seller_id, i.completed_sales), sellers))
        return sellers
