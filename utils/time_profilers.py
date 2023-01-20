from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any


@dataclass
class TimeProfilerMetrics:
    request_time: float
    date: datetime


class TimeProfilerBase:
    metrics: List[TimeProfilerMetrics]

    @classmethod
    def to_json(cls) -> List[Dict[str, Any]]:
        ...


class APIRequestTimeProfilerBase(TimeProfilerBase):
    api_name: str = ""
    metrics: List[TimeProfilerMetrics]

    @classmethod
    def to_json(cls) -> List[Dict[str, Any]]:
        return list(map(lambda m: {"date": m.date,
                                   "api_name": cls.api_name,
                                   "request_time": m.request_time},
                        cls.metrics))


class DBInsertTimeProfilerBase(TimeProfilerBase):
    table: str = ""
    metrics: List[TimeProfilerMetrics]

    @classmethod
    def to_json(cls) -> List[Dict[str, Any]]:
        return list(map(lambda m: {"date": m.date,
                                   "table_name": cls.table,
                                   "insert_time": m.request_time},
                        cls.metrics))


class ProcessTimeProfilerBase(TimeProfilerBase):
    process: str = ""
    metrics: List[TimeProfilerMetrics]

    @classmethod
    def to_json(cls) -> List[Dict[str, Any]]:
        return list(map(lambda m: {"date": m.date,
                                   "process_name": cls.process,
                                   "process_time": m.request_time},
                        cls.metrics))


class ConverterApiTimeProfiler(APIRequestTimeProfilerBase):
    api_name: str = "currency_coverter"
    metrics: List[TimeProfilerMetrics] = []


class SearchApiTimeProfiler(APIRequestTimeProfilerBase):
    api_name: str = "search"
    metrics: List[TimeProfilerMetrics] = []


class AttributesItemApiTimeProfiler(APIRequestTimeProfilerBase):
    api_name: str = "item_attributes"
    metrics: List[TimeProfilerMetrics] = []


class InsertItemsTimeProfiler(DBInsertTimeProfilerBase):
    table: str = "items"
    metrics: List[TimeProfilerMetrics] = []


class InsertItemsShippingTimeProfiler(DBInsertTimeProfilerBase):
    table: str = "item_shipping"
    metrics: List[TimeProfilerMetrics] = []


class InsertSellersTimeProfiler(DBInsertTimeProfilerBase):
    table: str = "sellers"
    metrics: List[TimeProfilerMetrics] = []


class ExtractTimeProfiler(ProcessTimeProfilerBase):
    process: str = "extract"
    metrics: List[TimeProfilerMetrics] = []


class TransformTimeProfiler(ProcessTimeProfilerBase):
    process: str = "transform"
    metrics: List[TimeProfilerMetrics] = []


class LoadTimeProfiler(ProcessTimeProfilerBase):
    process: str = "load"
    metrics: List[TimeProfilerMetrics] = []
