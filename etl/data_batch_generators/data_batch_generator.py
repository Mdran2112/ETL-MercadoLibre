import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import List, Dict, Any

from utils.useful import drop_repeated_dicts, flat_map


@dataclass
class DataBatchGenerator:

    @property
    def entity(self) -> str:
        return ""

    def _drop_repeated(self, objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        len_item_list = len(objects)
        objects = drop_repeated_dicts(objects)  # drop repeated items_list
        if len_item_list - len(objects) > 0:
            logging.warning(f"{len_item_list - len(objects)} {self.entity} registries were repeated.")
        return objects

    def build(self, items_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ...


@dataclass
class SellersBatchGenerator(DataBatchGenerator):
    """
    generator of sellers from preprocessed data
    """
    @property
    def entity(self): return "seller"

    def build(self, items_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sellers = list(map(lambda x: {"seller_id": x["seller_id"],
                                      "completed_sales": x["completed_sales"]}, items_list))
        sellers = self._drop_repeated(sellers)
        return sellers


@dataclass
class ShippingBatchGenerator(DataBatchGenerator):
    """
    generator of item shipping objects from preprocessed data
    """
    @property
    def entity(self): return "item_shipping"

    def build(self, items_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        item_shipping = list(flat_map(lambda x: [{"item_id": x["id"],
                                                  "shipping_method": sh_meth}
                                                 for sh_meth in x["shipping"]], items_list))

        item_shipping = self._drop_repeated(item_shipping)
        return item_shipping


@dataclass
class ProductsBatchGenerator(DataBatchGenerator):
    """
    generator of product objects from preprocessed data.
    Attributes
    ----------
    drop_keys: List[str]
        list of keys that will be removed from original data.
    """
    drop_keys: List[str]

    @property
    def entity(self): return "product"

    def build(self, items_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        item_list_copy = deepcopy(items_list)
        for item in item_list_copy:
            for key in self.drop_keys:
                item.pop(key, None)

        item_list_copy = self._drop_repeated(item_list_copy)
        return item_list_copy