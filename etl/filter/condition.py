import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple


@dataclass
class Condition:

    def satisfies(self, result: Dict[str, Any]) -> bool:
        ...


@dataclass
class NotNewProduct(Condition):

    def satisfies(self, result: Dict[str, Any]) -> bool:
        if result["condition"] == "new":
            return True
        logging.warning(f"item {result['id']} is not new. Will be filtered.")
        return False


@dataclass
class ItemAlreadyStored(Condition):
    current_day_item_ids: List[str]

    def satisfies(self, result: Dict[str, Any]) -> bool:
        if result["id"] not in self.current_day_item_ids:
            return True
        logging.warning(f"item {result['id']} has already been stored in database. Will be filtered.")
        return False

@dataclass
class SellerAlreadyStored(Condition):
    seller_data: List[Tuple[int, int]]

    def satisfies(self, seller: Dict[str, Any]) -> bool:
        if (seller["seller_id"], seller["completed_sales"]) not in self.seller_data:
            return True
        logging.warning(f"Seller {seller['seller_id']} is already in database and doesn't have changed its completed sales"
                        f". This registry will be filtered.")
        return False
