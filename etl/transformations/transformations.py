from dataclasses import dataclass
from typing import List, Dict, Any


class Transformation:

    def apply(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        ...


@dataclass
class HandleNoWarrantyString(Transformation):
    no_warranty_strings: List[str]

    def apply(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        if atribs["warranty"] in self.no_warranty_strings:
            atribs["warranty"] = None
        return atribs


@dataclass
class PriceConverter(Transformation):
    currency_factor: float

    def apply(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        atribs["price"] = atribs["price"] * self.currency_factor
        return atribs


@dataclass
class InsertSellerID(Transformation):

    def apply(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        atribs["seller_id"] = atribs["seller"]["id"]
        return atribs

@dataclass
class InsertSellerCompletedSales(Transformation):

    def apply(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        atribs["completed_sales"] = atribs["seller"]["seller_reputation"]["metrics"]["sales"]["completed"]
        return atribs

@dataclass
class ShippingMethods(Transformation):

    def apply(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        tags = atribs["shipping"]["tags"]
        atribs["shipping"] = tags if len(tags) > 0 else [None]
        return atribs
