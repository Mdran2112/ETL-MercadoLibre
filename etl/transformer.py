import logging
from typing import List, Dict, Any, Tuple, Optional

from etl.cleaner.data_cleaner import DataCleaner
from etl.data_batch_generators.data_batch_generator import DataBatchGenerator, SellersBatchGenerator, \
    ProductsBatchGenerator, ShippingBatchGenerator
from etl.filter.filter import Filter
from etl.transformations.transformations import Transformation
from utils.decorators import _time_profiling
from utils.time_profilers import TransformTimeProfiler
from utils.useful import flat_map, drop_repeated_dicts


################################################################


class Transformer:
    """
    Used for transformation and cleaning of item objects obtained with Extractor. Returns products, sellers and products
    shipping lists ready to be stored in database.
    Attributes
    ----------
    transformations: List[Transformation]
        list of transformations and processors that will be applied to each item object.
    preprocessed_data_cleaner: DataCleaner
        used for cleaning items objects, after transformations, in order to retain only the useful attributes for
        generating the sellers and item_shipping object lists.
    sellers_generator: SellersBatchGenerator
        generator of sellers from preprocessed data
    shipping_generator: ShippingBatchGenerator
        generator of item_shipping objects list from preprocessed data
    products_generator: ProductsBatchGenerator
        generator of products objects list from preprocessed data
    sellers_filter: Optional[Filter]
        To every seller object generated, a filter would be applied according to a list of Condition (for example,
        exclude sellers that were already stored in database.)
    """

    def __init__(self,
                 transformations: List[Transformation],
                 preprocessed_data_cleaner: DataCleaner,
                 sellers_generator: SellersBatchGenerator,
                 shipping_generator: ShippingBatchGenerator,
                 products_generator: ProductsBatchGenerator,
                 sellers_filter: Optional[Filter]):
        self.transformations = transformations
        self.preprocessed_data_cleaner = preprocessed_data_cleaner
        self.sellers_generator = sellers_generator
        self.products_generator = products_generator
        self.shipping_generator = shipping_generator
        self.sellers_filter = sellers_filter

    def _apply_transformations(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        for tr in self.transformations:
            atribs = tr.apply(atribs)
        return atribs

    def _clean_raw(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        return self.preprocessed_data_cleaner.clean(atribs)

    def _opt_filter_sellers(self, sellers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self.sellers_filter:
            len_sellers = len(sellers)
            sellers = self.sellers_filter.apply_to_all(sellers)
            after_filter_len = len(sellers)
            if len_sellers - after_filter_len > 0:
                logging.warning(f"{len_sellers - after_filter_len} seller registries were filtered.")
        return sellers

    @_time_profiling(TransformTimeProfiler)
    def transform(self, items_list: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]],
                                                                   List[Dict[str, Any]],
                                                                   List[Dict[str, Any]]]:

        for atribs in items_list:
            self._apply_transformations(atribs)
            self._clean_raw(atribs)

        sellers = self.sellers_generator.build(items_list)
        sellers = self._opt_filter_sellers(sellers)
        item_shipping = self.shipping_generator.build(items_list)
        products_list = self.products_generator.build(items_list)

        del items_list

        return products_list, item_shipping, sellers
