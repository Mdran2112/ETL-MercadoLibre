import requests
from requests import Response

from utils.decorators import _time_profiling
from utils.time_profilers import SearchApiTimeProfiler, ConverterApiTimeProfiler, AttributesItemApiTimeProfiler


class MLApiClient:
    """
    Client for requesting useful data to MeLi public API.
    Attributes
    ----------
    country_ml: str
        Country MeLi site alias. For example: MLB (Mercado Libre Brasil)
    """

    def __init__(self, country_ml: str) -> None:
        self.items_search_url = f"https://api.mercadolibre.com/sites/{country_ml}/search"
        self.item_attributes_url = "https://api.mercadolibre.com/items/"
        self.currencies_url = "https://api.mercadolibre.com/currencies"
        self.currency_convert_url = f"https://api.mercadolibre.com/currency_conversions/" \
                                    "search?from={}&to={}"

    @_time_profiling(ConverterApiTimeProfiler)
    def get_currency_conv_rate(self, from_currency_id: str = "BRL",
                               to_currenc_id: str = "USD") -> Response:
        return requests.get(self.currency_convert_url.format(from_currency_id, to_currenc_id))

    @_time_profiling(SearchApiTimeProfiler)
    def request_to_search_api(self, query: str, exclude_seller_id: int,
                              offset: int, limit: int) -> Response:
        return requests.get(
            self.items_search_url + f"?q={query}&offset={offset}&limit={limit}&seller_id!={exclude_seller_id}")

    @_time_profiling(AttributesItemApiTimeProfiler)
    def request_to_item_attributes_api(self, item_id: str) -> Response:
        return requests.get(self.item_attributes_url + item_id)
