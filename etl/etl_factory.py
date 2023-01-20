from typing import Tuple

from sqlalchemy.orm import Session

from database.client import DatabaseClient
from meli.api_client import MLApiClient
from etl.cleaner.data_cleaner import DataCleaner
from etl.data_batch_generators.data_batch_generator import SellersBatchGenerator, ShippingBatchGenerator, \
    ProductsBatchGenerator
from etl.extractor import Extractor
from etl.filter.condition import NotNewProduct, ItemAlreadyStored, SellerAlreadyStored
from etl.filter.filter import Filter
from etl.loader import Loader
from etl.transformations.transformations import HandleNoWarrantyString, PriceConverter, InsertSellerID, ShippingMethods, \
    InsertSellerCompletedSales
from etl.transformer import Transformer


def etl_factory(session: Session) -> Tuple[Extractor, Transformer, Loader]:
    """
    Builds objects for making an ETL Pipeline.
    :param session: SQLAlchemy session.
    :return: A tuple of Extractor, Transformer and Loader.
    """

    meli_client = MLApiClient(country_ml="MLB")
    database_client = DatabaseClient(session=session)
    currency_factor = meli_client.get_currency_conv_rate(from_currency_id="BRL",
                                                         to_currenc_id="USD").json()["ratio"]

    current_day_item_ids = database_client.get_current_day_item_ids()
    seller_data = database_client.get_sellers_data()

    extractor = Extractor(meli_client,
                          _filter=Filter(
                              conditions=[
                                  ItemAlreadyStored(current_day_item_ids=current_day_item_ids),
                                  NotNewProduct()]
                          ))

    transformer = Transformer(
        transformations=[HandleNoWarrantyString(no_warranty_strings=["Sem garantia"]),
                         PriceConverter(currency_factor=currency_factor),
                         InsertSellerID(),
                         InsertSellerCompletedSales(),
                         ShippingMethods()],
        preprocessed_data_cleaner=DataCleaner(relevant_keys=(["id", "title", "sold_quantity", "shipping",
                                                              "price", "warranty", "seller_id", "completed_sales"])),
        sellers_generator=SellersBatchGenerator(),
        shipping_generator=ShippingBatchGenerator(),
        products_generator=ProductsBatchGenerator(drop_keys=["shipping"]),
        sellers_filter=Filter(conditions=[SellerAlreadyStored(seller_data=seller_data)])
    )

    loader = Loader(database_client=database_client)

    return extractor, transformer, loader
