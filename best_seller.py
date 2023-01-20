from database import engine, Base
import logging
import sys
from utils.useful import str2bool

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

from etl.etl_factory import etl_factory
import time
from sqlalchemy.orm import sessionmaker, Session
import argparse


def create_database(drop_existing: bool) -> None:
    if drop_existing:
        logging.warning("All data in the database will be deleted.")
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine, checkfirst=True)


def _main(query: str, max_items: int,
          exclude_seller_id: int, _session: Session) -> None:

    extractor, transformer, loader = etl_factory(session=_session)

    t0 = time.time()
    # Extract
    logging.info(
        f"Getting raw data for {query}, excluding seller_id {exclude_seller_id} and max_items {max_items}... \n"
        f"--------------------------------------------------------------------------------------------------")
    item_list = extractor.search(query=query, max_items=max_items, exclude_seller_id=exclude_seller_id)

    # Transform
    logging.info("Applying transformations and preparing data to be stored in Database... \n"
                 "-----------------------------------------------------------------------")
    item_list_transformed, item_shippings, sellers = \
        transformer.transform(item_list)

    # Load
    logging.info("Loading new data into Database... \n"
                 "------------------------------------")
    loader.load(item_list_transformed, sellers, item_shippings)

    logging.info(f"Finished! Time: {round(time.time() - t0, 2)} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="request products filtered by name. For example: 'Iphone 11'", default="Iphone 11")
    parser.add_argument("--max_items", help="maximum amount of products to be requested.", default=200)
    parser.add_argument("--exclude_seller_id", help="This is the client seller id. "
                                                    "Products published by the client will be omitted.", default=82916233)
    parser.add_argument("--new_db", help="If true, all current data stored in the local database will be deleted.",
                        default="false", type=str2bool)

    args = parser.parse_args()

    session = sessionmaker(engine)()

    create_database(drop_existing=args.new_db)

    _main(query=args.query, max_items=args.max_items,
          exclude_seller_id=args.exclude_seller_id, _session=session)
