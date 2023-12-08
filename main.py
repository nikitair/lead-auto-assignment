import json
import logging
from utils import prepare_postalcode
from db.postgres import p_queries as postgres
from db.mysql import m_queries as mysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main(mls: str, postalcode: str, listing_province: str, buyer_city: str, buyer_province: str, email: str):
    province = listing_province if listing_province not in (
        None, "") else buyer_province
    postalcode = prepare_postalcode(postalcode)

    # search in additional cities
    additional_cities = postgres.get_all_additional_cities()

    logging.info(f"ADDITIOANL CITIES -- {additional_cities}")


if __name__ == "__main__":
    with open("demo_payloads.json", "r") as f:
        demo_payloads = json.load(f)
        print(demo_payloads)

        payload = demo_payloads["realtor_not_found"]
        mls = payload["listing_mls"]
        postalcode = payload["listing_zip"]
        listing_province = payload["listing_province"]
        buyer_city = payload["buyer_city"]
        buyer_province = payload["buyer_province"]
        email = payload["buyer_email"]

    main(mls, postalcode, listing_province, buyer_city, buyer_province, email)
