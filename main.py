import json
from random import randint
import pretty_errors
import logging
from utils import prepare_postalcode, get_not_excluded_realtors
from db.postgres import p_queries as postgres
from db.mysql import m_queries as mysql

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Terminal output
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

# File output
fh = logging.FileHandler('logs.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


def main(postalcode: str, listing_province: str, listing_city: str, buyer_city: str, buyer_province: str) -> dict:
    """
    the main function that executes full lead auto assignment cycle
    """
    response = {
        "realtor_1": 0,
        "realtor_emails": [],
        "assigned_realtor": "willow@fb4s.com"
    }

    province = listing_province if listing_province not in (
        None, "") else buyer_province
    city = listing_city if listing_city not in (
        None, "") else buyer_city
    
    # formatting postal code to the desired format -- A1A1A1 -> A1A 1A1
    postalcode = prepare_postalcode(postalcode)

    # searching in additional cities
    additional_cities = postgres.get_additional_cities_by_city_province(
        city=buyer_city, province=province)
    logging.info(f"ADDITIONAL CITIES -- {additional_cities}")

    # found in additional cities; returning those realtor
    if len(additional_cities) > 0:
        response["realtor_1"] = 1
        for city in additional_cities:
            response["realtor_emails"].append(city[3])
        response["assigned_realtor"] = response["realtor_emails"][randint(0, len(response["realtor_emails"] - 1))]
    else:
        # searching for realtors in overlapping polygons
        realtors_in_polygon = [
            realtor[2] for realtor in mysql.get_realtors_in_polygon(city, province, postalcode)]
        logging.info(f"ALL REALTORS IN POLYGON -- {realtors_in_polygon}")

        if len(realtors_in_polygon) > 0:
            # searching for realtors who's city is NOT excluded
            not_excluded_realtors = get_not_excluded_realtors(
                buyer_city, province, realtors_in_polygon)
            logging.info(f"NOT EXCLUDED REALTORS -- {not_excluded_realtors}")

            # realtors found in overlapping polygon; returning them
            if len(not_excluded_realtors) > 0:
                response["realtor_1"] = 1
                for realtor in not_excluded_realtors:
                    response["realtor_emails"].append(realtor)
                response["assigned_realtor"] = response["realtor_emails"][randint(0, len(response["realtor_emails"] - 1))]

    logging.info(f"RESULT RESPONSE -- {response}")
    return response


if __name__ == "__main__":
    with open("demo_payloads.json", "r") as f:
        demo_payloads = json.load(f)
        payload = demo_payloads["polygon_no_excluded"]
        # mls = payload["listing_mls"]
        postalcode = payload["listing_zip"]
        listing_province = payload["listing_province"]
        buyer_city = payload["buyer_city"]
        buyer_province = payload["buyer_province"]
        # email = payload["buyer_email"]

    main(postalcode, listing_province, buyer_city, buyer_province)
