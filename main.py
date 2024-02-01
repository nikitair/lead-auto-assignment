import json
from random import randint
import pretty_errors
from logging_config import logger as  logging
from utils import prepare_postalcode, get_not_excluded_realtors, get_realtor_by_round_robin, get_pond_id
from db.postgres import p_queries as postgres
from db.mysql import m_queries as mysql


def main(postalcode: str, listing_province: str, listing_city: str, buyer_city: str, buyer_province: str, buyer_email: str) -> dict:
    """
    the main function that executes full lead auto assignment cycle
    """
    logging.info(f"{main.__name__} -- STARTING LEAD AUTO ASSIGNMENT")

    response = {
        "realtor_1": 0,
        "realtor_emails": [],
        "assigned_realtor": "willow@fb4s.com",
        "assigned_pond_id": 31
    }

    province = listing_province if listing_province not in (None, "") else buyer_province
    city = listing_city if listing_city not in (None, "") else buyer_city
    # formatting postal code to the desired format -- A1A1A1 -> A1A 1A1
    postalcode = prepare_postalcode(postalcode)

    buyer_name = mysql.get_buyer_name(buyer_email=buyer_email)
    logging.info(f"{main.__name__} -- BUYER NAME -- {buyer_name}")

    logging.info(f"{main.__name__} -- SEARCH PROVINCE -- {province}")
    logging.info(f"{main.__name__} -- SEARCH CITY -- {city}")
    logging.info(f"{main.__name__} -- SEARCH POSTAL CODE -- {postalcode}")

    # searching in additional cities
    additional_cities = postgres.get_additional_cities_by_city_province(city=buyer_city, province=province)
    logging.info(f"{main.__name__} -- FOUND ADDITIONAL CITIES -- {additional_cities}")

    # if found in additional cities -> returning those realtor
    if len(additional_cities) > 0:
        response["realtor_1"] = 1
        for city in additional_cities:
            response["realtor_emails"].append(city[3])

        # evaluation assigned realtor by the Round-Robin logic
        response["assigned_realtor"] = get_realtor_by_round_robin(response["realtor_emails"], buyer_name)
    
    # nobody found in additional cities
    else:
        # searching for realtors in overlapping polygons
        realtors_in_polygon = [realtor[2] for realtor in mysql.get_realtors_in_polygon(city, province, postalcode)]
        logging.info(f"{main.__name__} -- FOUND REALTORS IN POLYGON -- {realtors_in_polygon}")

        if len(realtors_in_polygon) > 0:
            # searching for realtors who's city is NOT excluded
            not_excluded_realtors = get_not_excluded_realtors(buyer_city, province, realtors_in_polygon)
            logging.info(f"{main.__name__} -- NOT EXCLUDED REALTORS -- {not_excluded_realtors}")

            # realtors found in overlapping polygon; returning them
            if len(not_excluded_realtors) > 0:

                response["realtor_1"] = 1
                for realtor in not_excluded_realtors:
                    response["realtor_emails"].append(realtor)

                # evaluation assigned realtor by the Round-Robin logic
                response["assigned_realtor"] = get_realtor_by_round_robin(response["realtor_emails"], buyer_name)

    # evaluating initial Pond of the lead
    if response["assigned_realtor"] == "willow@fb4s.com": 
        response["assigned_pond_id"] = get_pond_id(listing_province)

    return response


if __name__ == "__main__":
    main("N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
