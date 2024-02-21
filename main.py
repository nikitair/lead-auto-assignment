import json
from random import randint

from db.mysql import m_queries as mysql
from db.postgres import p_queries as postgres
from logging_config import logger as logging
from utils import (format_listing_categories, get_not_excluded_realtors,
                   get_pond_id, get_realtor_to_assign, prepare_postalcode)


def main(postalcode: str, listing_province: str,
         listing_city: str, buyer_city: str,
         buyer_province: str, buyer_email: str,
         buyer_name: str, cold_lead: int = 0,
         listing_mls=None, listing_categories=None) -> dict:
    """
    the main function that executes full lead auto assignment cycle
    """
    logging.info(f"{main.__name__} -- STARTING LEAD AUTO ASSIGNMENT")

    response = {
        "assigned_realtor": "willow@fb4s.com",
        "possible_realtors": [
        ],
        "realtor_type_1": 1,
        "assigned_pond_id": 31,
        "detailed_info": {
            "win_type": "pond",
            "realtor_category": "",
            "buyer_nationality": "",
            "realtor_priority": 0
        }
    }

    if cold_lead:
        province = buyer_province
        city = buyer_city
        postalcode = None
    else:
        province = listing_province if listing_province not in (None, "") else buyer_province
        city = listing_city if listing_city not in (None, "") else buyer_city

        # formatting postal code to the desired format -- A1A1A1 -> A1A 1A1
        postalcode = prepare_postalcode(postalcode)

    if not buyer_name:
        buyer_name = mysql.get_buyer_name(buyer_email=buyer_email)

    listing_categories = format_listing_categories(listing_categories)

    logging.info(f"{main.__name__} -- BUYER NAME -- {buyer_name}")
    logging.info(f"{main.__name__} -- LISTING CATEGORIES -- {listing_categories}")
    logging.info(f"{main.__name__} -- SEARCH PROVINCE -- {province}")
    logging.info(f"{main.__name__} -- SEARCH CITY -- {city}")
    logging.info(f"{main.__name__} -- SEARCH POSTAL CODE -- {postalcode}")

    # searching in additional cities
    additional_cities = postgres.get_additional_cities(city=city, province=province)
    logging.info(f"{main.__name__} -- FOUND ADDITIONAL CITIES -- {additional_cities}")

    # if found in additional cities -> returning those realtor
    if len(additional_cities) > 0:
        response["realtor_type_1"] = 0
        for city in additional_cities:
            response["possible_realtors"].append(city[3])

        # evaluation assigned realtor by the Round-Robin logic
        assigned_realtor, detailed_info = get_realtor_to_assign(response["possible_realtors"],
                                                                    buyer_name, listing_mls, listing_categories)
        response["assigned_realtor"] = assigned_realtor
        response["detailed_info"] = detailed_info

    # nobody found in additional cities
    else:
        # searching for realtors in overlapping polygons
        realtors_in_polygon = [realtor[2] for realtor in mysql.get_realtors_in_polygon(city, province, postalcode)]
        logging.info(f"{main.__name__} -- FOUND REALTORS IN POLYGON -- {realtors_in_polygon}")

        if len(realtors_in_polygon) > 0:
            # searching for realtors who's city is NOT excluded
            not_excluded_realtors = get_not_excluded_realtors(city, province, realtors_in_polygon)

            # realtors found in overlapping polygon; returning them
            if len(not_excluded_realtors) > 0:

                response["realtor_type_1"] = 0
                for realtor in not_excluded_realtors:
                    response["possible_realtors"].append(realtor)

                # evaluation assigned realtor by the Round-Robin logic
                assigned_realtor, detailed_info = get_realtor_to_assign(response["possible_realtors"],
                                                                    buyer_name, listing_mls, listing_categories)
                response["assigned_realtor"] = assigned_realtor
                response["detailed_info"] = detailed_info

    # evaluating initial Pond of the lead
    if response["assigned_realtor"] == "willow@fb4s.com": 
        response["assigned_pond_id"] = get_pond_id(province)

    return response


if __name__ == "__main__":
    ...
