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
        "realtor_1": 0,
        "realtor_emails": [],
        "assigned_realtor": "willow@fb4s.com",
        "assigned_pond_id": 31,
        "additional_data": {},
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
        response["realtor_1"] = 1
        for city in additional_cities:
            response["realtor_emails"].append(city[3])

        response["additional_data"]["additional_city"] = additional_cities

        # evaluation assigned realtor by the Round-Robin logic
        response["assigned_realtor"] = get_realtor_to_assign(response["realtor_emails"],
                                                             buyer_name, listing_mls, listing_categories)

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

                response["realtor_1"] = 1
                for realtor in not_excluded_realtors:
                    response["realtor_emails"].append(realtor)

                # evaluation assigned realtor by the Round-Robin logic
                response["assigned_realtor"] = get_realtor_to_assign(response["realtor_emails"],
                                                                     buyer_name, listing_mls, listing_categories)

    # evaluating initial Pond of the lead
    if response["assigned_realtor"] == "willow@fb4s.com": 
        response["assigned_pond_id"] = get_pond_id(province)

    return response


if __name__ == "__main__":
    print(main("", "", "", "", "", "", ""))
