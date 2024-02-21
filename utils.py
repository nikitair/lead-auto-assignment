import os
import pprint
from random import randint

import requests
from dotenv import load_dotenv

from db.mysql import m_queries as mysql
from db.postgres import p_queries as postgres
from logging_config import logger as logging

load_dotenv()

FUB_API_64 = os.getenv("FUB_API_64")


def prepare_postalcode(postalcode: str):
    """
    formats formats code into desired format -- A1A1A1 -> A1A 1A1 
    """
    logging.info(f"{prepare_postalcode.__name__} -- FORMATTING POSTAL CODE - {postalcode}")

    res = ''
    if type(postalcode) == str:
        if len(postalcode) > 0:
            if len(postalcode) == 6:
                res = f"{postalcode[0:3]} {postalcode[3:6]}"
            else:
                res = postalcode
    return res.upper()


def get_not_excluded_realtors(city: str, province: str, email_array: list) -> list:
    """
    returns realtors who are not in excluded cities table
    """
    logging.info(f"{get_not_excluded_realtors.__name__} -- EVALUATING NOT EXCLUDED REALTORS - {email_array}")

    excluded_emails = []

    for email in email_array:

        excluded_email = postgres.get_excluded_cities(city, province, email)

        if len(excluded_email) > 0:
            excluded_emails.append(excluded_email[0][3])

    logging.info(f"{get_not_excluded_realtors.__name__} -- EXCLUDED REALTORS -- {excluded_emails}")

    not_excluded_emails = [email for email in email_array if email not in excluded_emails]
    logging.info(f"{get_not_excluded_realtors.__name__} -- NOT EXCLUDED REALTORS -- {not_excluded_emails}")

    return not_excluded_emails


def get_nationality(name: str, search_nations):

    logging.info(f"{get_nationality.__name__} -- EVALUATING BUYER NATIONALITY - {name}")

    if not name or len(search_nations) < 1:
        return None

    response = requests.get(
        f"https://api.nationalize.io/?name={name}"
    )

    status = response.status_code
    data = response.json()


    if status == 200 and type(data.get("country")) == list and len(data["country"]) > 0:
        nationality_array = [country["country_id"] for country in data["country"]]

    logging.info(f"{get_nationality.__name__} -- NATIONALITY API RESPONSE - {data}")

    for nation in nationality_array:
        if nation in search_nations:
            return nation

    return None


def get_realtor_to_assign(realtors: list, buyer_name: str,
                          listing_mls: str, listing_categories: list):
    """
    return realtor to assign according to the round-robin logic
    """
    assigned_realtor = None

    detailed_info = {
        "win_type": "pond",
        "realtor_category": "",
        "realtor_nationality": "",
        "realtor_priority": 0
    }


    if type(realtors) == list and len(realtors) > 0:
        logging.info(f"{get_realtor_to_assign.__name__} -- EVALUATING REALTOR TO ASSIGN - {realtors}")

        # 1. Category evaluation
        logging.info(f"{get_realtor_to_assign.__name__} -- 1. CATEGORY EVALUATION")
        realtors_categories = mysql.get_realtors_category(realtors)
        evaluated_realtors_by_category = []

        for realtor in realtors_categories:
            if realtor["category"] in listing_categories:
                evaluated_realtors_by_category.append(realtor["email"])

        logging.info("{get_realtor_to_assign.__name__} -- REALTORS BY CATEGORY EVALUATIONS - "
                     f"{evaluated_realtors_by_category}")

        if len(evaluated_realtors_by_category) == 1:

            detailed_info["realtor_category"] = realtors_categories[0]["category"]
            detailed_info["win_type"] = "category"

            return evaluated_realtors_by_category[0], detailed_info
        
        if len(evaluated_realtors_by_category) > 1:
            realtors = evaluated_realtors_by_category

        # 2. Evaluating top priority realtors (exit point)
        logging.info(f"{get_realtor_to_assign.__name__} -- 2. TOP PRIORITY EVALUATION")
        realtors, priority_score = mysql.get_top_priority_realtors(realtors)

        logging.info(f"{get_realtor_to_assign.__name__} -- REALTORS BY TOP PRIORITY EVALUATIONS - {realtors}")

        if realtors and len(realtors) == 1:

            detailed_info["win_type"] = "priority"
            detailed_info["realtor_priority"] = priority_score

            return realtors[0], detailed_info
        
        # 3. Nationality evaluation (exit point)
        logging.info(f"{get_realtor_to_assign.__name__} -- 3. NATIONALITY EVALUATION")

        realtors_nationalities = mysql.get_realtors_nationality(realtors)
        logging.info(f"{get_realtor_to_assign.__name__} -- REALTORS NATIONALITIES - {realtors_nationalities}")

        buyer_nationality = get_nationality(buyer_name, [list(nation.values())[0] for nation in realtors_nationalities if list(nation.values())[0]])
        logging.info(f"{get_realtor_to_assign.__name__} -- BUYER NATIONALITY - {buyer_nationality}")

        national_realtors = [list(realtor.keys())[0] for realtor in realtors_nationalities if list(realtor.values())[0] == buyer_nationality]
        logging.info(f"{get_realtor_to_assign.__name__} -- REALTORS BY NATIONALITY EVALUATION - {national_realtors}")

        if len(national_realtors) == 1:
            detailed_info["realtor_nationality"] = list(realtors_nationalities[0].values())[0]
            detailed_info["win_type"] = "nationality"

            return national_realtors[0], detailed_info
        

        elif len(national_realtors) > 1:
            realtors = national_realtors

        # 4. Round Robin
        logging.info(f"{get_realtor_to_assign.__name__} -- 4. ROUND ROBIN EVALUATION")
        try:
            assigned_realtor = postgres.get_realtor_by_round_robin(realtors)
        except Exception as ex:
            logging.error(f"{get_realtor_to_assign.__name__} -- !!! ERROR OCCURRED -- {ex}")

        if assigned_realtor:
            assigned_realtor = assigned_realtor[-1][0]
            detailed_info["win_type"] = "round-robin"
        else:
            assigned_realtor = realtors[randint(0, len(realtors) - 1)]

    logging.info(f"{get_realtor_to_assign.__name__} -- RESULT ASSIGNED REALTOR -- {assigned_realtor}")
    return assigned_realtor, detailed_info


def get_pond_id(lead_province: str):
    logging.info(f"{get_pond_id.__name__} -- EVALUATING POND ID -- {lead_province}")

    pond_id = 3

    if lead_province:

        url = "https://api.followupboss.com/v1/ponds?offset=0&limit=100"

        headers = {
            "accept": "application/json",
            "authorization": f"Basic {FUB_API_64}"
        }

        response = requests.get(url, headers=headers)

        logging.info(f"{get_pond_id.__name__} -- FUB RESPONSE STATUS -- {response.status_code}")

        try:
            data = response.json()
            logging.info(f"{get_pond_id.__name__} -- FUB RESPONSE DATA -- {data}")

        except Exception as ex:
            logging.error(f"{get_pond_id.__name__} -- !!! ERROR OCCURRED - {ex}")
            return pond_id

        if type(data) == dict and data.get("ponds"):
            for pond in data.get("ponds"):
                if f"{lead_province.title()} (Out of Polygon)" == pond["name"]:  
                    pond_id = pond["id"]
                    break
        logging.info(f"{get_pond_id.__name__} -- POND ID -- {pond_id}")

    return pond_id


def payload_validator(postalcode, listing_province,
                      listing_city, buyer_name,
                      buyer_city, buyer_province, buyer_email):
    logging.info(f"{payload_validator.__name__} -- VALIDATING PAYLOAD")
    valid = True

    if not postalcode and not listing_city and not listing_province and not buyer_city and not buyer_province:
        valid = False

    if not buyer_email and not buyer_name:
        valid = False

    return valid


def format_listing_categories(listing_categories: str) -> list:
    res = listing_categories.split(",") if listing_categories else []
    logging.info(f"{format_listing_categories.__name__} -- FORMATTED LISTING CATEGORIES - {res}")
    return res


if __name__ == "__main__":
    # get_pond_id("Manitoba")
    # pprint.pprint(get_nationality(None))
    # print(get_nationality("Nikita"))
    format_listing_categories()
