from random import randint
from logging_config import logger as logging
import pretty_errors
from db.postgres import p_queries as postgres


def prepare_postalcode(postalcode: str):
    """
    formats formats code into desired format -- A1A1A1 -> A1A 1A1 
    """
    res = ''
    if type(postalcode) == str:
        if len(postalcode) > 0:
            if len(postalcode) == 6:
                res = f"{postalcode[0:3]} {postalcode[3:6]}"
            else:
                res = postalcode
    return res


def get_not_excluded_realtors(city: str, province: str, email_array: list) -> list:
    """
    returns realtors who are not in excluded cities table
    """
    excluded_emails = []
    for email in email_array:
        excluded_email = postgres.get_excluded_cities_by_city_province_emails(
            city, province, email)
        if len(excluded_email) > 0:
            excluded_emails.append(excluded_email[0][3])
    logging.info(f"EXCLUDED REALTORS -- {excluded_emails}")
    not_excluded_emails = [
        email for email in email_array if email not in excluded_emails]
    return not_excluded_emails


def get_realtor_by_round_robin(realtors: list) -> dict:
    """
    return realtor to assign according to the round-robin logic
    """
    assigned_realtor = None
    if type(realtors) == list and len(realtors) > 0:
        try:
            assigned_realtor = postgres.get_realtor_to_assign(realtors)
            logging.info(f"{get_realtor_by_round_robin.__name__} -- ROUND-ROBIN ASSIGNED REALTOR -- {assigned_realtor}")
        except Exception as ex:
            logging.error(f"{get_realtor_by_round_robin.__name__} -- !!! ERROR OCCURRED -- {ex}")

        if assigned_realtor:
            assigned_realtor = assigned_realtor[-1][0]
        else:
            assigned_realtor = realtors[randint(
                0, len(realtors) - 1)]
            
    logging.info(f"{get_realtor_by_round_robin.__name__} -- RESULT ASSIGNED REALTOR -- {assigned_realtor}")
    return assigned_realtor


if __name__ == "__main__":
    ...
