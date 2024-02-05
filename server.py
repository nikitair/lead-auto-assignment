from flask import Flask, render_template, request, jsonify
import pretty_errors
from logging_config import logger as  logging
from a2wsgi import WSGIMiddleware
from main import main
from utils import get_realtor_to_assign, payload_validator
import os
from dotenv import load_dotenv
import pprint

load_dotenv()
SSH_MODE = os.getenv("SSH_MODE")

app = Flask(__name__)


hot_lead_payload = {
    "listing_mls": "123456789",
    "listing_zip": "A1A 1A1",
    "listing_city": "Toronto",
    "listing_province": "Ontario",
    "buyer_name": "John",
    "buyer_email": "john@mail.com"
}

cold_lead_payload = {
    "buyer_city": "Toronto",
    "buyer_province": "Ontario",
    "buyer_name": "John",
    "buyer_email": "john@mail.com"
}


@app.route('/', methods=['GET'])
def index():
    """
    echo endpoint for server health check 
    """
    logging.info(f"{index.__name__} -- INDEX ENDPOINT TRIGGERED -- {request.method}")
    return render_template('index.html')
    

@app.errorhandler(404)
def not_found(e):
  return render_template('404.html'), 404


@app.errorhandler(400)
def bad_request(e):
  return jsonify(
      {
            "error": "Bad request",
            "success": False  
        }
  ), 400


@app.errorhandler(405)
def bad_method(e):
  return jsonify(
      {
            "error": "Method is NOT allowed",
            "success": False,
            "message": str(e)
        }
  ), 405


@app.errorhandler(500)
def not_found(e):
  return jsonify(
      {
            "error": "Unexpected Server error occurred :(",
            "success": False
        }
  ), 500


@app.route('/assign_lead', methods=['POST'])
def lead_auto_assignment():
    """
    lead auto assignment endpoint
    """    
    logging.info(f"{lead_auto_assignment.__name__} -- LEAD AUTO ASSIGNMENT ENDPOINT TRIGGERED")

    try:
        # receiving lead payload
        payload = request.get_json()
        logging.info(f"{lead_auto_assignment.__name__} -- RAW PAYLOAD -- {payload}")

        postalcode = payload.get("listing_zip")
        listing_province = payload.get("listing_province")
        listing_city = payload.get("listing_city")

        buyer_name = payload.get("buyer_name")
        buyer_city = payload.get("buyer_city")
        buyer_province = payload.get("buyer_province")
        buyer_email = payload.get("buyer_email")
        cold_lead = payload.get("cold_lead", 0)

        if cold_lead or (not postalcode and not listing_city and not listing_province):
            logging.info(f"{lead_auto_assignment.__name__} -- COLD LEAD")

         # N/A formatting
        postalcode = postalcode if postalcode != "N/A" else ""
        listing_province = listing_province if listing_province != "N/A" else ""
        listing_city = listing_city if listing_city != "N/A" else ""
        buyer_city = buyer_city if buyer_city != "N/A" else ""
        buyer_province = buyer_province if buyer_province != "N/A" else ""
        buyer_email = buyer_email if buyer_email != "N/A" else ""
        buyer_name = buyer_name if buyer_name != "N/A" else ""

        logging.info(f"{lead_auto_assignment.__name__} -- POSTALCODE AFTER N/A FORMATTING -- {postalcode}")
        logging.info(f"{lead_auto_assignment.__name__} -- LISTING AFTER N/A FORMATTING -- {listing_province}")
        logging.info(f"{lead_auto_assignment.__name__} -- LISTING AFTER N/A FORMATTING -- {listing_city}")
        logging.info(f"{lead_auto_assignment.__name__} -- BUYER AFTER N/A FORMATTING -- {buyer_province}")
        logging.info(f"{lead_auto_assignment.__name__} -- BUYER CITY AFTER N/A FORMATTING -- {buyer_city}")
        logging.info(f"{lead_auto_assignment.__name__} -- BUYER EMAIL AFTER N/A FORMATTING -- {buyer_email}")
        logging.info(f"{lead_auto_assignment.__name__} -- BUYER NAME AFTER N/A FORMATTING -- {buyer_name}")

        if not payload_validator(postalcode, listing_province, listing_city, buyer_name, buyer_city, buyer_province, buyer_email): 
            raise ValueError("Invalid Payload")

    except Exception as ex:
        logging.error(f"{lead_auto_assignment.__name__} -- !!! ERROR OCCURRED - {ex}")
        return jsonify(
            {
                "status": "fail", 
                "error": "Invalid Payload received",
                "hot_lead_preferred_payload": hot_lead_payload,
                "cold_lead_preferred_payload": cold_lead_payload,
                "cold_lead_required_fields": ['buyer_email or buyer_name', 'buyer_province'],
                "hot_lead_required_fields": ['buyer_email or buyer_name', 'listing_province_province']
            }
            ), 422
   
    # executing lead auto assignment main function
    result = main(postalcode, listing_province, listing_city, buyer_city, buyer_province, buyer_email, buyer_name, int(cold_lead))
    
    logging.info(f"{lead_auto_assignment.__name__} -- RESPONSE -- {result}\n\n")
    return jsonify(result), 200



@app.route('/round_robin', methods=['POST'])
def round_robin():
    """
    Endpoint to choose a realtor to assign according to the for round-robin logic 
    """
    try:
        payload = request.get_json()
        realtors = payload["realtors"]
        buyer_name = payload["buyer_name"]
    except Exception as ex:
        logging.error(f"{round_robin.__name__} -- !!! UNEXPECTED ERROR - {ex}")
        return jsonify(
            {
                "status": "fail", 
                "error": "Invalid Payload received",
                "message": "Correct Payload format -> {'realtors': ['realtor1@mail.com', 'realtor1@mail.com'], 'buyer_name': 'John'}"
            }
            ), 422

   
    logging.info(f"{round_robin.__name__} -- RAW PAYLOAD -- {payload}")
    return jsonify({"assigned_realtor": get_realtor_to_assign(realtors, buyer_name)}), 200

app = WSGIMiddleware(app)

if __name__ == '__main__':
    app.run(debug=False, port=5000, host='0.0.0.0')
