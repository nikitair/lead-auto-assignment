from flask import Flask, render_template, request, jsonify
import pretty_errors
import pprint
from logging_config import logger as  logging
from a2wsgi import WSGIMiddleware
from main import main
from utils import get_realtor_by_round_robin


app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    """
    echo endpoint for server health check 
    """
    logging.info(f"{index.__name__} -- INDEX ENDPOINT TRIGGERED -- {request.method}")
    return render_template('index.html')


@app.route('/assign_lead', methods=['POST'])
def lead_auto_assignment():
    """
    lead auto assignment endpoint
    """
    # receiving lead payload
    payload = request.get_json()
    logging.info(f"\n\n{lead_auto_assignment.__name__} -- LEAD AUTO ASSIGNMENT TRIGGERED")
    logging.info(f"{lead_auto_assignment.__name__} -- RAW PAYLOAD -- {payload}")

    # extracting useful information from the payload
    postalcode = payload.get("listing_zip") if payload.get("listing_zip") != "N/A" else ""
    listing_province = payload.get("listing_province") if payload.get("listing_province") != "N/A" else ""
    listing_city = payload.get("listing_city") if payload.get("listing_city") != "N/A" else ""
    buyer_city = payload.get("buyer_city") if payload.get("buyer_city") != "N/A" else ""
    buyer_province = payload.get("buyer_province") if payload.get("buyer_province") != "N/A" else ""

    logging.info(f"{lead_auto_assignment.__name__} -- POSTALCODE AFTER N/A FORMATTING -- {postalcode}")
    logging.info(f"{lead_auto_assignment.__name__} -- LISTING AFTER N/A FORMATTING -- {listing_province}")
    logging.info(f"{lead_auto_assignment.__name__} -- LISTING AFTER N/A FORMATTING -- {listing_city}")
    logging.info(f"{lead_auto_assignment.__name__} -- BUYER AFTER N/A FORMATTING -- {buyer_province}")
    logging.info(f"{lead_auto_assignment.__name__} -- BUYER CITY AFTER N/A FORMATTING -- {buyer_city}")

    # executing lead auto assignment function; returning result
    result = main(postalcode, listing_province,
                  listing_city, buyer_city, buyer_province)
    
    logging.info(f"{lead_auto_assignment.__name__} -- RESPONSE -- {result}")
    return jsonify(result), 200



@app.route('/round_robin', methods=['POST'])
def round_robin():
    """
    Endpoint to choose a realtor to assign according to the for round-robin logic 
    """
    try:
        payload = request.get_json()
    except Exception as ex:
        logging.error(f"{round_robin.__name__} -- !!! ERROR OCCURRED -- {ex}")
        return jsonify({"status": "fail", "message": "No payload received"}), 415
    else:
        logging.info(f"{round_robin.__name__} -- RAW PAYLOAD -- {pprint.pformat(payload)}")
        realtors = payload.get("realtors")
        return jsonify({"assigned_realtor": get_realtor_by_round_robin(realtors)}), 200


# configuring wsgi
app = WSGIMiddleware(app)

if __name__ == '__main__':
    app.run(debug=False, port=5000, host='0.0.0.0')
