from flask import Flask, request, jsonify
import jwt
import logging
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# Placeholder for JWT_SECRET, should be securely managed
JWT_SECRET = "CIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNMCIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNM"

def decode_jwt(token):
    """
    Decodes the JWT token.
    """
    try:
        decoded_token = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return decoded_token
    except jwt.ExpiredSignatureError:
        logging.error("Token has expired")
        raise Exception("Token has expired")
    except jwt.InvalidTokenError:
        logging.error("Invalid token")
        raise Exception("Invalid token")

def load_data_for_business(business_id):
    """
    Simulated data load function.
    Replace with actual data processing logic.
    """
    logging.info(f"Attempting to load data for business ID: {business_id}")
    # Simulate data retrieval
    if business_id == "Faz-13-11-2024-08-11739": # Example valid business ID
        return f"Data loaded for business ID: {business_id}"
    else:
        logging.warning(f"No data found for business ID: {business_id}")
        return None

def process_data_for_business(business_id):
    """
    Processes data for the given business ID.
    Replace with actual logic to load and process data.
    """
    logging.info(f"Processing data for business ID: {business_id}")
    data = load_data_for_business(business_id)
    if data:
        # Further processing can happen here
        return data
    else:
        raise Exception(f"Failed to load or process data for business ID: {business_id}")

@app.route('/process', methods=['POST'])
def process_data_endpoint():
    """
    Endpoint to process data based on JWT token in Authorization header.
    """
    auth_header = request.headers.get('Authorization')

    if not auth_header:
        logging.warning("Authorization header is missing")
        return jsonify({"error": "Authorization header is missing"}), 401

    parts = auth_header.split()

    if parts[0].lower() != 'bearer':
        logging.warning("Authorization header must start with Bearer")
        return jsonify({"error": "Authorization header must start with Bearer"}), 401
    elif len(parts) == 1:
        logging.warning("Token not found after Bearer")
        return jsonify({"error": "Token not found"}), 401
    elif len(parts) > 2:
        logging.warning("Authorization header must be Bearer token")
        return jsonify({"error": "Authorization header must be Bearer token"}), 401

    token = parts[1]

    try:
        decoded_token = decode_jwt(token)
        # The problem description mentions 'businessId' but the example token payload uses 'companyId'.
        # I will try 'businessId' first, then 'companyId' for broader compatibility.
        business_id = decoded_token.get('businessId')
        if not business_id:
            business_id = decoded_token.get('companyId') # Fallback to companyId

        if not business_id:
            logging.error("Business ID (businessId or companyId) not found in token")
            return jsonify({"error": "Business ID (businessId or companyId) not found in token"}), 400

        # Process the data based on the extracted business ID
        data = process_data_for_business(business_id)
        return jsonify({"message": "Data processed successfully", "data": data}), 200

    except Exception as e:
        # Specific errors from decode_jwt are already logged.
        # Log other potential errors here.
        if "Token has expired" not in str(e) and "Invalid token" not in str(e):
             logging.error(f"Error processing request: {str(e)}")
        return jsonify({"error": str(e)}), 401

if __name__ == '__main__':
    app.run(debug=True)
