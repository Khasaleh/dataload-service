from flask import Flask, request, jsonify
import logging

# Import settings if needed for other parts, though not for JWT secret/algo directly here
# from app.core.config import settings

# Import the shared JWT parsing utility and custom exceptions
from app.dependencies.auth import _parse_jwt_token_data, TokenError, ExpiredTokenError, InvalidTokenError, MissingClaimsError

# Configure logging (once)
logging.basicConfig(level=logging.INFO)

# Initialize Flask app (once)
app = Flask(__name__)

# SECRET_KEY and ALGORITHM are now used by the shared _parse_jwt_token_data utility,
# which sources them from settings. So, no need to define them locally in this file.
# The old local decode_jwt_and_extract_claims function is also removed.

def load_data_for_business(business_id: int):
    """
    Simulated data load function.
    Now expects an integer business_id.
    Replace with actual data processing logic.
    """
    logging.info(f"Attempting to load data for integer business ID: {business_id}")
    # Simulate data retrieval using an integer ID.
    # For the example token with companyId "Faz-13-11-2024-08-11739", the extracted business_id is 11.
    if business_id == 11: # Example valid integer business ID
        return f"Data loaded for integer business ID: {business_id}"
    else:
        logging.warning(f"No data found for integer business ID: {business_id}")
        return None

def process_data_for_business(business_id: int): # Added type hint
    """
    Processes data for the given business ID.
    Replace with actual logic to load and process data.
    """
    logging.info(f"Processing data for business ID: {business_id}") # Kept original log message
    data = load_data_for_business(business_id)
    if data:
        # Further processing can happen here
        return data
    else:
        # Consider if this should raise a more specific custom exception
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
        # Use the shared utility from app.dependencies.auth
        extracted_claims = _parse_jwt_token_data(token)

        business_id_for_processing = extracted_claims.get('business_id')

        # _parse_jwt_token_data would raise MissingClaimsError if business_id couldn't be derived.
        # So, business_id_for_processing should be present here if no exception was raised.

        data = process_data_for_business(business_id_for_processing)

        response_data = {
            "message": "Data processed successfully",
            "processed_for_username": extracted_claims.get('username'),
            "processed_for_user_id": extracted_claims.get('user_id'),
            "processed_for_business_id": business_id_for_processing,
            "original_company_id_str": extracted_claims.get('company_id_str'), # From shared utility
            "data": data
        }
        return jsonify(response_data), 200

    except ExpiredTokenError as e:
        # Logged by _parse_jwt_token_data
        return jsonify({"error": str(e)}), 401
    except MissingClaimsError as e:
        # Logged by _parse_jwt_token_data
        return jsonify({"error": str(e)}), 400 # 400 for bad request due to missing/invalid claims
    except InvalidTokenError as e:
        # Logged by _parse_jwt_token_data
        return jsonify({"error": str(e)}), 401
    except TokenError as e: # Catch-all for other custom token errors from the utility
        # Logged by _parse_jwt_token_data
        return jsonify({"error": str(e)}), 401
    except Exception as e: # Catch other exceptions, like from process_data_for_business
        logging.error(f"Unexpected error in /process endpoint: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred"}), 500

if __name__ == '__main__':
    # Need to ensure app.core.config.settings are loaded if this is run directly
    # and settings are used by _parse_jwt_token_data.
    # For testing via unittest, the test setup handles app context.
    # If running this directly: `python jwt_processor_app.py`, ensure .env is accessible
    # or settings are configured appropriately.
    app.run(debug=True)
