import jwt
import os
import datetime
import argparse # Added for command-line arguments

# --- Configuration Switch ---
# Set this to True to enforce JWT signature and expiration verification (secure mode).
# Set this to False to bypass signature and expiration verification (insecure mode - FOR DEBUGGING/LIMITED USE ONLY).
# In a real application, this would likely come from an environment variable or a config file.
# Example for environment variable:
# VERIFY_JWT_SIGNATURE = os.getenv('VERIFY_JWT_SIGNATURE', 'True').lower() == 'true'
VERIFY_JWT_SIGNATURE = True # Default to secure

# The decoded secret key (replace with your actual secret if different)
# This is the same secret we've been using from the K8s example.
DECODED_JWT_SECRET = "CIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNMCIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNM"

# DEFAULT_TOKEN is removed, token will come from command line argument.

def extract_business_id_from_company_id(company_id_str: str, user_id_from_token) -> str | None:
    """
    Extracts the businessId from the companyId string based on the formula:
    FAZ + userId + businessId + year(4) + month(2) + randomNumber

    Args:
        company_id_str: The companyId string from the token.
        user_id_from_token: The userId from the token (used for verification).

    Returns:
        The extracted businessId string, or None if parsing fails or format is incorrect.

    Note: This parsing is based on an assumption due to ambiguity in separating
    businessId from randomNumber. It assumes year and month directly follow businessId.
    A more robust solution requires a delimiter or fixed lengths for businessId/randomNumber.
    """
    if not company_id_str:
        print("Debug: company_id_str is empty or None.")
        return None
    if user_id_from_token is None: # userId can be 0, so check for None explicitly
        print("Debug: user_id_from_token is None.")
        return None

    user_id_str = str(user_id_from_token)
    print(f"Debug: Attempting to parse companyId: '{company_id_str}' with userId: '{user_id_str}'")

    # Revised approach: Use split('-') as primary logic based on observed token format.
    parts = company_id_str.split('-')
    print(f"Debug: company_id_str split by '-': {parts}")

    # Expected structure: ["Faz", <userId_str>, <businessId_str>, <year_str>, <month_str>, <randomNumber_str>]

    if len(parts) < 6: # Faz, uid, bid, y, m, rnum
        print(f"Debug: companyId '{company_id_str}' does not have enough parts when split by '-' (expected at least 6). Found {len(parts)} parts.")
        return None

    # Validate prefix (case-insensitive)
    if parts[0].lower() != "faz":
        print(f"Debug: companyId prefix is not 'Faz'. Found '{parts[0]}'.")
        return None

    # Validate userId
    if parts[1] != user_id_str:
        print(f"Debug: userId in companyId ('{parts[1]}') does not match userId from token ('{user_id_str}').")
        return None

    # businessId is the third part
    business_id = parts[2]

    # Validate year and month parts
    year_str = parts[3]
    month_str = parts[4]

    # Check if year and month are digits and have correct length/value
    if not (year_str.isdigit() and len(year_str) == 4 and \
            month_str.isdigit() and len(month_str) == 2 and \
            1 <= int(month_str) <= 12):
        print(f"Debug: Year ('{year_str}') or month ('{month_str}') format is invalid.")
        return None

    # randomNumber is the fifth part (and potentially any subsequent parts if it contained hyphens, though unlikely)
    # For now, we assume randomNumber is parts[5]
    # random_number_str = parts[5]

    print(f"Debug: Successfully parsed businessId: '{business_id}'")
    return business_id


def extract_jwt_details(token_string: str, verify_signature: bool, secret: str) -> dict:
    """
    Decodes a JWT token and extracts details.
    Allows switching between secure (verify signature/expiration) and insecure (bypass verification) modes.
    """
    print(f"--- Processing token. Secure mode: {'ON' if verify_signature else 'OFF'} ---")

    if not token_string:
        return {"error": "Token string is empty."}

    header = {}
    try:
        header = jwt.get_unverified_header(token_string)
        print(f"Token header: {header}")
    except jwt.InvalidTokenError as e:
        return {"error": f"Invalid token header: {e}", "companyId": None, "userId": None, "subject": None, "payload": None, "verified_securely": False}

    algorithm = header.get("alg")
    if not algorithm:
         return {"error": "Token header does not contain 'alg' field.", "companyId": None, "userId": None, "subject": None, "business_id": None, "payload": None, "verified_securely": False}


    if verify_signature:
        print(f"Attempting secure verification with algorithm: {algorithm}...")
        try:
            # When verifying, PyJWT will check signature, expiration ('exp'), 'nbf', and 'iat' by default.
            payload = jwt.decode(token_string, secret, algorithms=[algorithm]) # Use algorithm from header
            print("✅ Token verified successfully (signature and claims).")
            company_id = payload.get("companyId")
            user_id = payload.get("userId")
            subject = payload.get("sub")
            business_id = None
            if company_id and user_id is not None:
                business_id = extract_business_id_from_company_id(company_id, user_id)
            return {
                "companyId": company_id,
                "userId": user_id,
                "subject": subject,
                "business_id": business_id,
                "payload": payload,
                "verified_securely": True
            }
        except jwt.ExpiredSignatureError:
            print("❌ Token has expired.")
            unverified_payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
            company_id = unverified_payload.get("companyId")
            user_id = unverified_payload.get("userId")
            business_id = None
            if company_id and user_id is not None:
                business_id = extract_business_id_from_company_id(company_id, user_id)
            return {
                "error": "Token has expired.",
                "companyId": company_id,
                "userId": user_id,
                "subject": unverified_payload.get("sub"),
                "business_id": business_id,
                "payload": unverified_payload,
                "verified_securely": False
            }
        except jwt.InvalidSignatureError:
            print("❌ Signature verification failed.")
            unverified_payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
            company_id = unverified_payload.get("companyId")
            user_id = unverified_payload.get("userId")
            business_id = None
            if company_id and user_id is not None:
                business_id = extract_business_id_from_company_id(company_id, user_id)
            return {
                "error": "Signature verification failed.",
                "companyId": company_id,
                "userId": user_id,
                "subject": unverified_payload.get("sub"),
                "business_id": business_id,
                "payload": unverified_payload,
                "verified_securely": False
            }
        except jwt.InvalidTokenError as e:
            print(f"❌ Invalid token: {e}")
            unverified_payload = None
            try:
                unverified_payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
                company_id = unverified_payload.get("companyId")
                user_id = unverified_payload.get("userId")
                business_id = None
                if company_id and user_id is not None:
                    business_id = extract_business_id_from_company_id(company_id, user_id)
            except Exception: # pylint: disable=broad-except
                company_id = None
                user_id = None
                business_id = None

            return {
                "error": f"Invalid token: {e}",
                "companyId": company_id,
                "userId": user_id,
                "subject": unverified_payload.get("sub") if unverified_payload else None,
                "business_id": business_id,
                "payload": unverified_payload,
                "verified_securely": False
            }
    else:
        # Insecure mode: bypass signature and expiration verification
        print("⚠️ Bypassing signature and expiration verification (INSECURE MODE).")
        try:
            payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
            company_id = payload.get("companyId")
            user_id = payload.get("userId")
            subject = payload.get("sub")
            business_id = None
            if company_id and user_id is not None:
                business_id = extract_business_id_from_company_id(company_id, user_id)
            print("✅ Token decoded (insecurely).")
            return {
                "companyId": company_id,
                "userId": user_id,
                "subject": subject,
                "business_id": business_id,
                "payload": payload,
                "verified_securely": False
            }
        except jwt.InvalidTokenError as e:
            # This might catch really malformed tokens, etc.
            print(f"❌ Error decoding token even in insecure mode: {e}")
            return {
                "error": f"Error decoding token (insecure mode): {e}",
                "companyId": None,
                "userId": None,
                "subject": None,
                "business_id": None,
                "payload": None,
                "verified_securely": False
            }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a JWT token securely or insecurely and extract claims, including business_id.")
    parser.add_argument("token", help="The JWT token string to process.")
    parser.add_argument("--secure", dest='verify_signature_arg', action='store_true', help="Force secure verification (overrides global VERIFY_JWT_SIGNATURE).")
    parser.add_argument("--insecure", dest='verify_signature_arg', action='store_false', help="Force insecure verification (overrides global VERIFY_JWT_SIGNATURE).")
    parser.set_defaults(verify_signature_arg=None) # Default to using the global switch

    args = parser.parse_args()
    token_to_test = args.token

    # Determine verification mode: command-line arg > global switch
    if args.verify_signature_arg is not None:
        current_verify_mode = args.verify_signature_arg
        print(f"Using command-line override for verification mode: {'Secure' if current_verify_mode else 'Insecure'}")
    else:
        current_verify_mode = VERIFY_JWT_SIGNATURE # Use global switch
        print(f"Using global VERIFY_JWT_SIGNATURE setting: {'Secure' if current_verify_mode else 'Insecure'}")

    # Scenario 1: Secure verification (VERIFY_JWT_SIGNATURE = True at the top of the script)
    print(f"\n--- SCENARIO 1: TESTING WITH VERIFICATION MODE = {'Secure' if current_verify_mode else 'Insecure'} ---")
    # To run this scenario, ensure VERIFY_JWT_SIGNATURE at the top is True or use --secure/--insecure

    details = extract_jwt_details(token_to_test, verify_signature=current_verify_mode, secret=DECODED_JWT_SECRET)
    print(f"Extracted: Company ID = {details.get('companyId')}, User ID = {details.get('userId')}, Subject = {details.get('subject')}, Business ID = {details.get('business_id')}")
    if details.get("error"):
        print(f"Error: {details.get('error')}")
    # print(f"Full Payload: {details.get('payload')}") # Potentially verbose
    print(f"Verified Securely: {details.get('verified_securely')}")


    # Example with an EXPIRED token (from previous tests)
    # This token will also be processed according to current_verify_mode
    EXPIRED_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW1haXJhd2FuIiwiY29tcGFueUlkIjoiRmF6LTItMi0yMDI0LTA3LTM3OTkzIiwidXNlcklkIjoyLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEyOTkxNzEsImV4cCI6MTc1MTMwMDk3MX0.c4uMTzeGm4niU9sPnN8M3K6JP9peRh29WwJbkiDb27D1Qxmhxivq0KGp8Bl8Y89XkO08Jks2a73_X8ZiyFTeFw"
    print(f"\n--- TESTING EXPIRED TOKEN WITH VERIFICATION MODE = {'Secure' if current_verify_mode else 'Insecure'} ---")
    details_expired = extract_jwt_details(EXPIRED_TOKEN, verify_signature=current_verify_mode, secret=DECODED_JWT_SECRET)
    print(f"Extracted (Expired Token): Company ID = {details_expired.get('companyId')}, User ID = {details_expired.get('userId')}, Subject = {details_expired.get('subject')}, Business ID = {details_expired.get('business_id')}")
    if details_expired.get("error"):
        print(f"Error (Expired Token): {details_expired.get('error')}")
    # print(f"Full Payload (Expired Token): {details_expired.get('payload')}") # Potentially verbose
    print(f"Verified Securely (Expired Token): {details_expired.get('verified_securely')}")
