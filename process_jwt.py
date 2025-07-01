import jwt
import os
import datetime

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

# Token to process (using the second token provided by the user, which is not expired)
DEFAULT_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"

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
         return {"error": "Token header does not contain 'alg' field.", "companyId": None, "userId": None, "subject": None, "payload": None, "verified_securely": False}


    if verify_signature:
        print(f"Attempting secure verification with algorithm: {algorithm}...")
        try:
            # When verifying, PyJWT will check signature, expiration ('exp'), 'nbf', and 'iat' by default.
            payload = jwt.decode(token_string, secret, algorithms=[algorithm]) # Use algorithm from header
            print("✅ Token verified successfully (signature and claims).")
            company_id = payload.get("companyId")
            user_id = payload.get("userId")
            subject = payload.get("sub")
            return {
                "companyId": company_id,
                "userId": user_id,
                "subject": subject,
                "payload": payload,
                "verified_securely": True
            }
        except jwt.ExpiredSignatureError:
            print("❌ Token has expired.")
            # Still decode unverified to get claims if needed for logging/info
            unverified_payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
            return {
                "error": "Token has expired.",
                "companyId": unverified_payload.get("companyId"),
                "userId": unverified_payload.get("userId"),
                "subject": unverified_payload.get("sub"),
                "payload": unverified_payload,
                "verified_securely": False # Even though we tried, it failed verification
            }
        except jwt.InvalidSignatureError:
            print("❌ Signature verification failed.")
            unverified_payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
            return {
                "error": "Signature verification failed.",
                "companyId": unverified_payload.get("companyId"),
                "userId": unverified_payload.get("userId"),
                "subject": unverified_payload.get("sub"),
                "payload": unverified_payload,
                "verified_securely": False
            }
        except jwt.InvalidTokenError as e:
            print(f"❌ Invalid token: {e}")
            # Attempt to get payload for debugging if possible
            try:
                unverified_payload = jwt.decode(token_string, options={"verify_signature": False, "verify_exp": False})
            except Exception: # pylint: disable=broad-except
                unverified_payload = None
            return {
                "error": f"Invalid token: {e}",
                "companyId": unverified_payload.get("companyId") if unverified_payload else None,
                "userId": unverified_payload.get("userId") if unverified_payload else None,
                "subject": unverified_payload.get("sub") if unverified_payload else None,
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
            print("✅ Token decoded (insecurely).")
            return {
                "companyId": company_id,
                "userId": user_id,
                "subject": subject,
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
                "payload": None,
                "verified_securely": False
            }

if __name__ == "__main__":
    token_to_test = DEFAULT_TOKEN

    # Scenario 1: Secure verification (VERIFY_JWT_SIGNATURE = True at the top of the script)
    print("\n--- SCENARIO 1: SECURE MODE (VERIFY_JWT_SIGNATURE = True) ---")
    # To run this scenario, ensure VERIFY_JWT_SIGNATURE at the top is True
    if VERIFY_JWT_SIGNATURE:
        details_secure = extract_jwt_details(token_to_test, verify_signature=True, secret=DECODED_JWT_SECRET)
        print(f"Extracted (Secure): Company ID = {details_secure.get('companyId')}, User ID = {details_secure.get('userId')}, Subject = {details_secure.get('subject')}")
        if details_secure.get("error"):
            print(f"Error (Secure): {details_secure.get('error')}")
        print(f"Payload (Secure): {details_secure.get('payload')}")
        print(f"Verified Securely (Secure): {details_secure.get('verified_securely')}")
    else:
        print("VERIFY_JWT_SIGNATURE is False, skipping direct secure test in main block for clarity.")
        print("To test secure mode, set VERIFY_JWT_SIGNATURE = True at the top and re-run.")


    # Scenario 2: Insecure verification (VERIFY_JWT_SIGNATURE = True at the top of the script)
    print("\n--- SCENARIO 2: INSECURE MODE (VERIFY_JWT_SIGNATURE = True) ---")
    # To run this scenario, ensure VERIFY_JWT_SIGNATURE at the top is False
    if not VERIFY_JWT_SIGNATURE:
        details_insecure = extract_jwt_details(token_to_test, verify_signature=False, secret=DECODED_JWT_SECRET)
        print(f"Extracted (Insecure): Company ID = {details_insecure.get('companyId')}, User ID = {details_insecure.get('userId')}, Subject = {details_insecure.get('subject')}")
        if details_insecure.get("error"):
            print(f"Error (Insecure): {details_insecure.get('error')}")
        print(f"Payload (Insecure): {details_insecure.get('payload')}")
        print(f"Verified Securely (Insecure): {details_insecure.get('verified_securely')}")
    else:
        print("VERIFY_JWT_SIGNATURE is True, skipping direct insecure test in main block for clarity.")
        print("To test insecure mode, set VERIFY_JWT_SIGNATURE = True at the top and re-run, or call extract_jwt_details directly with verify_signature=False.")

    # Example of direct call for insecure mode if needed, irrespective of global switch
    print("\n--- EXAMPLE: Direct call to INSECURE extraction ---")
    details_direct_insecure = extract_jwt_details(token_to_test, verify_signature=False, secret=DECODED_JWT_SECRET)
    print(f"Extracted (Direct Insecure): Company ID = {details_direct_insecure.get('companyId')}")
    print(f"Verified Securely (Direct Insecure): {details_direct_insecure.get('verified_securely')}")


    # Example of direct call for secure mode if needed
    print("\n--- EXAMPLE: Direct call to SECURE extraction ---")
    details_direct_secure = extract_jwt_details(token_to_test, verify_signature=True, secret=DECODED_JWT_SECRET)
    print(f"Extracted (Direct Secure): Company ID = {details_direct_secure.get('companyId')}")
    if details_direct_secure.get("error"):
        print(f"Error (Direct Secure): {details_direct_secure.get('error')}")
    print(f"Verified Securely (Direct Secure): {details_direct_secure.get('verified_securely')}")

    # Example with an EXPIRED token in SECURE mode
    EXPIRED_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW1haXJhd2FuIiwiY29tcGFueUlkIjoiRmF6LTItMi0yMDI0LTA3LTM3OTkzIiwidXNlcklkIjoyLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEyOTkxNzEsImV4cCI6MTc1MTMwMDk3MX0.c4uMTzeGm4niU9sPnN8M3K6JP9peRh29WwJbkiDb27D1Qxmhxivq0KGp8Bl8Y89XkO08Jks2a73_X8ZiyFTeFw"
    print("\n--- EXAMPLE: SECURE extraction with an EXPIRED token ---")
    # Will use global VERIFY_JWT_SIGNATURE for this one. If False, it will be insecure.
    # Set VERIFY_JWT_SIGNATURE = True at the top to see secure handling of expired token.
    if VERIFY_JWT_SIGNATURE:
        details_expired_secure = extract_jwt_details(EXPIRED_TOKEN, verify_signature=True, secret=DECODED_JWT_SECRET)
        print(f"Extracted (Expired Token, Secure Attempt): Company ID = {details_expired_secure.get('companyId')}")
        if details_expired_secure.get("error"):
            print(f"Error (Expired Token, Secure Attempt): {details_expired_secure.get('error')}")
        print(f"Payload (Expired Token, Secure Attempt): {details_expired_secure.get('payload')}")
        print(f"Verified Securely (Expired Token, Secure Attempt): {details_expired_secure.get('verified_securely')}")
    else:
        print("Skipping expired token secure test as VERIFY_JWT_SIGNATURE is False.")
        print("Set VERIFY_JWT_SIGNATURE = True at the top to test this.")
