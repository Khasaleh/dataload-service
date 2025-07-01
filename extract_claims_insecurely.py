import jwt
import datetime

# WARNING: THIS SCRIPT DECODES A JWT WITHOUT VERIFYING ITS SIGNATURE OR EXPIRATION.
# THIS IS INSECURE AND SHOULD NOT BE USED FOR AUTHENTICATION, AUTHORIZATION,
# OR ANY OPERATION THAT REQUIRES TRUSTING THE TOKEN'S CONTENTS.
# IT IS FOR DEBUGGING OR INSPECTION PURPOSES ONLY WHEN THE RISKS ARE UNDERSTOOD.

# Token provided by the user (the second one)
# You can change this to any token you want to inspect.
TOKEN_TO_INSPECT = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"

def decode_jwt_insecurely(token_string):
    """
    Decodes a JWT string without verifying its signature or expiration.
    WARNING: Insecure method. For inspection only.
    """
    print("="*60)
    print("WARNING: Decoding JWT without signature/expiration verification!")
    print("This method is INSECURE and for INSPECTION PURPOSES ONLY.")
    print("DO NOT use the extracted data for authentication or authorization.")
    print("="*60)
    print(f"\nInspecting Token: {token_string[:30]}...{token_string[-30:]}\n")

    try:
        # Decode the token without verifying the signature or expiration
        # verify_exp=False is important if you want to see claims from an expired token.
        # verify_signature=False is the key part for insecure decoding.
        decoded_payload = jwt.decode(token_string,
                                     options={"verify_signature": False,
                                              "verify_exp": False})

        print("--- Successfully Decoded (Insecurely) ---")

        # Extract and print common claims
        # Your token uses "companyId", not "business_id" based on previous examples
        company_id = decoded_payload.get("companyId")
        user_id = decoded_payload.get("userId")
        subject = decoded_payload.get("sub")
        issued_at_timestamp = decoded_payload.get("iat")
        expires_at_timestamp = decoded_payload.get("exp")
        roles = decoded_payload.get("role")

        print(f"  Subject (sub): {subject}")
        print(f"  Company ID (companyId): {company_id}")
        print(f"  User ID (userId): {user_id}")

        if issued_at_timestamp:
            issued_at_datetime = datetime.datetime.fromtimestamp(issued_at_timestamp, tz=datetime.timezone.utc)
            print(f"  Issued At (iat): {issued_at_timestamp} ({issued_at_datetime})")
        else:
            print(f"  Issued At (iat): Not found")

        if expires_at_timestamp:
            expires_at_datetime = datetime.datetime.fromtimestamp(expires_at_timestamp, tz=datetime.timezone.utc)
            print(f"  Expires At (exp): {expires_at_timestamp} ({expires_at_datetime})")
            # Also check against current time
            if expires_at_datetime < datetime.datetime.now(tz=datetime.timezone.utc):
                print("    Status: This token is EXPIRED.")
            else:
                print("    Status: This token is NOT EXPIRED.")
        else:
            print(f"  Expires At (exp): Not found")

        print(f"  Roles (role): {roles}")

        print("\n--- All Claims ---")
        for key, value in decoded_payload.items():
            print(f"  {key}: {value}")

        return decoded_payload

    except jwt.InvalidTokenError as e:
        # This might catch errors like a malformed token.
        print(f"Error decoding token: {e}")
        print("This could be due to a malformed token structure, not necessarily a bad signature (as we are ignoring it).")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    extracted_data = decode_jwt_insecurely(TOKEN_TO_INSPECT)
    print("\n" + "="*60)
    if extracted_data:
        print("Extraction complete. Remember the security warnings.")
    else:
        print("Extraction failed or an error occurred.")
    print("="*60)
