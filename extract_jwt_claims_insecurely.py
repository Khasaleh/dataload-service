import jwt
import datetime # Required for printing expiry if we were checking it

# WARNING: THIS SCRIPT DEMONSTRATES DECODING A JWT WITHOUT SIGNATURE VERIFICATION.
# THIS IS INSECURE AND SHOULD NOT BE USED FOR AUTHENTICATION OR AUTHORIZATION
# OR ANY SITUATION WHERE THE TRUSTWORTHINESS OF THE TOKEN CLAIMS IS REQUIRED.
# IT IS SHOWN PURELY FOR EDUCATIONAL PURPOSES TO EXTRACT CLAIMS FROM A TOKEN
# WHEN THE SECRET KEY IS UNKNOWN OR MISMATCHED, AND SIGNATURE VERIFICATION IS BYPASSED.

# Token provided by the user (replace if you have a different one)
# First token: "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW1haXJhd2FuIiwiY29tcGFueUlkIjoiRmF6LTItMi0yMDI0LTA3LTM3OTkzIiwidXNlcklkIjoyLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEyOTkxNzEsImV4cCI6MTc1MTMwMDk3MX0.c4uMTzeGm4niU9sPnN8M3K6JP9peRh29WwJbkiDb27D1Qxmhxivq0KGp8Bl8Y89XkO08Jks2a73_X8ZiyFTeFw"
# Second token: "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"
# Third token (from chat history): "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW1haXJhd2FuIiwiY29tcGFueUlkIjoiRmF6LTItMi0yMDI0LTA3LTM3OTkzIiwidXNlcklkIjoyLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTA5NjE1MDAsImV4cCI6MTc1MDk2MzMwMH0.wWi0QtROMqnuSdcnGcEb09pOesc1ehJ0f0F8ekpMr6968KAruGZSAEd_pWd-S0P5_JZtjIbJLM3AIjTmCRAX-g"

token_to_decode = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"

print("="*70)
print("WARNING: JWT Signature Verification is BYPASSED.")
print("The extracted claims below are NOT verified and should NOT be trusted for security purposes.")
print("="*70)
print(f"Processing Token: {token_to_decode[:50]}...") # Print first 50 chars

try:
    # Decode the token without verifying the signature or expiration
    # options={"verify_signature": False} is the key part for bypassing signature check.
    # options={"verify_exp": False} bypasses expiry check if you also need that.
    # For just extracting data, bypassing both might be what's intended in this insecure scenario.
    decoded_payload = jwt.decode(
        token_to_decode,
        options={
            "verify_signature": False,
            "verify_exp": False  # Also bypassing expiration check as per the use-case
        }
    )

    print("\n--- Successfully Decoded Payload (INSECURE) ---")

    # Extract specific claims
    # Using .get() is safer as it returns None if the key is missing, rather than raising a KeyError
    subject = decoded_payload.get("sub")
    company_id = decoded_payload.get("companyId") # As per token structure
    user_id = decoded_payload.get("userId")
    issued_at_timestamp = decoded_payload.get("iat")
    expires_at_timestamp = decoded_payload.get("exp")
    roles = decoded_payload.get("role")

    # Print extracted claims
    print(f"  Subject (sub): {subject}")
    print(f"  Company ID (companyId): {company_id}") # This is likely the "business_id" user is after
    print(f"  User ID (userId): {user_id}")

    if roles:
        print(f"  Roles (role): {roles}")
    else:
        print("  Roles (role): Not found")

    if issued_at_timestamp:
        iat_datetime = datetime.datetime.fromtimestamp(issued_at_timestamp, tz=datetime.timezone.utc)
        print(f"  Issued At (iat): {issued_at_timestamp} ({iat_datetime})")
    else:
        print("  Issued At (iat): Not found")

    if expires_at_timestamp:
        exp_datetime = datetime.datetime.fromtimestamp(expires_at_timestamp, tz=datetime.timezone.utc)
        print(f"  Expires At (exp): {expires_at_timestamp} ({exp_datetime})")
        # For informational purpose, check if it would have been expired if we were checking
        if exp_datetime < datetime.datetime.now(tz=datetime.timezone.utc):
            print("    (Note: This token would be considered EXPIRED if expiration was checked.)")
        else:
            print("    (Note: This token would NOT be considered EXPIRED if expiration was checked.)")
    else:
        print("  Expires At (exp): Not found")

    print("\n--- Full Decoded Payload (INSECURE) ---")
    for key, value in decoded_payload.items():
        print(f"  {key}: {value}")

except jwt.DecodeError as e:
    # This catches errors if the token is malformed (e.g., not enough segments)
    # It's less likely to be InvalidSignatureError or ExpiredSignatureError
    # because we are disabling those specific checks.
    print(f"\nError decoding token: {e}")
    print("This could be due to a malformed token (e.g., incorrect structure).")
except Exception as e:
    # Catch any other unexpected errors
    print(f"\nAn unexpected error occurred: {e}")

print("\nReminder: The above information was extracted without verifying the token's signature.")
print("It should be used with extreme caution and NOT for sensitive operations.")
