import os
import argparse
import base64 # For potentially decoding a base64 secret from env
from jwt_utils import extract_jwt_details # Import the function from our new library

def main():
    # --- Load Configuration from Environment Variables ---
    # CRITICAL: JWT_SECRET_KEY is the secret used to verify JWT signatures.
    # This value MUST EXACTLY MATCH the secret key used by the JWT issuer
    # (e.g., your Java/Spring application) to SIGN the tokens.
    # If these keys do not match, signature verification WILL FAIL.
    jwt_secret_key = os.getenv("JWT_SECRET_KEY")

    # Optional: If the secret in env is base64 encoded (like in K8s secrets)
    # Set JWT_SECRET_IS_BASE64="True" if JWT_SECRET_KEY holds a base64 encoded version
    # of the actual raw secret. The script will then decode it.
    # Otherwise, JWT_SECRET_KEY is treated as the raw secret string.
    jwt_secret_is_base64 = os.getenv("JWT_SECRET_IS_BASE64", "False").lower() == 'true'

    # This variable will hold the secret, either as string or bytes
    actual_jwt_secret_for_lib: str | bytes

    if jwt_secret_is_base64 and jwt_secret_key:
        try:
            # Decode from base64, keep as bytes
            actual_jwt_secret_for_lib = base64.b64decode(jwt_secret_key)
            print("Successfully decoded base64 encoded JWT_SECRET_KEY from environment (kept as bytes).")
        except Exception as e:
            print(f"Error decoding base64 JWT_SECRET_KEY: {e}. Please ensure it's valid base64.")
            jwt_secret_key = None # Indicate failure to load/decode the original string
            actual_jwt_secret_for_lib = b'' # or some other indicator of failure for bytes
    elif jwt_secret_key:
        # Use the secret key as a string directly
        actual_jwt_secret_for_lib = jwt_secret_key
        print("Using JWT_SECRET_KEY from environment as a raw string.")
    else:
        # jwt_secret_key was not set initially
        pass # The check below will handle it

    if not jwt_secret_key: # This checks if the original env var was set
        print("Error: JWT_SECRET_KEY environment variable not set.")
        return

    if isinstance(actual_jwt_secret_for_lib, bytes) and not actual_jwt_secret_for_lib and jwt_secret_is_base64 :
        # This means base64 decoding failed, and we ended up with empty bytes from the error handling
        print("Error: Failed to obtain a valid secret after base64 decoding attempt.")
        print("Please set it to your JWT signing secret (base64 decoded if JWT_SECRET_IS_BASE64 is True).")
        return

    # Determine verification mode: Default to secure (True)
    # Environment variable JWT_VERIFY_MODE can be "True" or "False" (case-insensitive)
    verify_mode_str = os.getenv("JWT_VERIFY_MODE", "True")
    app_verify_signature = verify_mode_str.lower() == 'true'

    print(f"Application JWT Verification Mode: {'Secure' if app_verify_signature else 'Insecure'}")
    if not app_verify_signature:
        print("WARNING: Application is running in INSECURE JWT verification mode. DO NOT USE IN PRODUCTION.")

    # --- Command-line Argument Parsing for the Token ---
    parser = argparse.ArgumentParser(
        description="Process a JWT token using environment-configured settings."
    )
    parser.add_argument("token", help="The JWT token string to process.")
    # Allow command-line override for verification mode for quick testing,
    # but it will still use the app's loaded secret.
    parser.add_argument(
        "--override_secure",
        dest='cli_verify_mode',
        action='store_true',
        help="Force secure verification for this run (overrides JWT_VERIFY_MODE from env)."
    )
    parser.add_argument(
        "--override_insecure",
        dest='cli_verify_mode',
        action='store_false',
        help="Force insecure verification for this run (overrides JWT_VERIFY_MODE from env)."
    )
    parser.set_defaults(cli_verify_mode=None)

    args = parser.parse_args()
    token_to_process = args.token

    current_run_verify_mode = app_verify_signature
    if args.cli_verify_mode is not None:
        current_run_verify_mode = args.cli_verify_mode
        print(f"CLI override for verification mode: {'Secure' if current_run_verify_mode else 'Insecure'}")
        if not current_run_verify_mode:
             print("WARNING: CLI override to INSECURE JWT verification mode for this run.")

    # --- Process the Token ---
    print(f"\nProcessing token (Mode: {'Secure' if current_run_verify_mode else 'Insecure'})...")
    token_details = extract_jwt_details(
        token_string=token_to_process,
        verify_signature=current_run_verify_mode,
        secret=actual_jwt_secret_for_lib # Pass the potentially bytes secret
    )

    # --- Display Results ---
    print("\n--- Token Processing Results ---")
    if token_details.get("error"):
        print(f"Error: {token_details.get('error')}")

    print(f"Company ID: {token_details.get('companyId')}")
    print(f"User ID: {token_details.get('userId')}")
    print(f"Subject: {token_details.get('subject')}")
    print(f"Business ID (from companyId): {token_details.get('business_id')}")
    print(f"Verified Securely: {token_details.get('verified_securely')}")

    if current_run_verify_mode and not token_details.get("verified_securely") and not token_details.get("error"):
        # This case might happen if an error occurred but wasn't properly flagged,
        # or if it's a non-verifying error like 'alg mismatch' but still got some payload.
        print("WARNING: Token was processed in secure mode but was not flagged as securely verified and no primary error was listed.")

    # print(f"Full Payload: {token_details.get('payload')}") # Uncomment for debugging if needed

if __name__ == "__main__":
    main()
