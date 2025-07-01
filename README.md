## JWT Processing Application

This application provides utilities to process JWT (JSON Web Tokens) and extract relevant claims, including a custom `business_id` from a structured `companyId` claim. It is designed to be configurable via environment variables for use in different environments (e.g., development, production).

### Modules

1.  **`jwt_utils.py`**:
    *   A library module containing the core logic for JWT processing.
    *   `extract_jwt_details(token_string, verify_signature, secret)`: Decodes the JWT.
        *   `token_string` (str): The JWT to process.
        *   `verify_signature` (bool): If `True`, attempts full cryptographic verification (signature, expiration). If `False`, bypasses these checks (INSECURE, for debugging or specific internal uses only).
        *   `secret` (str): The secret key for HS512 algorithm verification.
        *   Returns a dictionary with claims, error messages, and a `verified_securely` flag.
    *   `extract_business_id_from_company_id(company_id_str, user_id_from_token)`: Parses `business_id`.
        *   Assumes `companyId` format: `Faz-<userId>-<businessId>-<year>-<month>-<randomNumber>`
        *   Returns the extracted `businessId` string or `None`.

2.  **`app_main.py`**:
    *   An example command-line application that uses `jwt_utils.py`.
    *   Demonstrates how to load configuration from environment variables and process a token passed as an argument.

### Configuration (Environment Variables for `app_main.py`)

*   **`JWT_SECRET_KEY`** (Required):
    *   The secret key used for JWT signature verification.
    *   Example: `export JWT_SECRET_KEY="your_actual_secret_string"`
*   **`JWT_SECRET_IS_BASE64`** (Optional, Default: `"False"`):
    *   Set to `"True"` if the `JWT_SECRET_KEY` environment variable contains a base64 encoded secret (common in Kubernetes). The application will attempt to decode it.
    *   Example: `export JWT_SECRET_IS_BASE64="True"`
*   **`JWT_VERIFY_MODE`** (Optional, Default: `"True"`):
    *   Determines the default JWT verification behavior for the application.
    *   `"True"`: Secure mode (verify signature and expiration).
    *   `"False"`: Insecure mode (bypass verification - **NOT FOR PRODUCTION unless you have a very specific reason and understand the risks**).
    *   Example: `export JWT_VERIFY_MODE="False"`

### Running `app_main.py`

1.  **Set Environment Variables:**
    ```bash
    export JWT_SECRET_KEY="your_secret" # Replace with your actual HS512 secret
    # export JWT_SECRET_IS_BASE64="True" # Uncomment if your secret is base64 encoded
    # export JWT_VERIFY_MODE="False"   # Uncomment to default to insecure mode
    ```

2.  **Run the script with a JWT token:**
    ```bash
    python app_main.py <your_jwt_token_string>
    ```
    Example:
    ```bash
    python app_main.py "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"
    ```

3.  **Command-line Overrides for Verification Mode:**
    You can override the `JWT_VERIFY_MODE` from the environment for a single run:
    *   Force secure: `python app_main.py --override_secure <token>`
    *   Force insecure: `python app_main.py --override_insecure <token>`

### Security Note
Bypassing JWT signature verification (`verify_signature=False` or `JWT_VERIFY_MODE="False"`) means the application will not check if the token is authentic or if it has been tampered with. This should only be done with extreme caution in non-production environments or for very specific, trusted internal use cases where the token's authenticity has been verified by another component. **For any security-sensitive operations, always verify the token signature.**