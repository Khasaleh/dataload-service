# Upload Service

FastAPI + Celery-based upload and processing service.

## Configuration

The application is configured using environment variables. A comprehensive list of all required and optional environment variables can be found in the `.env.example` file in the root of this repository. See the "Local Development Setup" section for details on using `.env` files.

For deployed environments such as production or staging, environment variables should be set directly in the execution environment. This is typically managed through:

*   Kubernetes ConfigMaps and Secrets
*   CI/CD pipeline variables
*   Platform-as-a-Service (PaaS) environment variable settings
*   Other secure configuration management tools

Do **not** deploy `.env` files to these environments. The `.env.example` file can serve as a reference for which variables need to be configured.

## Getting Started / Local Development Setup

### Prerequisites
*   Python 3.8+ (or the version specified in your project, e.g., pyproject.toml)
*   Poetry (recommended for managing dependencies - see `poetry.lock`) or `pip`.
*   A running PostgreSQL instance (or the database type specified by `DB_DRIVER` in your `.env` file).
*   A running Redis instance.
*   Alembic (installed as part of project dependencies, used for database migrations).

### Setup Instructions

1.  **Clone the repository**:
    ```bash
    git clone <repository_url> # Replace <repository_url> with the actual URL
    cd <repository_name>     # Replace <repository_name> with the project's directory name
    ```

2.  **Install dependencies**:
    If using Poetry:
    ```bash
    poetry install
    ```
    If using pip with `requirements.txt`:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables**:
    Copy the `.env.example` file to a new file named `.env`. This file will contain your local configuration.
    ```bash
    cp .env.example .env
    ```
    Open the `.env` file and edit the placeholder values with your actual development settings for:
    *   Database connection (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_DRIVER`)
    *   Wasabi S3 storage (`WASABI_ENDPOINT_URL`, `WASABI_ACCESS_KEY`, `WASABI_SECRET_KEY`, `WASABI_BUCKET_NAME`)
    *   Redis connection (`REDIS_HOST`, `REDIS_PORT`, database numbers for mapping, Celery)
    *   JWT secrets (`JWT_SECRET`, `JWT_ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`)

    The project uses `python-dotenv` to automatically load variables from this `.env` file when the application starts up locally.

    **Important**: The `.env` file contains sensitive credentials and is specific to your local setup. It is already listed in `.gitignore` and **must not be committed to version control**.

4.  **Database Setup & Migrations**:
    This application uses SQLAlchemy for database interaction and Alembic for managing schema migrations.

    *   **Ensure your database instance is running** and accessible with the credentials provided in your `.env` file. You will need to create the main database (e.g., `your_db_name` from `.env`) if it doesn't exist:
        ```sql
        -- Example for PostgreSQL:
        CREATE DATABASE your_db_name;
        ```

    *   **Multi-Tenant Schema Setup (Schema-per-Tenant)**:
        This application is designed for multi-tenancy using a schema-per-tenant strategy (when using PostgreSQL, as configured in `app/db/connection.py`). This means data for each business (`business_id`) is stored in its own dedicated schema within the main database.
        *   The convention used is `business_<business_id>` (e.g., `business_acme` for `business_id="acme"`).
        *   **You must create these tenant-specific schemas manually in your database before you can apply migrations to them or load data for that tenant.**
            ```sql
            -- Example for PostgreSQL to create a schema for business_id 'acme':
            CREATE SCHEMA IF NOT EXISTS business_acme;
            ```
        *   Repeat this for each `business_id` you plan to use during development or testing. The application's runtime (`app/db/connection.py`) will attempt to set the `search_path` to this schema for relevant database sessions.

    *   **Apply Database Migrations**: Alembic is used to manage database schema versions. To apply migrations to a specific tenant schema (after creating it):
        *   You'll need to ensure your database session for running Alembic targets the correct schema. One common way for local development is to connect to your database using a tool like `psql` or a GUI client, set the `search_path` for your session, and then run Alembic commands in a separate terminal where Alembic will use the default database connection (which should now resolve to the correct schema due to your session's `search_path`).
            ```sql
            -- In your SQL client, connected to your main database (DB_NAME):
            SET search_path TO business_yourbusinessid, public;
            ```
        *   Alternatively, for more robust control, especially in scripts or automated environments, you can adapt `alembic/env.py` to accept a schema parameter (e.g., via `-x` option in Alembic CLI) and set the `search_path` or `version_table_schema` within `env.py` before migrations run.
        *   Once targeting the desired schema, run:
            ```bash
            alembic upgrade head
            ```
        This will create all necessary tables (brands, products, etc.) inside that tenant's schema. Repeat this process for each tenant schema.
        *(For advanced multi-tenant migration strategies with Alembic, refer to Alembic documentation on handling multiple schemas or programmable migration environments.)*

    *   **Creating New Migrations (for developers)**: If you modify the SQLAlchemy ORM models in `app/db/models.py`, you'll need to generate a new migration script:
        ```bash
        # Ensure your virtual environment is active
        alembic revision -m "short_description_of_your_model_changes" --autogenerate
        ```
        After generation, **always review and edit** the script created in the `alembic/versions/` directory to ensure it accurately reflects the intended changes. This script will then need to be applied to each tenant schema using the `alembic upgrade head` command as described above.

5.  **Running the Application**:

    *   **FastAPI Web Server**:
        The application is served using Uvicorn. To run the development server:
        ```bash
        uvicorn app.main:app --reload
        ```
        This will typically start the server on `http://127.0.0.1:8000`. The `--reload` flag enables auto-reloading when code changes are detected.

    *   **Celery Worker**:
        For background task processing, you need to run at least one Celery worker. Open a new terminal window/tab, activate your virtual environment, and run:
        ```bash
        celery -A app.tasks.celery_worker.celery_app worker -l info
        # For Windows or environments without eventlet/gevent, or for simpler local testing:
        # celery -A app.tasks.celery_worker.celery_app worker -l info -P solo
        ```
        Ensure your Redis instance (used as the Celery broker and result backend) is running and accessible as per your `.env` configuration.

    Once both the FastAPI server and Celery worker are running, you can access the GraphQL API (e.g., via the GraphiQL interface at `/graphql`).

## API Access (GraphQL)

This service exposes a GraphQL API.

**Endpoint**: `/graphql`

When the application is running, you can also access an interactive GraphiQL interface by navigating to the `/graphql` endpoint in your browser. This interface allows you to explore the schema and execute queries and mutations directly.

### Example Query: Get Current User Details

```graphql
query GetMe {
  me {
    userId     # User's unique ID
    username   # User's login name
    businessId # ID of the business/company the user belongs to
    roles      # List of roles assigned to the user (e.g., ["ROLE_ADMIN"])
  }
}
```

### Example Query: Get Upload Sessions for a Business

(Requires authentication)

```graphql
query GetMyBusinessUploads {
  # Assumes business_id is derived from your authentication token
  # The resolver for uploadSessionsByBusiness uses the authenticated user's business_id
  uploadSessionsByBusiness(limit: 10) {
    sessionId
    originalFilename
    loadType
    status
    createdAt
    updatedAt
    recordCount
    errorCount
    details
  }
}
```

### Mutations

This section details the available GraphQL mutations for creating or modifying data.

#### General Error Response Structure

When a mutation (or query) encounters an error that is handled at the GraphQL layer (e.g., validation error, resource not found, authorization issue caught by resolver), the response will typically have a status code of 200 OK (unless it's a server-level or network error) but will include an `errors` array in the JSON payload. The `data` field for the specific mutation may be `null`.

```json
{
  "errors": [
    {
      "message": "Specific error message here.",
      "path": ["mutationName"],
      "extensions": {
        "code": "OPTIONAL_ERROR_CODE"
        // Additional extensions might be present depending on the error
      }
    }
  ],
  "data": {
    "mutationName": null
  }
}
```
Note: For authentication failures handled by FastAPI before GraphQL execution (e.g., missing or fundamentally invalid token), the server might return an HTTP 401 or 403 status code directly with a non-GraphQL JSON error body.

---

#### `generateToken`

```graphql
**Signature**: `generateToken(input: GenerateTokenInput!) -> TokenResponseType`

**Description**: Authenticates a user based on their username and password, returning a set of JWT access and refresh tokens upon success.

**Input (`GenerateTokenInput`)**:
*   `username: String!`
*   `password: String!`

**Happy Path Example**:

*Request*:
```graphql
mutation GenerateUserToken {
  generateToken(input: {username: "testuser", password: "password"}) {
    token
    tokenType
    refreshToken
  }
}
```

*Success Response (200 OK)*:
```json
{
  "data": {
    "generateToken": {
      "token": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0dXNlciIs...",
      "tokenType": "bearer",
      "refreshToken": "mock-rt-testuser-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    }
  }
}
```

**Unhappy Path Example (Invalid Credentials)**:

*Request* (same as above, but with wrong credentials)

*Error Response (200 OK with GraphQL errors)*:
```json
{
  "errors": [
    {
      "message": "Invalid username or password.",
      "path": ["generateToken"],
      "extensions": {} // Extensions might vary
    }
  ],
  "data": {
    "generateToken": null
  }
}
```

---

#### `refreshToken`

(Assumes you have a valid, unexpired `refreshToken`)

```graphql
**Signature**: `refreshToken(input: RefreshTokenInput!) -> TokenResponseType`

**Description**: Exchanges a valid (and unexpired, unrevoked) refresh token for a new set of access and refresh tokens. This typically implements refresh token rotation, where the used refresh token is invalidated and a new one is issued.

**Input (`RefreshTokenInput`)**:
*   `refreshToken: String!`

**Happy Path Example**:

*Request*:
```graphql
mutation RefreshUserToken($rt: String!) {
  refreshToken(input: {refreshToken: $rt}) {
    token
    tokenType
    refreshToken
  }
}
```
*Variables*:
```json
{
  "rt": "previously_issued_refresh_token_value"
}
```

*Success Response (200 OK)*:
```json
{
  "data": {
    "refreshToken": {
      "token": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0dXNlciIs...",
      "tokenType": "bearer",
      "refreshToken": "new_mock-rt-testuser-yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
    }
  }
}
```

**Unhappy Path Examples**:

*   **Invalid/Expired Refresh Token**:
    *Request* (with an invalid token)
    *Error Response (200 OK with GraphQL errors)*:
    ```json
    {
      "errors": [
        {
          "message": "Invalid or expired refresh token.",
          "path": ["refreshToken"],
          "extensions": {}
        }
      ],
      "data": {
        "refreshToken": null
      }
    }
    ```

*   **User Account Disabled**:
    *Request* (with a refresh token belonging to a disabled user)
    *Error Response (200 OK with GraphQL errors)*:
    ```json
    {
      "errors": [
        {
          "message": "User account is disabled.",
          "path": ["refreshToken"],
          "extensions": {}
        }
      ],
      "data": {
        "refreshToken": null
      }
    }
    ```

---

#### Access Token (auth-token)

The `token` returned by `generateToken` and `refreshToken` is a JWT (JSON Web Token). It includes standard claims like `exp` (expiration time) and `iat` (issued at time), as well as application-specific claims such as:
*   `sub` (Subject - typically the username)
*   `userId` (The user's unique identifier)
*   `companyId` (The identifier for the user's company/business, used internally as `business_id`)
*   `role` (An array of role objects, e.g., `[{"authority":"ROLE_ADMIN"}]`)

Clients should typically treat the access token as opaque but must send it in the `Authorization: Bearer <token>` header for authenticated requests. The application uses HS512 as the default signing algorithm for these tokens.

---

#### `uploadFile`

**Signature**: `uploadFile(file: Upload!, input: UploadFileInput!) -> UploadSessionType`

**Description**: Uploads a CSV file for a specified `load_type`. This mutation requires authentication. The server validates the file, stores it temporarily (e.g., to Wasabi), creates an `UploadSession` record, and queues a background task (Celery) for processing the file content.

**Input**:
*   `file: Upload!`: The actual CSV file being uploaded. This is handled as part of a multipart form request.
*   `input: UploadFileInput!`:
    *   `load_type: String!`: Specifies the type of data being uploaded (e.g., "products", "brands", "attributes").

**Happy Path Example**:

*Request*:
(This is a conceptual representation. Actual execution requires a multipart request.)
```graphql
mutation UploadMyFile($theFile: Upload!, $type: String!) {
  uploadFile(file: $theFile, input: { loadType: $type }) {
    sessionId
    originalFilename
    loadType
    status
    wasabiPath
    createdAt
  }
}
```
*Variables*:
```json
{
  "theFile": null, /* Actual file data sent as part of multipart form */
  "type": "products"
}
```

*Success Response (200 OK)*:
```json
{
  "data": {
    "uploadFile": {
      "sessionId": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
      "originalFilename": "my_products.csv",
      "loadType": "products",
      "status": "pending",
      "wasabiPath": "uploads/biz_123/a1b2c3d4-e5f6-7890-1234-567890abcdef/products/my_products.csv",
      "createdAt": "2023-10-27T10:30:00Z"
      // ... other fields like recordCount, errorCount, details, updatedAt will also be present
    }
  }
}
```

**Unhappy Path Examples**:

*   **Invalid `load_type`**:
    *Error Response (200 OK with GraphQL errors)*:
    (Assuming `non_existent_type` was the `load_type` provided in the input)
    ```json
    {
      "errors": [
        {
          "message": "Invalid load type: non_existent_type",
          "path": ["uploadFile"],
          "extensions": {}
        }
      ],
      "data": null
    }
    ```

*   **Invalid File Type (Not CSV)**:
    *Error Response (200 OK with GraphQL errors)*:
    ```json
    {
      "errors": [
        {
          "message": "Invalid file type. Only CSV files are allowed.",
          "path": ["uploadFile"],
          "extensions": {}
        }
      ],
      "data": null
    }
    ```

*   **Empty File Submitted**:
    *Error Response (200 OK with GraphQL errors)*:
    ```json
    {
      "errors": [
        {
          "message": "Empty CSV file submitted.",
          "path": ["uploadFile"],
          "extensions": {}
        }
      ],
      "data": null
    }
    ```

*   **Unauthenticated Access**:
    If authentication fails (e.g., missing or invalid `Authorization: Bearer <token>` header), the server typically returns an HTTP 401 Unauthorized response with a JSON body like `{"detail":"Not authenticated"}` *before* GraphQL execution. If the token is present but deemed invalid by the GraphQL layer's authentication check (e.g., user not found for token, or specific claim issue not caught by `get_current_user`'s initial validation), an error within the GraphQL response `errors` array, such as "Authentication required: User or business ID not found in context.", might occur.

---

### Tools for Interacting with GraphQL

You can use various tools to interact with the GraphQL API, such as:
*   GraphiQL (available at `/graphql` when the server runs)
*   Insomnia
*   Postman (supports GraphQL requests)
*   Altair GraphQL Client (browser extension or desktop app)
*   Programmatic clients like `httpx` in Python, Apollo Client in JavaScript, etc.
