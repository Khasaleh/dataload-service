# Upload Service

FastAPI + Celery-based upload and processing service.

## Configuration

The application is configured using environment variables. A comprehensive list of all required and optional environment variables can be found in the `.env.example` file in the root of this repository.

### Local Development

For local development, follow these steps to set up your environment:

1.  **Create a `.env` file**:
    Copy the example file to a new file named `.env`:
    ```bash
    cp .env.example .env
    ```

2.  **Edit `.env`**:
    Open the newly created `.env` file and replace the placeholder values with your actual development settings for databases, external services, secrets, etc.

3.  **Automatic Loading**:
    The project uses `python-dotenv` to automatically load variables from the `.env` file when the application starts up locally. There's no need to manually source the file.

**Important**: The `.env` file often contains sensitive credentials and configuration specific to your local setup. It is already listed in `.gitignore` and **must not be committed to version control**.

### Deployed Environments (Production, Staging, etc.)

For deployed environments such as production or staging, environment variables should be set directly in the execution environment. This is typically managed through:

*   Kubernetes ConfigMaps and Secrets
*   CI/CD pipeline variables
*   Platform-as-a-Service (PaaS) environment variable settings
*   Other secure configuration management tools

Do **not** deploy `.env` files to these environments. The `.env.example` file can serve as a reference for which variables need to be configured.

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
