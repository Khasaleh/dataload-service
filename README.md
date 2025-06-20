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
    businessId
    role
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

### Example Mutation: Generate Token

```graphql
mutation GenerateToken {
  generateToken(input: {username: "your_username", password: "your_password"}) {
    token
    tokenType
  }
}
```

### Example Mutation: Upload File

(Requires authentication)

File uploads in GraphQL are typically handled using multipart form requests. The `file` argument would be the actual file content, and `input` would be a JSON string for other arguments if using `curl`, or handled by the GraphQL client library.

```graphql
mutation UploadProductFile($file: Upload!, $loadType: String!) {
  uploadFile(file: $file, input: { loadType: $loadType }) {
    sessionId
    originalFilename
    status
    # ... other fields from UploadSessionType
  }
}
```
To execute this with `curl`, it would involve constructing a `multipart/form-data` request. Using a GraphQL client tool or library is generally easier for file uploads.

**Variables for the above mutation:**
```json
{
  "file": null, // The actual file data is sent as a separate part of the multipart request
  "loadType": "products" // Or "brands", "attributes", etc.
}
```

### Tools for Interacting with GraphQL

You can use various tools to interact with the GraphQL API, such as:
*   GraphiQL (available at `/graphql` when the server runs)
*   Insomnia
*   Postman (supports GraphQL requests)
*   Altair GraphQL Client (browser extension or desktop app)
*   Programmatic clients like `httpx` in Python, Apollo Client in JavaScript, etc.
