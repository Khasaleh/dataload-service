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
