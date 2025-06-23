# Upload Service

FastAPI + Celery-based upload and processing service.

## Configuration

The application is configured using environment variables, managed by the Pydantic `Settings` class located in `app/core/config.py`.

A comprehensive list of all required and optional environment variables can be found in the `.env.example` file in the root of this repository. This file serves as a template and shows default values where applicable.

**Key Configuration Areas:**

*   **General Application:** `ENVIRONMENT`, `LOG_LEVEL`, `API_PREFIX`, `PROJECT_NAME`.
*   **Database:** Connection parameters (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`, or a full `DATABASE_URL`).
*   **Wasabi S3 Storage:** Credentials and bucket information (`WASABI_ENDPOINT_URL`, `WASABI_ACCESS_KEY`, `WASABI_SECRET_KEY`, `WASABI_BUCKET_NAME`, `WASABI_REGION`).
*   **JWT Authentication:** Secrets and algorithm (`JWT_SECRET`, `JWT_ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`).
*   **Redis:** Connection details and database numbers for Celery and application usage (`REDIS_HOST`, `REDIS_PORT`, `REDIS_DB_ID_MAPPING`, `CELERY_BROKER_DB_NUMBER`, `CELERY_RESULT_BACKEND_DB_NUMBER`).
*   **Celery:** Broker and result backend URLs (can be constructed from Redis settings or provided fully).

For local development, copy `.env.example` to `.env` and fill in your specific values. The application will load these variables from the `.env` file.

For deployed environments (e.g., production, staging), these environment variables should be set directly in the execution environment. This is typically managed through:

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

## CSV Upload Formats

This section describes the expected CSV formats for different `load_type` values used with the `uploadFile` mutation.

### Categories (`load_type: "categories"`)

This CSV format is used to upload category information, including hierarchical structures.

**Key Columns**:

*   `category_path` (Mandatory, String): Defines the category's position in the hierarchy using a `/` delimiter. For example:
    *   `Electronics` (for a top-level category)
    *   `Electronics/Computers` (for a sub-category "Computers" under "Electronics")
    *   `Electronics/Computers/Laptops` (for a sub-sub-category "Laptops")
    The system will create/update parent categories based on these path segments. This path is the unique identifier for a row's target category in the CSV.
*   `name` (Optional, String): The display name for the category level being defined by the full `category_path`. If omitted, the name is derived from the last segment of the `category_path`. For example, if `category_path` is "Electronics/Computers" and `name` is blank, the name "Computers" will be used. If `name` is "Desktop & Laptop Computers", that will be used.
*   `description` (Mandatory, String): A text description for the category.
*   `enabled` (Optional, Boolean: `TRUE` or `FALSE`): Defaults to `TRUE`. Indicates if the category is active/published.
*   `image_name` (Optional, String): Filename or path for the category image.
*   `long_description` (Optional, Text): More detailed description.
*   `order_type` (Optional, String): E.g., "DEFAULT", "SPECIAL".
*   `shipping_type` (Optional, String): E.g., "STANDARD", "OVERSIZE".
*   `active` (Optional, String): Another status field (note: DDL is `varchar(255)`). Consider if `enabled` (boolean) is sufficient or if this has a distinct meaning (e.g., for specific display states beyond simple enabled/disabled).
*   `seo_description` (Optional, String): SEO meta description.
*   `seo_keywords` (Optional, String): Comma-separated SEO keywords.
*   `seo_title` (Optional, String): SEO meta title.
*   `url` (Optional, String): A custom URL slug for the category. If not provided, one might be auto-generated based on the name/path.
*   `position_on_site` (Optional, Integer): For ordering categories.

**Sample File**:
A sample CSV file demonstrating this structure can be found at `sample_data/category.csv`.

**Hierarchical Processing**:
When a row with `category_path` like "L1/L2/L3" is processed:
1.  The system ensures "L1" exists (creating it if necessary with minimal data derived from the path).
2.  Then ensures "L2" exists under "L1" (creating it if necessary).
3.  Finally, "L3" is created or updated under "L2", and the metadata from the CSV row (description, image_name, etc.) is applied to this "L3" category.

*(Documentation for other CSV formats like "brands", "products", etc., would follow here in respective subsections.)*

### Brands (`load_type: "brands"`)

This CSV format is used to upload brand information.

**Key Columns**:

*   `name` (Mandatory, String): The unique display name of the brand. This is used as the primary identifier for the brand within a business context in the CSV.
*   `logo` (Mandatory, String): A URL or path to the brand's logo image.
*   `supplier_id` (Optional, Integer): An identifier for an associated supplier, if applicable.
*   `active` (Optional, String): A status indicator for the brand (e.g., "TRUE", "FALSE", or other status string like "active", "inactive"). The system interprets this as a string that can be used for filtering or display logic.
*   `created_by` (Optional, Integer): User ID (BigInt in DB) of the creator if providing this data via CSV.
*   `created_date` (Optional, Integer): Epoch timestamp (BigInt in DB) of creation if providing via CSV.
*   `updated_by` (Optional, Integer): User ID (BigInt in DB) of the last updater if providing via CSV.
*   `updated_date` (Optional, Integer): Epoch timestamp (BigInt in DB) of last update if providing via CSV.

*(Note: Audit fields like `created_by`, `created_date`, etc., are typically managed by the system automatically upon record creation/update if not supplied in the CSV. If provided, they will be stored as specified.)*

**Sample File**:
A sample CSV file demonstrating this structure can be found at `sample_data/brands.csv`.

### Attributes (`load_type: "attributes"`)

This CSV format is used to upload attribute definitions and their possible values. A single row in this CSV defines one parent attribute and all its associated values.

**Key Columns**:

*   `attribute_name` (Mandatory, String): The name of the attribute itself (e.g., "Color", "Size", "Material"). This is the unique identifier for the attribute within a business.
*   `is_color` (Mandatory, Boolean: `TRUE` or `FALSE`): Set to `TRUE` if this attribute represents color swatches/options, `FALSE` otherwise. This affects how `value_value` is interpreted for its child values.
*   `attribute_active` (Optional, String): The status of the attribute itself (e.g., "ACTIVE", "INACTIVE").
*   `values_name` (Optional, String): A pipe (`|`) separated list of display names for the attribute's values. Example: `Red|Blue|Green` or `Small|Medium|Large`. This field is required if any other `values_*` field (like `value_value`, `img_url`, `values_active`) is provided.
*   `value_value` (Optional, String): A pipe (`|`) separated list of the actual underlying values corresponding to each name in `values_name`.
    *   If `is_color` is `TRUE`, these should be the color codes (e.g., hex like `FF0000|0000FF`).
    *   If `is_color` is `FALSE`, these are the specific values (e.g., `S|M|L`). If a part in this list is empty for a non-color attribute, the system will use the corresponding part from `values_name` as its value.
*   `img_url` (Optional, String): A pipe (`|`) separated list of image URLs, corresponding to each attribute value. Useful for color swatches or visual options.
*   `values_active` (Optional, String): A pipe (`|`) separated list of statuses (e.g., "ACTIVE" or "INACTIVE") for each corresponding attribute value. If a part is empty or the entire field is omitted for a value, that value defaults to "INACTIVE".

**Important Notes on Pipe-Separated Fields**:
*   If provided, `values_name`, `value_value`, `img_url`, and `values_active` must all have the same number of pipe-separated parts if they are not empty. For example, if `values_name` has 3 names, `value_value` (if provided) must also have 3 values.
*   To omit a value for a specific part in an optional pipe-separated list (like `img_url` or `value_value` for non-colors), leave that part empty but include the delimiters. For example, `image1.png||image3.png` means the second value has no image.

**Sample File**:
A sample CSV file demonstrating this structure can be found at `sample_data/attributes.csv`.

### Return Policies (`load_type: "return_policies"`)

This CSV format is used to upload return policy information.

**Key Columns**:

*   `id` (Optional, Integer): The existing database ID of the policy if you are updating a specific record. If this field is blank, or if the ID is not found for the associated business, a new return policy record will be created.
*   `created_date` (Optional, DateTime String): The creation timestamp (e.g., "YYYY-MM-DD HH:MM:SS.ffffff"). If omitted for new policies, this is automatically set by the database.
*   `grace_period_return` (Optional, Integer): The grace period (in days) allowed for returns. This field is typically applicable if `return_policy_type` is "SALES_RETURN_ALLOWED". It will be ignored (nulled) if `return_policy_type` is "SALES_ARE_FINAL".
*   `policy_name` (Optional, String): A descriptive name for the return policy (e.g., "14 Day Full Refund", "No Returns"). This field is typically applicable if `return_policy_type` is "SALES_RETURN_ALLOWED". It will be ignored (nulled) if `return_policy_type` is "SALES_ARE_FINAL".
*   `return_policy_type` (Mandatory, String): Defines the type of policy. Must be one of:
    *   `SALES_RETURN_ALLOWED`: Indicates returns are allowed under specified conditions.
    *   `SALES_ARE_FINAL`: Indicates all sales are final, and no returns are accepted.
*   `time_period_return` (Optional, Integer): The time period (in days) during which a return is allowed. This field is **required if `return_policy_type` is "SALES_RETURN_ALLOWED"**. It will be ignored (nulled) if `return_policy_type` is "SALES_ARE_FINAL".
*   `updated_date` (Optional, DateTime String): The last update timestamp. If omitted, this is automatically set by the database when a record is updated.
*   `business_details_id` (Optional, Integer): The integer ID of the business this policy belongs to. While this can be included in the CSV, the system will primarily associate the policy with the `business_details_id` derived from the authenticated user's context during upload.

**Conditional Fields based on `return_policy_type`**:
*   If `return_policy_type` is "SALES_RETURN_ALLOWED", the `time_period_return` field is mandatory. `policy_name` and `grace_period_return` are typically provided.
*   If `return_policy_type` is "SALES_ARE_FINAL", the system will ensure `policy_name`, `grace_period_return`, and `time_period_return` are stored as `NULL` in the database, regardless of any values provided for them in the CSV for that row.

**Sample File**:
A sample CSV file demonstrating this structure can be found at `sample_data/return_policies.csv`.

### Products (`load_type: "products"`)

This CSV format is used to upload core product information, including links to categories, brands, return policies, and optionally shopping categories. It also handles product specifications and images for non-variant products.

**Key Columns**:

*   `product_name` (Mandatory, String): The display name of the product. Used in conjunction with `business_details_id` as a unique key for upserting.
*   `self_gen_product_id` (Mandatory, String): A unique identifier for the product within the business's scope (e.g., an internal SKU or product code). This is the primary key used for database upserts.
*   `business_details_id` (Mandatory, Integer): The ID of the business this product belongs to. While the uploader's business context is used, this can be in the CSV for explicit assignment or validation.
*   `description` (Mandatory, String): A detailed description of the product.
*   `brand_name` (Mandatory, String): The name of the brand. Must match an existing brand name previously loaded for this business.
*   `category_id` (Mandatory, Integer): The database ID of the category this product belongs to. This category must exist and belong to the business.
*   `shopping_category_name` (Optional, String): The name of the shopping category. If provided, must match an existing shopping category name (which are considered global or pre-defined).
*   `price` (Mandatory, Float): The standard selling price of the product. Must be greater than 0.
*   `sale_price` (Optional, Float): The discounted sale price. Must be non-negative if provided.
*   `cost_price` (Optional, Float): The cost price of the product. Must be non-negative if provided.
*   `quantity` (Mandatory, Integer): The available stock quantity. Must be non-negative.
*   `package_size_length` (Mandatory, Float): Length of the product packaging. Must be greater than 0.
*   `package_size_width` (Mandatory, Float): Width of the product packaging. Must be greater than 0.
*   `package_size_height` (Mandatory, Float): Height of the product packaging. Must be greater than 0.
*   `product_weights` (Mandatory, Float): Weight of the product. Must be greater than 0.
*   `size_unit` (Mandatory, String): Unit for package dimensions (e.g., "CM", "INCHES").
*   `weight_unit` (Mandatory, String): Unit for product weight (e.g., "KG", "POUNDS").
*   `active` (Mandatory, String): Status of the product. Must be "ACTIVE" or "INACTIVE" (case-insensitive, stored as uppercase).
*   `return_type` (Mandatory, String): Defines the return eligibility. Must be one of:
    *   `SALES_RETURN_ALLOWED`: Returns are allowed. `return_fee_type` becomes mandatory.
    *   `SALES_ARE_FINAL`: Sales are final. `return_fee_type` and `return_fee` must be empty/null.
*   `return_fee_type` (Optional, String): Required if `return_type` is "SALES_RETURN_ALLOWED". Must be one of:
    *   `FIXED`: A fixed fee is charged for returns. `return_fee` is the amount.
    *   `PERCENTAGE`: A percentage of the price is charged. `return_fee` is the percentage value (e.g., 10 for 10%).
    *   `FREE`: Returns are free. `return_fee` will be 0.
*   `return_fee` (Optional, Float): The fee amount or percentage, corresponding to `return_fee_type`. Required and must be non-negative if `return_fee_type` is "FIXED" or "PERCENTAGE". Ignored and set to 0 if `return_fee_type` is "FREE". Must be empty/null if `return_type` is "SALES_ARE_FINAL".
*   `url` (Optional, String): A custom URL slug for the product (e.g., "my-product-slug"). If not provided, a slug is auto-generated from `product_name`. Must be lowercase, alphanumeric with hyphens.
*   `video_url` (Optional, String): URL to a product video.
*   `video_thumbnail_url` (Optional, String): URL to a thumbnail image for the product video. **If `video_url` is provided, this field becomes mandatory.** If `video_thumbnail_url` is provided, it will be used for the product's main thumbnail. If `video_url` is provided but `video_thumbnail_url` is missing, the row will fail validation. If `video_url` is empty, and `video_thumbnail_url` is also empty, the product's main image (from the `images` column, if available) will be used as the thumbnail.
*   `images` (Optional, String): Pipe-separated list of image URLs and their main image status. Format: `url1|main_image:true/false|url2|main_image:true/false|...`. Example: `https://cdn.com/img1.jpg|main_image:true|https://cdn.com/img2.jpg|main_image:false`. Images are processed only if `is_child_item` is 0.
*   `specifications` (Optional, String): Pipe-separated list of product specifications. Format: `Name1:Value1|Name2:Value2|...`. Example: `Color:Red|Material:Steel`.
*   `is_child_item` (Mandatory, Integer): Flag indicating if this is a parent product (1) with variants (items/SKUs to be loaded separately) or a standalone/non-variant product (0). If 0, `images` from this CSV are associated with this product. If 1, product-level images from this CSV are ignored (item-level images are expected in an items/SKUs dataload).
*   `ean` (Optional, String): European Article Number.
*   `isbn` (Optional, String): International Standard Book Number.
*   `keywords` (Optional, String): Comma-separated keywords for search or tagging.
*   `mpn` (Optional, String): Manufacturer Part Number.
*   `seo_description` (Optional, String): SEO meta description.
*   `seo_title` (Optional, String): SEO meta title.
*   `upc` (Optional, String): Universal Product Code.

**Database Table Interactions**:
The product dataload interacts with the following tables:
*   `public.products`: Core product data is stored here.
*   `public.product_images`: Populated from the `images` column if `is_child_item` is 0.
*   `public.product_specification`: Populated from the `specifications` column.
*   `public.brands`: `brand_name` is used to look up an existing brand ID.
*   `public.categories`: `category_id` is used directly (existence is verified).
*   `public.shopping_categories`: `shopping_category_name` (if provided) is used to look up an existing shopping category ID.
*   `public.return_policy`: The combination of `return_type`, `return_fee_type`, and `return_fee` is used to look up an existing return policy ID. An exact match must be found for the business.

**Barcode Handling**:
The `products` table has a non-null `barcode` column. Since the CSV does not provide this, the system currently generates a placeholder barcode in the format `BARCODE-{self_gen_product_id}`.

**Sample Mockup Row (Illustrative)**:
```
product_name,self_gen_product_id,business_details_id,description,brand_name,category_id,shopping_category_name,price,sale_price,cost_price,quantity,package_size_length,package_size_width,package_size_height,product_weights,size_unit,weight_unit,active,return_type,return_fee_type,return_fee,url,video_url,images,specifications,is_child_item
Samsung Smart TV 55inch,PROD-001,10,High-resolution smart TV,Samsung,101,Electronics,700,649,500,50,123.5,15.3,77.2,18.5,CENTIMETERS,KILOGRAMS,ACTIVE,SALES_RETURN_ALLOWED,FIXED,20,samsung-smart-tv-55inch,https://cdn.example.com/video.mp4,https://cdn.example.com/img1.jpg|main_image:true,Resolution:4K|HDMI Ports:3,0
```

*(A full sample `products.csv` would be beneficial here if available.)*


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
  "type": "products" /* Valid types include "categories", "brands", "attributes", "return_policies", "products", etc. */
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
