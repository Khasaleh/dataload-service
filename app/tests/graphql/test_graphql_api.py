import pytest
import asyncio
import io
from unittest.mock import patch, MagicMock, call, ANY
import uuid
import datetime
from datetime import timedelta # For checking token expiry
import time # For checking token expiry
from jose import jwt # For decoding JWTs

from fastapi.testclient import TestClient

from app.main import app # Main FastAPI application with GraphQL router
from app.dependencies.auth import get_current_user, SECRET_KEY, ALGORITHM # For overriding and token decoding
from app.graphql_types import UploadSessionType, UserType, TokenResponseType
from app.db.models import UploadSessionOrm # Import the ORM model for spec

client = TestClient(app)

# --- Mock Data ---
# Updated mock user structure to match new JWT claims and UserType
MOCK_USER_ADMIN = {
    "username": "admin_gql_user",
    "user_id": "admin_user_id_123", # Can be string or int depending on what UserType expects for ID
    "business_id": "biz_gql_test",
    "roles": ["ROLE_ADMIN", "ROLE_EDITOR"]
}
MOCK_USER_OTHER_BIZ = {
    "username": "other_biz_user",
    "user_id": "other_user_id_456",
    "business_id": "biz_other",
    "roles": ["ROLE_USER"]
}

# --- Helper for Dependency Overrides ---
def override_get_current_user(user_data: dict):
    app.dependency_overrides[get_current_user] = lambda: user_data

def clear_get_current_user_override():
    if get_current_user in app.dependency_overrides:
        del app.dependency_overrides[get_current_user]

# --- Query Tests ---
def test_query_me_authenticated():
    override_get_current_user(MOCK_USER_ADMIN)
    query = """
        query {
            me {
                userId
                username
                businessId
                roles
            }
        }
    """
    response = client.post("/graphql", json={"query": query})
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data["data"]["me"]["userId"] == MOCK_USER_ADMIN["user_id"]
    assert data["data"]["me"]["username"] == MOCK_USER_ADMIN["username"]
    assert data["data"]["me"]["businessId"] == MOCK_USER_ADMIN["business_id"]
    assert data["data"]["me"]["roles"] == MOCK_USER_ADMIN["roles"]

def test_query_me_unauthenticated():
    # The get_context expects Optional[dict] = Depends(get_current_user)
    # and get_current_user raises HTTPException if token is invalid/missing.
    # FastAPI's TestClient and Depends system will simulate this.
    # If get_current_user raises an HTTPException, it's caught by FastAPI
    # before Strawberry's context getter even fully resolves with a None user.
    # The GraphQL endpoint itself might not be reached or might return a generic error
    # if the dependency fails. Strawberry typically expects None or valid user from context.
    # For this test, we assume the current get_current_user raises error if no token.

    # To properly test "unauthenticated" for a GraphQL query where the field itself
    # handles a None user from context, we'd need get_current_user to be optional
    # (e.g. using auto_error=False in OAuth2PasswordBearer and returning None).
    # Given current setup, an invalid/missing token leads to FastAPI level 401/403.
    # This test as is will likely fail if the /graphql endpoint itself is protected
    # by a strict top-level Depends(get_current_user) not on the context_getter.
    # However, our get_context has Depends(get_current_user), so an invalid token
    # will result in an error response from FastAPI's auth handling.

    # Let's simulate by not providing auth headers, expecting FastAPI's auth to fail.
    query = """
        query {
            me {
                businessId
                role
            }
        }
    """
    # No headers means get_current_user's oauth2_scheme will fail.
    response = client.post("/graphql", json={"query": query})
    assert response.status_code == 401 # Or 403 depending on OAuth2PasswordBearer setup
    # The response will be FastAPI's error, not a GraphQL error with data:null.
    assert "Not authenticated" in response.json().get("detail", "") # Common FastAPI error

@patch('app.graphql_queries.get_db_session_sync') # Patched to actual DB session getter
def test_query_upload_session_found(mock_get_db_session_sync):
    override_get_current_user(MOCK_USER_ADMIN)
    mock_session_id_str = "sess_found_db"

    # Prepare a mock ORM object that query().filter().first() would return
    mock_orm_session = MagicMock()
    mock_orm_session.session_id = mock_session_id_str
    mock_orm_session.business_id = MOCK_USER_ADMIN["business_id"]
    mock_orm_session.load_type = "products"
    mock_orm_session.original_filename = "f_db.csv"
    mock_orm_session.wasabi_path = "p/f_db.csv"
    mock_orm_session.status = "completed_from_db"
    mock_orm_session.details = "DB OK"
    mock_orm_session.record_count = 100
    mock_orm_session.error_count = 1
    mock_orm_session.created_at = datetime.datetime.utcnow()
    mock_orm_session.updated_at = datetime.datetime.utcnow()
    # Make __table__.columns accessible for the dict comprehension in resolver
    mock_orm_session.__table__ = MagicMock()
    # Corrected way to get field names from Strawberry type
    mock_orm_session.__table__.columns = [MagicMock(name=f.name) for f in UploadSessionType.__strawberry_definition__.fields]


    mock_db_session = MagicMock()
    mock_db_session.query().filter().first.return_value = mock_orm_session
    mock_get_db_session_sync.return_value = mock_db_session

    query = f"""
        query {{
                uploadSession(sessionId: "{mock_session_id_str}") {{
                sessionId
                businessId
                status
                originalFilename
            }}
        }}
    """
    response = client.post("/graphql", json={"query": query})
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data["data"]["uploadSession"]["sessionId"] == mock_session_id_str
    assert data["data"]["uploadSession"]["businessId"] == MOCK_USER_ADMIN["business_id"]
    assert data["data"]["uploadSession"]["status"] == "completed_from_db"
    mock_get_db_session_sync.assert_called_once_with(business_id=MOCK_USER_ADMIN["business_id"])
    mock_db_session.query().filter().first.assert_called_once()


@patch('app.graphql_queries.get_db_session_sync') # Patched to actual DB session getter
def test_query_upload_session_not_found(mock_get_db_session_sync):
    override_get_current_user(MOCK_USER_ADMIN)
    mock_session_id_str = "sess_not_found_db"

    mock_db_session = MagicMock()
    mock_db_session.query().filter().first.return_value = None # Simulate not found
    mock_get_db_session_sync.return_value = mock_db_session

    query = f"""
        query {{
            uploadSession(sessionId: "{mock_session_id_str}") {{
                sessionId
            }}
        }}
    """
    response = client.post("/graphql", json={"query": query})
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data["data"]["uploadSession"] is None
    mock_get_db_session_sync.assert_called_once_with(business_id=MOCK_USER_ADMIN["business_id"])
    mock_db_session.query().filter().first.assert_called_once()

@patch('app.graphql_queries.get_db_session_sync') # Patched to actual DB session getter
def test_query_upload_session_unauthorized(mock_get_db_session_sync):
    # User from MOCK_USER_OTHER_BIZ tries to access a session.
    # The resolver filters by UploadSessionOrm.business_id == user_business_id.
    # So, if the session_id exists but for another biz, .first() will return None.
    override_get_current_user(MOCK_USER_OTHER_BIZ)
    mock_session_id_str = "sess_for_admin_biz_db" # A session ID that wouldn't match MOCK_USER_OTHER_BIZ

    mock_db_session = MagicMock()
    mock_db_session.query().filter().first.return_value = None # Simulate not found due to business_id mismatch
    mock_get_db_session_sync.return_value = mock_db_session

    query = f"""
        query {{
            uploadSession(sessionId: "{mock_session_id_str}") {{
                sessionId
            }}
        }}
    """
    response = client.post("/graphql", json={"query": query})
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data["data"]["uploadSession"] is None
    mock_get_db_session_sync.assert_called_once_with(business_id=MOCK_USER_OTHER_BIZ["business_id"])
    mock_db_session.query().filter().first.assert_called_once()


@patch('app.graphql_queries.get_db_session_sync') # Patched to actual DB session getter
def test_query_upload_sessions_by_business(mock_get_db_session_sync):
    override_get_current_user(MOCK_USER_ADMIN)

    # Prepare mock ORM objects
    mock_orm_session1 = MagicMock()
    mock_orm_session1.session_id = "s1_db"
    mock_orm_session1.business_id = MOCK_USER_ADMIN["business_id"]
    # ... other fields for session1 ...
    mock_orm_session1.load_type="products"; mock_orm_session1.original_filename="f1.csv"; mock_orm_session1.wasabi_path="p/f1.csv"; mock_orm_session1.status="completed"; mock_orm_session1.created_at=datetime.datetime.utcnow(); mock_orm_session1.updated_at=datetime.datetime.utcnow(); mock_orm_session1.details=None; mock_orm_session1.record_count=None; mock_orm_session1.error_count=None
    mock_orm_session1.__table__ = MagicMock(); mock_orm_session1.__table__.columns = [MagicMock(name=f.name) for f in UploadSessionType.__strawberry_definition__.fields]


    mock_orm_session2 = MagicMock()
    mock_orm_session2.session_id = "s2_db"
    mock_orm_session2.business_id = MOCK_USER_ADMIN["business_id"]
    # ... other fields for session2 ...
    mock_orm_session2.load_type="brands"; mock_orm_session2.original_filename="f2.csv"; mock_orm_session2.wasabi_path="p/f2.csv"; mock_orm_session2.status="pending"; mock_orm_session2.created_at=datetime.datetime.utcnow(); mock_orm_session2.updated_at=datetime.datetime.utcnow(); mock_orm_session2.details=None; mock_orm_session2.record_count=None; mock_orm_session2.error_count=None
    mock_orm_session2.__table__ = MagicMock(); mock_orm_session2.__table__.columns = [MagicMock(name=f.name) for f in UploadSessionType.__strawberry_definition__.fields]

    mock_db_session = MagicMock()
    mock_db_session.query().filter().order_by().offset().limit().all.return_value = [mock_orm_session1, mock_orm_session2]
    mock_get_db_session_sync.return_value = mock_db_session

    query = """
        query {
            uploadSessionsByBusiness(skip: 0, limit: 5) {
                sessionId
                status
            }
        }
    """
    response = client.post("/graphql", json={"query": query})
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["uploadSessionsByBusiness"]) == 2
    assert data["data"]["uploadSessionsByBusiness"][0]["sessionId"] == "s1_db"
    mock_get_db_session_sync.assert_called_once_with(business_id=MOCK_USER_ADMIN["business_id"])
    mock_db_session.query().filter().order_by().offset(0).limit(5).all.assert_called_once()


# --- Mutation Tests ---
@patch('app.graphql_mutations.authenticate_user_placeholder')
def test_mutation_generate_token_success(mock_authenticate_user):
    # Mock the return value of authenticate_user_placeholder
    # This should now be the full user details dict
    user_auth_details = {
        "username": "testuser",
        "user_id": "user_123",
        "business_id": "biz_789",
        "roles": ["ROLE_USER"]
    }
    mock_authenticate_user.return_value = user_auth_details

    mutation = """
        mutation($input: GenerateTokenInput!) {
            generateToken(input: $input) {
                token
                tokenType
                refreshToken
            }
        }
    """
    variables = {"input": {"username": "testuser", "password": "password"}} # Password matches placeholder
    response = client.post("/graphql", json={"query": mutation, "variables": variables})

    assert response.status_code == 200
    data = response.json().get("data", {}).get("generateToken")
    assert data is not None
    assert "token" in data
    assert data["tokenType"] == "bearer"
    assert "refreshToken" in data
    assert data["refreshToken"].startswith("mock-rt-testuser-") # Check prefix

    mock_authenticate_user.assert_called_once_with(username="testuser", password="password")

    # Decode the access token and verify claims
    decoded_token = jwt.decode(data["token"], SECRET_KEY, algorithms=[ALGORITHM])
    assert decoded_token["sub"] == user_auth_details["username"]
    assert decoded_token["userId"] == user_auth_details["user_id"]
    assert decoded_token["companyId"] == user_auth_details["business_id"]
    assert len(decoded_token["role"]) == 1
    assert decoded_token["role"][0]["authority"] == user_auth_details["roles"][0]

    # Check expiry (iat is present, exp is in the future)
    assert "iat" in decoded_token
    assert "exp" in decoded_token
    assert decoded_token["exp"] > time.time()
    # ACCESS_TOKEN_EXPIRE_MINUTES is from app.graphql_mutations, check if it's roughly correct
    # This requires importing ACCESS_TOKEN_EXPIRE_MINUTES or knowing its value.
    # For now, just checking it's in the future is fine.

@patch('app.graphql_mutations.authenticate_user_placeholder')
def test_mutation_generate_token_failure(mock_authenticate_user):
    mock_authenticate_user.return_value = None # Simulate invalid credentials

    mutation = """
        mutation($input: GenerateTokenInput!) {
            generateToken(input: $input) {
                token # Querying for token, but expect it to be null or error
            }
        }
    """
    variables = {"input": {"username": "wronguser", "password": "badpassword"}}
    response = client.post("/graphql", json={"query": mutation, "variables": variables})

    assert response.status_code == 200 # GraphQL itself returns 200
    data = response.json()
    # Depending on how the resolver handles None from authenticate_user:
    # It might return data: { generateToken: null } if Optional
    # Or an error in the "errors" list if it raises GraphQLError
    # The current generateToken raises GraphQLError("Invalid username or password.")
    assert data.get("data") is None or data.get("data", {}).get("generateToken") is None
    assert len(data.get("errors", [])) == 1
    assert "Invalid username or password." in data["errors"][0]["message"]
    mock_authenticate_user.assert_called_once_with(username="wronguser", password="badpassword")


@patch('app.graphql_mutations.get_db_session_sync') # Patch DB session for upload_file
@patch('app.graphql_mutations.upload_to_wasabi')
@patch('app.graphql_mutations.CELERY_TASK_MAP')
@patch('uuid.uuid4') # To control session_id generation
def test_mutation_upload_file_success(
    mock_uuid4, mock_celery_task_map, mock_upload_wasabi, mock_get_db_session_sync_in_mutations
):
    override_get_current_user(MOCK_USER_ADMIN)

    mock_generated_session_id = "fixed_session_uuid_db"
    mock_uuid4.return_value = mock_generated_session_id

    # Configure the mock DB session for create
    mock_db_session_create = MagicMock()
    # When get_db_session_sync is called the first time (for creating session)
    # For the Wasabi failure case, we might need a side_effect if get_db_session_sync is called multiple times
    mock_get_db_session_sync_in_mutations.return_value = mock_db_session_create

    # Capture the instance passed to add() and simulate refresh()
    # This mock_upload_session_orm_instance will be what db.refresh() acts upon.
    mock_upload_session_orm_instance = None
    def capture_add(instance):
        nonlocal mock_upload_session_orm_instance
        mock_upload_session_orm_instance = instance
        # Simulate DB setting the 'id' (PK) on add/flush, not through refresh typically for new objects.
        # However, refresh is used to get all DB-generated values.
        # For this test, we primarily care that the ORM object is constructed correctly.
        # The resolver converts it to dict for UploadSessionType.
        # Ensure the instance passed to UploadSessionType has all fields.
        # The resolver uses {c.name: getattr(orm_instance, c.name) ...}
        # So, the mock_upload_session_orm_instance needs these attributes.
        # The attributes are set from session_data_for_orm in the resolver.
        # We need to ensure `id` (PK) is set if it's part of UploadSessionType.
        # UploadSessionType does not have 'id' (PK), it has 'session_id' (UUID).
        # So, refresh mainly ensures other DB defaults or triggers run. We can just pass here.
        pass

    mock_db_session_create.add.side_effect = capture_add
    # mock_db_session_create.refresh = MagicMock() # refresh is called on the instance

    # Mock Celery task dispatch (same as before)

    # Mock Celery task dispatch
    mock_celery_task_delay = MagicMock(return_value=MagicMock(id="celery_task_123"))
    mock_celery_task_fn = MagicMock()
    mock_celery_task_fn.delay = mock_celery_task_delay
    mock_celery_task_map.get.return_value = mock_celery_task_fn # .get() on the map returns the task function

    file_content = b"header,col\nval1,val2"
    file_name = "test_upload.csv"
    load_type = "products"

    operations = {
        "query": """
            mutation ($file: Upload!, $inputType: UploadFileInput!) {
                uploadFile(file: $file, input: $inputType) {
                    sessionId
                    originalFilename
                    loadType
                    status
                    businessId
                    wasabiPath
                }
            }
        """,
        "variables": {
            "file": None, # Standard practice for multipart requests
            "inputType": {"loadType": load_type}
        }
    }
    file_map = {"file": (file_name, io.BytesIO(file_content), "text/csv")}

    response = client.post("/graphql", data={"operations": str(operations).replace("'", "\"")}, files=file_map)
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data["data"]["uploadFile"]["sessionId"] == mock_session_id
    assert data["data"]["uploadFile"]["originalFilename"] == file_name
    assert data["data"]["uploadFile"]["loadType"] == load_type
    assert data["data"]["uploadFile"]["status"] == "pending"
    assert data["data"]["uploadFile"]["businessId"] == MOCK_USER_ADMIN["business_id"]

    expected_wasabi_path = f"uploads/{MOCK_USER_ADMIN['business_id']}/{mock_session_id}/{load_type}/{file_name}"
    assert data["data"]["uploadFile"]["wasabiPath"] == expected_wasabi_path

    mock_upload_wasabi.assert_called_once_with(bucket=ANY, path=expected_wasabi_path, file_obj=ANY)
    mock_celery_task_map.get.assert_called_once_with(load_type)
    mock_celery_task_delay.assert_called_once_with(
        business_id=MOCK_USER_ADMIN["business_id"],
        session_id=mock_generated_session_id,
        wasabi_file_path=expected_wasabi_path,
        original_filename=file_name
    )

    # Verify DB interactions for create
    mock_db_session_create.add.assert_called_once()
    # Check attributes of the added instance if needed, using mock_upload_session_orm_instance
    if mock_upload_session_orm_instance:
        assert mock_upload_session_orm_instance.session_id == mock_generated_session_id
        assert mock_upload_session_orm_instance.business_id == MOCK_USER_ADMIN["business_id"]
        assert mock_upload_session_orm_instance.status == "pending"
    mock_db_session_create.commit.assert_called_once()
    mock_db_session_create.refresh.assert_called_once_with(mock_upload_session_orm_instance)


def test_mutation_upload_file_invalid_load_type():
    override_get_current_user(MOCK_USER_ADMIN)
    file_content = b"header,col\nval1,val2"
    file_name = "test_upload.csv"

    operations = {
        "query": "mutation ($file: Upload!, $inputType: UploadFileInput!) { uploadFile(file: $file, input: $inputType) { sessionId } }",
        "variables": {"file": None, "inputType": {"loadType": "nonexistent_type"}}
    }
    file_map = {"file": (file_name, io.BytesIO(file_content), "text/csv")}

    response = client.post("/graphql", data={"operations": str(operations).replace("'", "\"")}, files=file_map)
    clear_get_current_user_override()

    assert response.status_code == 200 # GraphQL errors usually return 200 OK with an errors block
    data = response.json()
    assert data.get("data") is None or data.get("data", {}).get("uploadFile") is None
    assert len(data.get("errors", [])) == 1
    assert "Invalid load type" in data["errors"][0]["message"]


def test_mutation_upload_file_not_csv():
    override_get_current_user(MOCK_USER_ADMIN)
    file_content = b"this is not a csv"
    file_name = "test_upload.txt" # Invalid extension

    operations = {
        "query": "mutation ($file: Upload!, $inputType: UploadFileInput!) { uploadFile(file: $file, input: $inputType) { sessionId } }",
        "variables": {"file": None, "inputType": {"loadType": "products"}}
    }
    file_map = {"file": (file_name, io.BytesIO(file_content), "text/plain")}

    response = client.post("/graphql", data={"operations": str(operations).replace("'", "\"")}, files=file_map)
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data.get("data") is None or data.get("data", {}).get("uploadFile") is None
    assert len(data.get("errors", [])) == 1
    assert "Invalid file type. Only CSV files are allowed" in data["errors"][0]["message"]

@patch('app.graphql_mutations.get_db_session_sync')
@patch('app.graphql_mutations.upload_to_wasabi')
# Removed import from here, it should be at the top of the file.

@patch('uuid.uuid4')
def test_mutation_upload_file_wasabi_failure_db_update(mock_uuid4, mock_upload_wasabi, mock_get_db_session_sync_in_mutations):
    override_get_current_user(MOCK_USER_ADMIN)

    mock_generated_session_id = "wasabi_fail_session_uuid"
    mock_uuid4.return_value = mock_generated_session_id

    # --- Mock setup for the two DB interactions ---
    # 1. Initial session creation (success)
    mock_db_session_create = MagicMock(name="create_session")
    created_orm_instance = None
    def capture_add_for_create(instance):
        nonlocal created_orm_instance
        created_orm_instance = instance
    mock_db_session_create.add.side_effect = capture_add_for_create

    # 2. Session update after Wasabi failure (this mock will be returned on the second call)
    mock_db_session_update = MagicMock(name="update_session")
    # This is the instance that query().filter().first() should return for the update path
    mock_session_to_be_updated = MagicMock(spec=UploadSessionOrm) # Use imported UploadSessionOrm
    mock_session_to_be_updated.session_id = mock_generated_session_id # Ensure it has the ID
    mock_db_session_update.query().filter().first.return_value = mock_session_to_be_updated

    # get_db_session_sync will be called twice: once for create, once for update
    mock_get_db_session_sync_in_mutations.side_effect = [mock_db_session_create, mock_db_session_update]

    # --- Setup Wasabi mock to fail ---
    mock_upload_wasabi.side_effect = Exception("S3 Critical Fail")

    file_content = b"header,col\nval1,val2"
    file_name = "test_upload_wasabi_fail.csv"
    operations = {
        "query": "mutation ($file: Upload!, $inputType: UploadFileInput!) { uploadFile(file: $file, input: $inputType) { sessionId } }",
        "variables": {"file": None, "inputType": {"loadType": "products"}}
    }
    file_map = {"file": (file_name, io.BytesIO(file_content), "text/csv")}

    response = client.post("/graphql", data={"operations": str(operations).replace("'", "\"")}, files=file_map)
    clear_get_current_user_override()

    assert response.status_code == 200
    data = response.json()
    assert data.get("data") is None or data.get("data", {}).get("uploadFile") is None
    assert len(data.get("errors", [])) == 1
    assert "Failed to upload file to Wasabi: S3 Critical Fail" in data["errors"][0]["message"]

    # Verify initial DB creation was attempted and completed
    mock_db_session_create.add.assert_called_once()
    mock_db_session_create.commit.assert_called_once()
    mock_db_session_create.refresh.assert_called_once_with(created_orm_instance)

    # Verify DB update was attempted for failure status
    mock_db_session_update.query(app.db.models.UploadSessionOrm).filter(app.db.models.UploadSessionOrm.session_id == mock_generated_session_id).first.assert_called_once()
    assert mock_session_to_be_updated.status == "failed_wasabi_upload"
    assert "Failed to upload file to Wasabi: S3 Critical Fail" in mock_session_to_be_updated.details
    mock_db_session_update.commit.assert_called_once()


# --- Refresh Token Mutation Tests ---

# To test refreshToken, we need to control the _MOCK_USERS_DB in app.graphql_mutations
# or mock the part of the refreshToken resolver that uses it.
# Patching _MOCK_USERS_DB directly for the scope of a test is cleaner.

@patch('app.graphql_mutations._MOCK_USERS_DB')
def test_mutation_refresh_token_success(mock_users_db_in_mutations):
    # Setup the mock DB that the refreshToken resolver will see
    original_user_details = {
        "username": "testuser", "hashed_password": "password",
        "user_id": "user_123", "business_id": "biz_789",
        "roles": ["ROLE_USER"], "disabled": False
    }
    mock_users_db_in_mutations.get.return_value = original_user_details # Simulate finding "testuser"

    # This refresh token should be "valid" by the placeholder logic in refreshToken resolver
    # because it starts with "mock-rt-testuser"
    input_refresh_token = f"mock-rt-testuser-{uuid.uuid4()}"

    mutation = """
        mutation($input: RefreshTokenInput!) {
            refreshToken(input: $input) {
                token
                tokenType
                refreshToken
            }
        }
    """
    variables = {"input": {"refreshToken": input_refresh_token}}
    response = client.post("/graphql", json={"query": mutation, "variables": variables})

    assert response.status_code == 200
    data = response.json().get("data", {}).get("refreshToken")
    assert data is not None

    assert "token" in data
    assert data["tokenType"] == "bearer"
    assert "refreshToken" in data
    assert data["refreshToken"] != input_refresh_token # Check rotation
    assert data["refreshToken"].startswith(f"mock-rt-{original_user_details['username']}-")

    # Decode the new access token
    new_decoded_access_token = jwt.decode(data["token"], SECRET_KEY, algorithms=[ALGORITHM])
    assert new_decoded_access_token["sub"] == original_user_details["username"]
    assert new_decoded_access_token["userId"] == original_user_details["user_id"]
    assert new_decoded_access_token["companyId"] == original_user_details["business_id"]
    assert len(new_decoded_access_token["role"]) == 1
    assert new_decoded_access_token["role"][0]["authority"] == original_user_details["roles"][0]
    assert new_decoded_access_token["exp"] > time.time()


def test_mutation_refresh_token_invalid():
    mutation = """
        mutation($input: RefreshTokenInput!) {
            refreshToken(input: $input) {
                token
            }
        }
    """
    variables = {"input": {"refreshToken": "invalid-or-expired-token"}}
    response = client.post("/graphql", json={"query": mutation, "variables": variables})

    assert response.status_code == 200
    data = response.json()
    assert data.get("data") is None or data.get("data", {}).get("refreshToken") is None
    assert len(data.get("errors", [])) == 1
    assert "Invalid or expired refresh token." in data["errors"][0]["message"]


@patch('app.graphql_mutations._MOCK_USERS_DB')
def test_mutation_refresh_token_disabled_user(mock_users_db_in_mutations):
    disabled_user_details = {
        "username": "testuser", "hashed_password": "password",
        "user_id": "user_123", "business_id": "biz_789",
        "roles": ["ROLE_USER"], "disabled": True # User is disabled
    }
    mock_users_db_in_mutations.get.return_value = disabled_user_details

    input_refresh_token = f"mock-rt-testuser-{uuid.uuid4()}" # Token for "testuser"

    mutation = """
        mutation($input: RefreshTokenInput!) {
            refreshToken(input: $input) {
                token
            }
        }
    """
    variables = {"input": {"refreshToken": input_refresh_token}}
    response = client.post("/graphql", json={"query": mutation, "variables": variables})

    assert response.status_code == 200
    data = response.json()
    assert data.get("data") is None or data.get("data", {}).get("refreshToken") is None
    assert len(data.get("errors", [])) == 1
    assert "User account is disabled." in data["errors"][0]["message"]
