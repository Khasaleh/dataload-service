import pytest
from fastapi.testclient import TestClient
from typing import Generator, Any

from app.main import app # Import the FastAPI app instance
from app.dependencies.auth import get_current_user # Import the actual dependency

# Mock user data (can be shared or defined per test module)
MOCK_USER_BUSINESS_ID = 456
MOCK_USER_ID = "test_user_me_id"
MOCK_USERNAME = "me_user"
MOCK_COMPANY_ID_STR = f"FAZ-{MOCK_USER_ID}-{MOCK_USER_BUSINESS_ID}-2024-02-randomABC"
MOCK_USER_ROLES = ["editor"]

def mock_get_current_user_for_me_endpoint():
    return {
        "user_id": MOCK_USER_ID,
        "username": MOCK_USERNAME,
        "business_id": MOCK_USER_BUSINESS_ID,
        "company_id_str": MOCK_COMPANY_ID_STR,
        "roles": MOCK_USER_ROLES,
    }

@pytest.fixture(scope="module")
def client() -> Generator[TestClient, Any, None]:
    with TestClient(app) as c:
        yield c

def test_read_users_me(client: TestClient):
    # Override the dependency for this test
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_me_endpoint

    response = client.get("/api/v1/users/me") # Path based on router prefix in main.py and users_api.py

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == MOCK_USER_ID
    assert data["username"] == MOCK_USERNAME
    assert data["business_id"] == MOCK_USER_BUSINESS_ID
    assert data["roles"] == MOCK_USER_ROLES
    assert data["company_id_str"] == MOCK_COMPANY_ID_STR

    # Clean up dependency override
    del app.dependency_overrides[get_current_user]

def test_read_users_me_unauthenticated(client: TestClient):
    # Ensure no override is active or override with a dependency that raises 401
    if get_current_user in app.dependency_overrides:
        del app.dependency_overrides[get_current_user]

    # To properly test unauthenticated, we'd need oauth2_scheme to fail.
    # TestClient by default doesn't run the actual OAuth2 scheme if not provided a token.
    # So, we can expect a 401 if the dependency is not met or raises HTTPException.
    # The OAuth2PasswordBearer dependency itself will raise a 401 if no token is sent.

    response = client.get("/api/v1/users/me")
    assert response.status_code == 401 # Expect 401 as no token is provided by default
    assert "Not authenticated" in response.json().get("detail", "") or \
           "Could not validate credentials" in response.json().get("detail", "")

    # Note: If get_current_user was mocked to return None, the endpoint might return 500
    # if it doesn't handle None from the dependency gracefully (though our /me endpoint does check).
    # The default behavior of OAuth2PasswordBearer is to raise 401 if token is missing/invalid.
    # The exact detail message might vary.
    # If you have a specific way to simulate "no token" that results in 403 from your auth logic, test that.
    # For now, relying on default TestClient behavior + OAuth2PasswordBearer for 401.
    # If `get_current_user` itself raised HTTPException for unauth, that would also be 401.
    # Our `get_current_user` raises 401 for JWTError, and 403 for missing claims.
    # A missing token header will lead to a 401 from OAuth2PasswordBearer.
    # An invalid token that passes OAuth2PasswordBearer but fails jwt.decode in get_current_user will also lead to 401.
    # An invalid token that decodes but misses claims will lead to 403 from get_current_user.
    # This test covers the "no token" case.

    # Test with an invalid token format that would fail OAuth2PasswordBearer or JWT decoding
    response_invalid_token = client.get("/api/v1/users/me", headers={"Authorization": "Bearer invalidtokenstring"})
    assert response_invalid_token.status_code == 401 # Expect 401 from get_current_user due to JWTError
    assert "Could not validate credentials" in response_invalid_token.json().get("detail", "")

    # Test with a token that decodes but is missing essential claims (leading to 403 from get_current_user)
    # This requires generating such a token or more complex mocking of jwt.decode.
    # For simplicity, this specific sub-case (403 from get_current_user) is omitted here but important for full coverage.
