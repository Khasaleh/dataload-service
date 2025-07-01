import jwt
import os
import base64
import pytest
import datetime
import time

# The base64 encoded secret, as if read from environment
MOCK_JWT_SECRET_B64 = "Q0lVSVNlY3JldEtleXF3ZXJ0eXVpb3BBU0RGR0hKS0x6eGN2Ym5tUVdFUlRZVUlPUGFzZGZnaGprbFpYQ1ZCTk1DSVVJU2VjcmV0S2V5cXdlcnR5dWlvcEFTREZHSEpLTHp4Y3Zibm1RV0VSVFlVSU9QYXNkZmdoamtsWlhDVkJOTQ=="
DECODED_JWT_SECRET = "CIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNMCIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNM"

# The NEW problematic token provided by the user
USER_PROVIDED_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"

@pytest.fixture
def mock_env_secret(monkeypatch):
    monkeypatch.setenv("JWT_SECRET", MOCK_JWT_SECRET_B64)

def test_jwt_secret_decoding_from_env(mock_env_secret):
    secret_b64 = os.environ["JWT_SECRET"]
    secret_decoded_bytes = base64.b64decode(secret_b64)
    secret_decoded_str = secret_decoded_bytes.decode('utf-8')
    assert secret_decoded_str == DECODED_JWT_SECRET
    assert secret_decoded_str.startswith("CIUISecretKey")

def test_user_token_decoding_unverified():
    """Tests that the user-provided token can be decoded without verification and checks its claims."""
    payload = jwt.decode(USER_PROVIDED_TOKEN, options={"verify_signature": False, "verify_exp": False})
    assert payload["sub"] == "muna_Store"
    assert payload["companyId"] == "Faz-13-11-2024-08-11739"
    assert payload["userId"] == 13
    assert "HS512" == jwt.get_unverified_header(USER_PROVIDED_TOKEN)["alg"]

def test_new_user_token_is_not_expired_but_signature_invalid():
    """
    Confirms the new user-provided token is NOT expired, but still fails signature verification.
    PyJWT checks signature before expiry, so InvalidSignatureError is expected.
    """
    # First, assert it's not expired by trying to decode with verify_exp=True and verify_signature=False
    # This specific combination isn't directly supported by options in a single call in old pyjwt,
    # so we'll decode without signature check and then manually check expiry.
    payload_no_sig_check = jwt.decode(USER_PROVIDED_TOKEN, options={"verify_signature": False})
    token_exp = datetime.datetime.fromtimestamp(payload_no_sig_check["exp"], tz=datetime.timezone.utc)
    assert token_exp > datetime.datetime.now(tz=datetime.timezone.utc), "Token should not be expired"

    # Now, assert that with signature verification, it raises InvalidSignatureError
    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(USER_PROVIDED_TOKEN, DECODED_JWT_SECRET, algorithms=["HS512"])

def test_generated_token_verification():
    """Tests that a newly generated token with the DECODED_JWT_SECRET can be verified."""
    # Generate a new token that expires in 1 hour
    iat = datetime.datetime.now(tz=datetime.timezone.utc)
    exp = iat + datetime.timedelta(hours=1)
    payload = {
        "sub": "testuser",
        "userId": 123,
        "iat": iat,
        "exp": exp
    }
    generated_token = jwt.encode(payload, DECODED_JWT_SECRET, algorithm="HS512")

    # Attempt to decode it
    decoded_payload = jwt.decode(generated_token, DECODED_JWT_SECRET, algorithms=["HS512"])
    assert decoded_payload["sub"] == "testuser"
    assert decoded_payload["userId"] == 123

def test_user_token_signature_failure_even_if_not_expired():
    """
    This test checks if the user token *still* fails signature verification
    even if we tell pyjwt to ignore the expiration. This helps isolate
    if the problem is *only* expiration or also a signature mismatch.
    """
    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(USER_PROVIDED_TOKEN, DECODED_JWT_SECRET, algorithms=["HS512"], options={"verify_exp": False})

def test_another_secret_fails_generated_token():
    """Verify that a token generated with the correct secret fails with a wrong secret."""
    payload = {
        "sub": "testuser",
        "iat": datetime.datetime.now(tz=datetime.timezone.utc),
        "exp": datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=1)
    }
    correct_secret = "correct_secret_key"
    wrong_secret = "wrong_secret_key"

    generated_token = jwt.encode(payload, correct_secret, algorithm="HS512")

    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(generated_token, wrong_secret, algorithms=["HS512"])

    # And verify it works with the correct secret
    decoded_payload = jwt.decode(generated_token, correct_secret, algorithms=["HS512"])
    assert decoded_payload["sub"] == "testuser"
