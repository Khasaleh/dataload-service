import unittest
import json
from jwt_processor_app import app # app is now directly from jwt_processor_app
from app.core.config import settings # To get SECRET_KEY and ALGORITHM for test token generation
from datetime import datetime, timedelta, timezone
from jose import jwt as jose_jwt # Use python-jose for token generation

# Helper function to generate a JWT token using python-jose
def generate_jose_token(payload, secret, algorithm="HS512", expiry_delta_seconds=3600):
    payload_copy = payload.copy()
    # python-jose uses 'exp' and 'iat' directly as epoch timestamps
    iat = datetime.now(timezone.utc)
    exp = iat + timedelta(seconds=expiry_delta_seconds)
    payload_copy["iat"] = iat.timestamp()
    payload_copy["exp"] = exp.timestamp()
    return jose_jwt.encode(payload_copy, secret, algorithm=algorithm)

class AppTestCase(unittest.TestCase):

    def setUp(self):
        self.app_context = app.app_context()
        self.app_context.push() # Pushing app context for settings to be available if app relies on it
        self.client = app.test_client()
        app.testing = True # Set testing flag on the app object itself

        # Use SECRET_KEY and ALGORITHM from the app's settings for consistency
        self.jwt_secret = settings.JWT_SECRET
        self.jwt_algorithm = settings.JWT_ALGORITHM # Should be HS512

        # The integer business_id that load_data_for_business expects for success
        self.valid_extracted_business_id = 11
        # The companyId string that, when parsed, yields self.valid_extracted_business_id
        # Format: "Faz-{userId}-{businessIdFromToken}-{year}-{month}-{random}"
        # Here, businessIdFromToken should be 11
        self.valid_company_id_str = f"Faz-testUser-{self.valid_extracted_business_id}-2024-01-testSuffix"
        self.test_user_id = "testUser" # Corresponds to userId in companyId string

    def tearDown(self):
        self.app_context.pop()

    def test_process_data_success(self):
        # Token payload should now include 'companyId' as a string, and 'sub' (username), 'userId'
        token_payload = {
            "sub": "test_user",
            "userId": self.test_user_id,
            "companyId": self.valid_company_id_str
        }
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json') # No data body needed for this endpoint

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(data['processed_for_business_id'], self.valid_extracted_business_id)
        self.assertIn(str(self.valid_extracted_business_id), data['data']) # Check if data string contains the ID
        self.assertEqual(data['original_company_id_str'], self.valid_company_id_str)
        self.assertEqual(data['processed_for_username'], "test_user")
        self.assertEqual(data['processed_for_user_id'], self.test_user_id)

    def test_missing_authorization_header(self):
        response = self.client.post('/process', content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Authorization header is missing')

    def test_malformed_authorization_header_no_bearer(self):
        token = generate_jose_token({"companyId": "test_id", "sub": "user", "userId": "id"}, self.jwt_secret, self.jwt_algorithm)
        response = self.client.post('/process',
                                 headers={'Authorization': f'Token {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Authorization header must start with Bearer')

    def test_malformed_authorization_header_no_token(self):
        response = self.client.post('/process',
                                 headers={'Authorization': 'Bearer '},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Token not found')

    def test_invalid_signature_token(self):
        # Generate with a different secret
        invalid_secret = "THIS IS NOT THE RIGHT SECRET_djhfgsjdhfg DSFGSDFG DSF"
        token_payload = {"companyId": self.valid_company_id_str, "sub": "user", "userId": self.test_user_id}
        token = generate_jose_token(token_payload, invalid_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        # python-jose might give a more specific error, e.g. "Invalid token (Signature verification failed)"
        self.assertIn('Invalid token', data['error'])
        self.assertIn('Signature verification failed', data['error'])


    def test_expired_token(self):
        token_payload = {"companyId": self.valid_company_id_str, "sub": "user", "userId": self.test_user_id}
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm, expiry_delta_seconds=-3600) # Expired

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Token has expired')

    def test_missing_companyid_in_token(self):
        # Missing companyId, but sub and userId are present
        token_payload = {"sub": "test_user", "userId": self.test_user_id}
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400) # MissingClaimsError should result in 400
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid token: companyId missing')

    def test_malformed_companyid_in_token_not_enough_parts(self):
        # companyId does not have enough parts to extract business_id (parts[2])
        token_payload = {"sub": "test_user", "userId": self.test_user_id, "companyId": "Faz-OnlyTwoParts"}
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400) # MissingClaimsError should result in 400
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid token: companyId format error (not enough parts)')

    def test_malformed_companyid_in_token_part_not_int(self):
        # companyId has enough parts, but parts[2] is not an integer
        token_payload = {"sub": "test_user", "userId": self.test_user_id, "companyId": "Faz-User-NotAnInt-2024-01-Test"}
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400) # MissingClaimsError should result in 400
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid token: companyId format error (parsing business_id part)')

    def test_missing_sub_in_token(self):
        # companyId is fine, but 'sub' (username) is missing
        token_payload = {"userId": self.test_user_id, "companyId": self.valid_company_id_str}
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400) # MissingClaimsError should result in 400
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid token: Essential claims missing or invalid (username, userId, or parsable companyId)')

    def test_missing_userid_in_token(self):
        # companyId is fine, but 'userId' is missing
        token_payload = {"sub": "test_user", "companyId": self.valid_company_id_str}
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400) # MissingClaimsError should result in 400
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid token: Essential claims missing or invalid (username, userId, or parsable companyId)')


    def test_data_processing_failure_for_unknown_business_id(self):
        unknown_extracted_business_id = 99999
        unknown_company_id_str = f"Faz-{self.test_user_id}-{unknown_extracted_business_id}-2024-01-Test"
        token_payload = {
            "sub": "test_user_unknown",
            "userId": self.test_user_id,
            "companyId": unknown_company_id_str
        }
        token = generate_jose_token(token_payload, self.jwt_secret, algorithm=self.jwt_algorithm)

        response = self.client.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 content_type='application/json')
        # process_data_for_business raises an Exception, which the endpoint turns into a 500
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'An unexpected server error occurred')


if __name__ == '__main__':
    unittest.main()