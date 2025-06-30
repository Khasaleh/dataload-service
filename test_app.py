import unittest
import json
from jwt_processor_app import app, JWT_SECRET  # Updated import
from datetime import datetime, timedelta, timezone
import jwt # PyJWT library

# Helper function to generate a JWT token
def generate_token(payload, secret, algorithm="HS256", expiry_delta_seconds=3600):
    payload_copy = payload.copy()
    payload_copy["iat"] = datetime.now(timezone.utc)
    payload_copy["exp"] = datetime.now(timezone.utc) + timedelta(seconds=expiry_delta_seconds)
    return jwt.encode(payload_copy, secret, algorithm=algorithm)

class AppTestCase(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True
        # Use the same JWT_SECRET as the app for generating test tokens
        self.jwt_secret = JWT_SECRET
        self.valid_business_id = "Faz-13-11-2024-08-11739" # Must match one in app.py's load_data_for_business

    def test_process_data_success_with_businessid(self):
        token = generate_token({"businessId": self.valid_business_id, "sub": "test_user"}, self.jwt_secret)
        response = self.app.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertIn(self.valid_business_id, data['data'])

    def test_process_data_success_with_companyid(self):
        # Test fallback to companyId
        token = generate_token({"companyId": self.valid_business_id, "sub": "test_user"}, self.jwt_secret)
        response = self.app.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertIn(self.valid_business_id, data['data'])

    def test_missing_authorization_header(self):
        response = self.app.post('/process',
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Authorization header is missing')

    def test_malformed_authorization_header_no_bearer(self):
        token = generate_token({"businessId": "test_id"}, self.jwt_secret)
        response = self.app.post('/process',
                                 headers={'Authorization': f'Token {token}'}, # Incorrect prefix
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Authorization header must start with Bearer')

    def test_malformed_authorization_header_no_token(self):
        response = self.app.post('/process',
                                 headers={'Authorization': 'Bearer '}, # No token
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Token not found')


    def test_invalid_token(self):
        invalid_secret = "THIS IS NOT THE RIGHT SECRET"
        token = generate_token({"businessId": "test_id"}, invalid_secret)
        response = self.app.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid token') # Error from decode_jwt

    def test_expired_token(self):
        token = generate_token({"businessId": "test_id"}, self.jwt_secret, expiry_delta_seconds=-3600) # Expired
        response = self.app.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Token has expired') # Error from decode_jwt

    def test_missing_business_id_in_token(self):
        token = generate_token({"sub": "test_user"}, self.jwt_secret) # No businessId or companyId
        response = self.app.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Business ID (businessId or companyId) not found in token')

    def test_data_processing_failure_for_unknown_business_id(self):
        unknown_business_id = "unknown-business-id-123"
        token = generate_token({"businessId": unknown_business_id}, self.jwt_secret)
        response = self.app.post('/process',
                                 headers={'Authorization': f'Bearer {token}'},
                                 data=json.dumps({}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 401) # Falls into general exception
        data = json.loads(response.data)
        self.assertIn(f"Failed to load or process data for business ID: {unknown_business_id}", data['error'])

if __name__ == '__main__':
    unittest.main()
