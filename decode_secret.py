import base64

jwt_secret_base64 = (
    "Q0lVSVNlY3JldEtleXF3ZXJ0eXVpb3BBU0RGR0hKS0x6eGN2Ym5tUVdFUlRZVUlPUGFzZGZnaGprbFpYQ1ZCTk1DSVVJU2VjcmV0S2V5cXdlcnR5dWlvcEFTREZHSEpLTHp4Y3Zibm1RV0VSVFlVSU9QYXNkZmdoamtsWlhDVkJOTQ=="
)
jwt_secret_bytes = base64.b64decode(jwt_secret_base64)
jwt_secret = jwt_secret_bytes.decode()
print(f"Decoded JWT_SECRET: {jwt_secret}")
