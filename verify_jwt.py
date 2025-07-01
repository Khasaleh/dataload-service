import jwt

token = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJtdW5hX1N0b3JlIiwiY29tcGFueUlkIjoiRmF6LTEzLTExLTIwMjQtMDgtMTE3MzkiLCJ1c2VySWQiOjEzLCJyb2xlIjpbeyJhdXRob3JpdHkiOiJST0xFX0FETUlOIn1dLCJpYXQiOjE3NTEzMjk5MDgsImV4cCI6MTc1MTMzMTcwOH0.KlvUEQsmcHUQqvj6263WMMT-O9BtF8DcVY_0yZDZdCzkDb3XKdo7wmr1L0E7z3KOgVu34zvmTkgqvXGooX4IYQ"
jwt_secret = "CIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNMCIUISecretKeyqwertyuiopASDFGHJKLzxcvbnmQWERTYUIOPasdfghjklZXCVBNM"

try:
    # Attempting HS256 first as per plan progression
    print("--- Attempting verification with HS256 ---")
    payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
    print("✅ Payload (HS256):", payload)
except jwt.ExpiredSignatureError:
    print("❌ Token expired")
except jwt.InvalidSignatureError:
    print("❌ Signature verification failed")
except jwt.InvalidTokenError as e:
    print("❌ Invalid token:", e)

# Inspect payload without verification (also check original alg from header)
print("\n--- Inspecting payload without verification ---")
try:
    unverified_header = jwt.get_unverified_header(token)
    print(f"🔎 Token Algorithm from header: {unverified_header.get('alg')}")
    decoded_payload_unverified = jwt.decode(token, options={"verify_signature": False})
    print("🔎 Decoded Payload (unverified):", decoded_payload_unverified)
    if "exp" in decoded_payload_unverified:
        import datetime
        expiry_timestamp = decoded_payload_unverified["exp"]
        expiry_datetime = datetime.datetime.fromtimestamp(expiry_timestamp, tz=datetime.timezone.utc)
        current_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        print(f"Token Expiry Time (UTC): {expiry_datetime}")
        print(f"Current Time (UTC): {current_datetime}")
        if expiry_datetime < current_datetime:
            print("⚠️ Token is expired based on 'exp' claim.")
        else:
            print("✅ Token is not expired based on 'exp' claim.")

except jwt.ExpiredSignatureError:
    print("❌ Token expired (even when trying to decode without signature verification, which is unusual but indicates the 'exp' check is still active)")
except jwt.InvalidTokenError as e:
    print("❌ Invalid token (when trying to decode without signature verification):", e)
