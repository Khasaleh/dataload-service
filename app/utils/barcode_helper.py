import base64
from io import BytesIO

from barcode import Code128 # type: ignore
from barcode.writer import ImageWriter # type: ignore
from PIL import Image # To control image dimensions more effectively

class BarcodeGenerationError(Exception):
    """Custom exception for barcode generation failures."""
    pass

def generate_barcode_image(text: str, desired_width: int, desired_height: int) -> bytes:
    """
    Generates a CODE-128 barcode image as PNG bytes with specified dimensions.

    Args:
        text: The text to encode in the barcode.
        desired_width: The desired width of the barcode image in pixels.
        desired_height: The desired height of the barcode image in pixels.

    Returns:
        bytes: The barcode image in PNG format.

    Raises:
        BarcodeGenerationError: If barcode generation fails.
    """
    if not text:
        raise ValueError("Barcode text cannot be empty.")
    if desired_width <= 0 or desired_height <= 0:
        raise ValueError("Barcode dimensions (width, height) must be positive.")

    try:
        # Initialize writer with options for finer control if necessary
        writer_options = {
            'module_height': 15.0,  # Default, can be adjusted
            'font_size': 10,        # Default, can be adjusted
            'text_distance': 5.0,   # Default, can be adjusted
            'quiet_zone': 6.5,      # Default, can be adjusted
        }
        
        # Create a BytesIO buffer to hold the image data
        buffer = BytesIO()
        
        # Generate the barcode with ImageWriter, writing to the buffer
        # The python-barcode library's write method doesn't directly support width/height in pixels.
        # It generates an image based on module_width, font_size etc.
        # We will generate it and then resize using Pillow.
        code128 = Code128(text, writer=ImageWriter())
        code128.write(buffer, options=writer_options) # Pass options here
        
        buffer.seek(0) # Reset buffer position to the beginning for reading
        
        # Open the image with Pillow and resize it
        img = Image.open(buffer)
        
        # Calculate aspect ratio to maintain it if one dimension is not critical,
        # or use exact dimensions if specified. For now, we'll resize to desired w/h.
        # This might stretch the barcode if aspect ratio is not maintained by caller.
        # For barcodes, maintaining aspect ratio by adjusting one dimension or padding is often better.
        # However, the Java code implies specific width and height are enforced.
        resized_img = img.resize((desired_width, desired_height), Image.Resampling.LANCZOS)
        
        # Save the resized image back to a BytesIO buffer in PNG format
        resized_buffer = BytesIO()
        resized_img.save(resized_buffer, format="PNG")
        
        return resized_buffer.getvalue()

    except Exception as e:
        # Log the exception here if a logger is available
        raise BarcodeGenerationError(f"Failed to generate barcode for text '{text}': {e}") from e

def encode_barcode_to_base64(image_bytes: bytes) -> str:
    """
    Encodes image bytes to a Base64 string.

    Args:
        image_bytes: The image data in bytes.

    Returns:
        str: The Base64 encoded string representation of the image.
    """
    if not image_bytes:
        raise ValueError("Image bytes cannot be empty for Base64 encoding.")
    
    try:
        base64_encoded_str = base64.b64encode(image_bytes).decode('utf-8')
        return base64_encoded_str
    except Exception as e:
        # Log the exception here
        raise BarcodeGenerationError(f"Failed to encode image bytes to Base64: {e}") from e

if __name__ == '__main__':
    # Example Usage (for testing the helper directly)
    sample_text = "P123456789"
    width_px = 350
    height_px = 100

    print(f"Generating barcode for: '{sample_text}' with dimensions {width_px}x{height_px}px")

    try:
        barcode_bytes = generate_barcode_image(sample_text, width_px, height_px)
        print(f"Barcode image generated successfully ({len(barcode_bytes)} bytes).")

        # Optional: Save the generated barcode to a file for verification
        with open("test_barcode.png", "wb") as f:
            f.write(barcode_bytes)
        print("Saved test_barcode.png for verification.")

        base64_string = encode_barcode_to_base64(barcode_bytes)
        print("\nBase64 Encoded String:")
        print(base64_string)
        
        # Test edge cases or invalid inputs
        print("\nTesting invalid input for generation:")
        try:
            generate_barcode_image("", 100, 50)
        except ValueError as ve:
            print(f"Caught expected error: {ve}")
        
        print("\nTesting invalid input for encoding:")
        try:
            encode_barcode_to_base64(b"")
        except ValueError as ve:
            print(f"Caught expected error: {ve}")

    except BarcodeGenerationError as bge:
        print(f"Error: {bge}")
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
