import barcode
from barcode.writer import ImageWriter
from io import BytesIO
import base64

def generate_barcode_image(data: str, writer_options=None) -> bytes:
    """
    Generates a Code128 barcode image from the given data.

    Args:
        data: The data to encode in the barcode.
        writer_options: A dictionary of options for the ImageWriter.
                        Example: {'write_text': False} to hide text below the barcode.

    Returns:
        The barcode image in PNG format as a bytes object.
    """
    if writer_options is None:
        writer_options = {'module_height': 10, 'font_size': 8, 'text_distance': 3}

    code128 = barcode.get_barcode_class('code128')
    barcode_instance = code128(data, writer=ImageWriter())

    buffer = BytesIO()
    barcode_instance.write(buffer, options=writer_options)
    buffer.seek(0)

    return buffer.getvalue()

def encode_barcode_to_base64(image_bytes: bytes) -> str:
    """
    Encodes the given barcode image bytes to a Base64 string.

    Args:
        image_bytes: The barcode image as a bytes object.

    Returns:
        A Base64 encoded string of the image.
    """
    return base64.b64encode(image_bytes).decode('utf-8')

def generate_barcode(data: str, desired_width=None, desired_height=None) -> str:
    """
    Generates a barcode and returns it as a Base64 encoded string.

    Args:
        data: The string to be encoded into the barcode.
        desired_width: The desired width of the barcode image.
        desired_height: The desired height of the barcode image.
        
    Returns:
        A Base64 encoded string representing the barcode image.
    """
    writer_options = {}
    if desired_width is not None:
        writer_options['width'] = desired_width
    if desired_height is not None:
        writer_options['height'] = desired_height

    image_bytes = generate_barcode_image(data, writer_options=writer_options)
    return encode_barcode_to_base64(image_bytes)
