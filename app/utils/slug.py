# app/utils/slug.py
import re

def generate_slug(input_string: str) -> str:
    """
    Generates a URL-friendly slug from the input string:
    - lowercase
    - spaces → hyphens
    - strip non-alphanumeric (except hyphens)
    - trim leading/trailing hyphens
    """
    slug = input_string.lower()
    slug = slug.replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    return slug.strip("-")
