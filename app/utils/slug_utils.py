import re

def generate_slug(input_string: str) -> str:
    """
    Generates a URL-friendly slug from the input string.
    
    - Converts to lowercase
    - Strips leading/trailing spaces
    - Replaces spaces with hyphens
    - Removes non-alphanumeric characters except for hyphens

    Args:
        input_string (str): The input string to convert into a slug.

    Returns:
        str: The generated slug.
    """
    # Lowercase the string
    slug = input_string.lower()

    # Replace spaces with hyphens
    slug = slug.replace(' ', '-')

    # Remove non-alphanumeric characters, except for hyphens
    slug = re.sub(r'[^a-z0-9-]', '', slug)

    # Strip any leading or trailing hyphens
    slug = slug.strip('-')

    return slug
