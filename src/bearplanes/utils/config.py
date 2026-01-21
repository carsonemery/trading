"""Configuration management."""
import os
from pathlib import Path

from dotenv import load_dotenv


def load_environment():
    """Load environment variables from .env file."""
    # config.py -> utils -> bearplanes -> src -> project_root
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    load_dotenv(env_path)

def get_aws_credentials() -> dict:
    """Get AWS credentials from environment."""
    load_environment()
    return {
        "aws_access_key_id": os.getenv("ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("SECRET_ACCESS_KEY"),
    }

def get_api_key(service: str) -> str:
    """Get API key for a service."""
    load_environment()
    key_map = {
        "polygon": "POLYGON_API_KEY",
        "databento": "DATABENTO_API_KEY",
    }
    env_var = key_map.get(service.lower())
    if not env_var:
        raise ValueError(f"Unknown service: {service}")
    
    key = os.getenv(env_var)
    if not key:
        raise ValueError(f"Missing {env_var} in environment")
    return key

def get_wrds_credentials() -> dict:
    """Get WRDS credentials from environment."""
    load_environment()
    return {
        "username": os.getenv("WRDS_USERNAME"),
        "password": os.getenv("WRDS_PASSWORD"),
        # Path to .pgpass file for passwordless connection
        "pgpass_path": os.getenv("PGPASS_PATH")
    }
