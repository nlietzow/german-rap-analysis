"""
This script loads the project's secrets from the .env file.
"""
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"

if not load_dotenv(dotenv_path=PROJECT_ROOT / ".env"):
    raise RuntimeError("Could not load .env file with secrets")


@dataclass(frozen=True)
class _Secrets:
    """
    Class for storing secrets.
    """

    GENIUS_TOKEN: str
    OPENAI_API_KEY: str


try:
    SECRETS = _Secrets(
        GENIUS_TOKEN=os.environ["GENIUS_TOKEN"],
        OPENAI_API_KEY=os.environ["OPENAI_API_KEY"],
    )
except KeyError as e:
    raise RuntimeError(f"Could not find {e} in .env file with secrets")
