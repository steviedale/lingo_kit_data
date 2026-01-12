"""
Backward-compatible shim for the Stanza tokenizer utility.
The implementation now lives in utils.tokenization.stanza_tokenizer.
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from utils.tokenization import StanzaTokenizer  # noqa: E402,F401

__all__ = ["StanzaTokenizer"]
