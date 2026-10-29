"""Snowflake connection settings. No passwords: login uses a key pair (private key stays in .secrets/).

The account identifier is read from config_local.py (not committed) or the SNOWFLAKE_ACCOUNT
environment variable, e.g.  orgname-accountname
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))

try:
    from config_local import ACCOUNT  # noqa: F401  (file is git-ignored)
except ImportError:
    ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
if not ACCOUNT:
    raise RuntimeError("Set SNOWFLAKE_ACCOUNT or create config_local.py with ACCOUNT = 'orgname-accountname'")

SNOWFLAKE = dict(
    account=ACCOUNT,
    user="RECON_LOADER",
    role="RECON_ROLE",
    warehouse="RECON_WH",
    database="BERLIN_MIGRATION",
    private_key_file=os.path.join(HERE, ".secrets", "rsa_key.p8"),
)
