import duckdb

from app.setup.config import settings

# Initiate a MotherDuck connection using an access token
con = duckdb.connect(f"md:?motherduck_token={settings.MOTHERDUCK_TOKEN}")
