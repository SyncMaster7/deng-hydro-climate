import os

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ['SUPERSET_DB_USER']}:{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@superset-db:5432/{os.environ['SUPERSET_DB_NAME']}"
)

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Production-like settings
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # set True if using HTTPS
TALISMAN_ENABLED = False
