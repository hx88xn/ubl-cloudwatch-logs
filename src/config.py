import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-1')
LOG_GROUP_NAME = os.getenv('LOG_GROUP_NAME', '/aws/ecs/fastapi-fortvoice-ubl-prod')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'fastapi-fortvoice-ubl-prod')

# Grafana Loki Configuration
GRAFANA_CLOUD_LOKI_URL = os.getenv('GRAFANA_CLOUD_LOKI_URL', '')
GRAFANA_CLOUD_LOKI_USER = os.getenv('GRAFANA_CLOUD_LOKI_USER', '')
GRAFANA_CLOUD_LOKI_TOKEN = os.getenv('GRAFANA_CLOUD_LOKI_TOKEN', '')

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'fastapi-fortvoice-ubl-prod-db-0.cxyk8ukc4zw1.eu-west-1.rds.amazonaws.com')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER','master')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_NAME', 'fortvoice_db')

# RDS Data API Configuration
RDS_RESOURCE_ARN = os.getenv('RDS_RESOURCE_ARN', '')
RDS_SECRET_ARN = os.getenv('RDS_SECRET_ARN', '')
JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-secret-key')
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '1440'))

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))

# Cache TTL (seconds) - TTL is half of time range for optimal freshness
CACHE_TTL_1H = 1800     # 1 hour range: 30 minutes TTL
CACHE_TTL_6H = 1900   # 6 hour range: 3 hours TTL
CACHE_TTL_24H = 3600    # 24 hour range: 1 hour TTL
CACHE_TTL_48H = 7200    # 48+ hour range: 2 hours TTL
CACHE_TTL_1MONTH = 86400  # 1 month range: 24 hours TTL (auto-expire and renew)

# Legacy TTL (for backward compatibility)
CACHE_TTL_SECONDS = 1800
