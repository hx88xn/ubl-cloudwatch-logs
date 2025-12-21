import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-1')
LOG_GROUP_NAME = os.getenv('LOG_GROUP_NAME', '/aws/ecs/fastapi-fortvoice-ubl-prod')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'fastapi-fortvoice-ubl-prod')
JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-secret-key')
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '1440'))

CACHE_TTL_SECONDS = 30
