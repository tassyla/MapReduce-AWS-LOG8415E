import os
from dotenv import load_dotenv
import boto3


def make_session():
    
    load_dotenv() # Load environment variables from a .env file if present
    
    region = os.getenv("AWS_REGION", "us-east-1")
    profile_name = os.getenv("AWS_PROFILE", "default")
    access = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    token  = os.getenv("AWS_SESSION_TOKEN")
    
    # Use explicit credentials if provided
    if access and secret:
        return boto3.Session(
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            aws_session_token=token,
            region_name=region,
        )

    # Otherwise, use AWS CLI profile from ~/.aws/credentials
    else:
        return boto3.Session(profile_name=profile_name, region_name=region)
