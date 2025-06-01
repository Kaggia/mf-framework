import os
import boto3
import json
from botocore.exceptions import NoCredentialsError, ClientError, EndpointConnectionError

from static_color_string import failed_str, utils_str

def remove_files_in_dir(folder_path:str):
    """
    Deletes all files in the specified folder.
    
    :param folder_path: Path to the folder where files should be deleted.
    """
    if not os.path.exists(folder_path):
        print(f"[{utils_str}] The folder '{folder_path}' does not exist.")
        return

    if not os.path.isdir(folder_path):
        print(f"[{utils_str}] The path '{folder_path}' is not a folder.")
        return

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):  # Check if it's a file
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"[{failed_str}] Cannot delete {file_path}: {e}")

def upload_file_to_s3(bucket_name:str, file_path:str, obj_file_name=None, region_name='eu-west-1'):
    """
    Uploads a file to an S3 bucket.

    Args:
        file_path (str): The path of the file to upload.
        bucket_name (str): The name of the S3 bucket.
        object_name (str, optional): The name of the file in the bucket. Defaults to the file name.
        region_name (str, optional): The AWS region where the bucket is located. Defaults to 'us-east-1'.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    # If S3 object name is not specified, use the file name
    if obj_file_name is None:
        obj_file_name = file_path.split('/')[-1]

    # Create an S3 client
    s3 = boto3.client('s3', region_name=region_name)

    try:
        # Upload the file
        response = s3.upload_file(file_path, bucket_name, obj_file_name)
        return {'result':True,  'message': 'Model correctly loaded.'}
    except FileNotFoundError:
        return {'result':False, 'message': 'FileNotFoundError'}
    except NoCredentialsError:
        return {'result':False, 'message': 'NoCredentialsError'}
    except EndpointConnectionError:
        return {'result':False, 'message': 'EndpointConnectionError'}

#Get a secret from the SecretManager
def get_secret(secret_name:str, format:str='plain'):
    """
    Function to retrieve a secret from AWS Secrets Manager

    Args:
        secret_name (str): name of the secret to retrieve.
        format (str, optional): Format of the secret to be casted. Defaults to 'plain'.

    Raises:
        e: ClientError from boto3

    Returns:
        str:  secret value content as string
        dict: secret value content as dict
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=os.getenv('region'))

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise ValueError from e


    value = get_secret_value_response['SecretString']

    #formatting to json if expressed
    if format == 'json':
        value = json.loads(get_secret_value_response['SecretString'])
    
    return value