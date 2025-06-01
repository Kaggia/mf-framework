################################################################################
# Module:      secret.py
# Description: 
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

import boto3
import json

from mosaic_framework.vault.secret_protocol import ProtocolSecret
from mosaic_framework.vault.exceptions import APIKeySecretRetrievingException

class Secret(ProtocolSecret):
    """
    Secret class, used to retrieve secret data from AWS Secrets Manager.

    Attributes:
        secret_name (str): The name of the secret.
        secret_value (str): The value of the secret.
    
    Methods:
        get(): Retrieves the secret data from the secret manager.
    """
    def __init__(self, secret_name: str) -> None:
        self.secret_name  = secret_name
        self.secret_value = None
    
    def get(self):
        """
        Get the secret data from the secret manager, by its name.
        """

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name='eu-west-1'
        )
        print(f"[Secret] Getting secret: {self.secret_name}")
        # Retrieve the secret value
        get_secret_value_response = client.get_secret_value(
            SecretId=self.secret_name)

        # Get the secret value from the response
        secret_value = get_secret_value_response['SecretString']
        print("[Secret] Base secret has been retrieved.")
        return secret_value

class APIKeySecret(Secret):
    """
    APIKeySecret class, used to retrieve ApiKey from AWS Secrets Manager.

    Attributes:
        secret_name (str): The name of the secret.
        secret_value (str): The value of the secret.
    
    Methods:
        get(): Retrieves the secret data from the secret manager.
    """

    def __init__(self, secret_name: str, **kwargs) -> None:
        super().__init__(secret_name=secret_name)
        self.params = kwargs

    def get(self):
        """
        Get the APIKey credentials from the secret manager.
        """
        base_secret_value = super().get()

        stage = self.params.get('stage', None)
        if stage == None:
            raise APIKeySecretRetrievingException("Missing 'stage' parameter")
        
        self.secret_value = json.loads(base_secret_value)[stage]
        print(f"[APIKeySecret] APIKeySecret has been retrieved for the stage: {stage}")
        return self.secret_value

class DatabaseCredentialsSecret(Secret):
    """
    DatabaseCredentialsSecret class, used to retrieve DatabaseCredentialsSecret from AWS Secrets Manager.

    Attributes:
        secret_name (str): The name of the secret.
        secret_value (str): The value of the secret.
    
    Methods:
        get(): Retrieves the secret data from the secret manager.
    """

    def __init__(self, secret_name: str) -> None:
        super().__init__(secret_name=secret_name)
    
    def get(self):
        """
        Get the Database credentials from the secret manager.
        """
        base_secret_value = super().get()

        raise NotImplementedError("Not implemented yet") 