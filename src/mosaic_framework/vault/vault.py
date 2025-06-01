################################################################################
# Module:      vault.py
# Description: 
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, Any, TYPE_CHECKING

from mosaic_framework.vault.exceptions import SecretNotMappedException

if TYPE_CHECKING:
    from mosaic_framework.vault.secret import APIKeySecret
    APIKeySecretType  = APIKeySecret

class MosaicVault:
    def __init__(self) -> None:
        self.prefix   = 'Mosaic'
        self.mappings = {
            'growth-models'        : f'{self.prefix}_api_keys_growth_models',
            'api-xmade'            : f'{self.prefix}_api_keys_made',
            'disease'              : f'{self.prefix}_api_keys_disease',
            'insurtech'            : f'{self.prefix}_api_keys_insurtech'}

    def retrieve_secret(self, secret_class:object, secret_name: str, **kwargs) -> str:
        secret_name_mapped = self.mappings.get(secret_name, None)
        if secret_name_mapped == None:
            raise SecretNotMappedException(f"Secret '{secret_name}' not found in the mappings.")
        secret             = secret_class(secret_name_mapped, **kwargs)
        return secret.get()
