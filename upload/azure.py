#!/usr/bin/env python

from azure.core.credentials import (
    AzureNamedKeyCredential,
    AzureSasCredential,
    TokenCredential,
)
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContentSettings,
)
from typing import Any, Dict
from upload.settings import CloudAzProperties, File


class AzBlobServiceClient(BlobServiceClient):
    def __init__(
        self,
        az: CloudAzProperties,
        credential: str
        | Dict[str, str]
        | AzureNamedKeyCredential
        | AzureSasCredential
        | TokenCredential
        | None = DefaultAzureCredential(),
        **kwargs: Any,
    ) -> None:
        super().__init__(
            f"https://{az.storage_account_name}.{az.storage_url_suffix}",
            credential,
            **kwargs,
        )
        self.storage_container_name = az.storage_container_name

    def get_blob_client(
        self,
        blob: str,
        snapshot: Dict[str, Any] | str | None = None,
        *,
        version_id: str | None = None,
    ) -> BlobClient:
        return super().get_blob_client(
            self.storage_container_name, blob, snapshot, version_id=version_id
        )
