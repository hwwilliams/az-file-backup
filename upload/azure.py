#!/usr/bin/env python

import logging
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AzBlobServiceClient(BlobServiceClient):
    def __init__(
        self,
        az: CloudAzProperties,
        credential: str = DefaultAzureCredential(),
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


class Blob(File):
    def __init__(
        self, blob_service_client: AzBlobServiceClient, path: str, name: str = None
    ) -> None:
        super().__init__(path, name)
        self.service_client = blob_service_client
        self.client = self.service_client.get_blob_client(blob=self.name)
        self.file_md5 = super().get_md5()

    def exists(self) -> bool:
        return self.client.exists()

    def content_differs(self) -> bool:
        blob_properties = self.client.get_blob_properties()
        return blob_properties.content_settings.content_md5 != self.file_md5

    def upload(self) -> None:
        logger.info(f"Uploading blob '{self.name}'")
        with open(file=self.path, mode="rb") as data:
            self.client.upload_blob(
                data,
                content_settings=ContentSettings(content_md5=self.file_md5),
                overwrite=True,
            )
