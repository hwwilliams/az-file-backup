#!/usr/bin/env python

import logging
import os
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from upload.settings import File, Settings
from typing import List

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def health_check(request_url: str, data=None) -> None:
    try:
        if data:
            requests.post(request_url, data=data)
        else:
            requests.post(request_url)
    except requests.RequestException as e:
        logger.error(f"Health check ping failed {str(e)}")
        raise


def get_files_to_upload(paths: List[str]):
    files = []
    for path in paths:
        for root, _, file_names in os.walk(os.path.abspath(path)):
            for file_name in file_names:
                file = File(os.path.join(root, file_name), file_name)
                files.append(
                    {
                        "name": file.name,
                        "path": file.path,
                        "md5_digest": file.get_md5(),
                    }
                )
    return files


def diff_file_blob_md5(file: object, blob_client: BlobClient):
    blob_properties = blob_client.get_blob_properties()
    blob_md5_digest = blob_properties.content_settings.content_md5
    if file["md5_digest"] != blob_md5_digest:
        logger.info(
            f"File '{file['name']}' already exists as blob but file content is different, overwriting blob"
        )
        return True
    else:
        logger.info(f"File '{file['name']}' already exists as blob, skipping upload")
        return False


def upload_blob(file: object, blob_client: BlobClient):
    logger.info(f"Uploading blob '{file['name']}'")
    with open(file=file["path"], mode="rb") as data:
        blob_client.upload_blob(
            data,
            content_settings=ContentSettings(content_md5=file["md5_digest"]),
            overwrite=True,
        )


def check_paths(paths: List[str]):
    try:
        if len(paths) != len(set(paths)):
            raise DuplicateItemInListError(f"Found duplicate paths {paths}")
        for path in paths:
            if not os.path.exists(path):
                raise FileNotFoundError(f"Failed to path '{path}'")
    except:
        raise
    else:
        return paths


class DuplicateItemInListError(Exception):
    pass


class Upload:
    def __init__(self) -> None:
        self.upload_definition = Settings.file().upload_definition

    def upload_blob(self):
        for definition in self.upload_definition:
            try:
                logger.info(
                    f"Connecting to storage account '{definition.storage_account_name}' and storage container '{definition.storage_container_name}'"
                )
                storage_account_url = f"https://{definition.storage_account_name}.{definition.storage_url_suffix}"
                blob_service_client = BlobServiceClient(
                    storage_account_url, credential=DefaultAzureCredential()
                )

                logger.info(f"Getting files to upload")
                files_to_upload = get_files_to_upload(check_paths(definition.paths))
                logger.info(f"Found {len(files_to_upload)} file(s) to upload")
                for file in files_to_upload:
                    blob_client = blob_service_client.get_blob_client(
                        container=definition.storage_container_name,
                        blob=file["name"],
                    )

                    if blob_client.exists():
                        if diff_file_blob_md5(file, blob_client):
                            upload_blob(file, blob_client)
                    else:
                        upload_blob(file, blob_client)

            except Exception as e:
                # health_check(definition.health_check_url + "/fail", str(e))
                logger.error(e)
                raise

            # else:
            #     health_check(definition.health_check_url)
