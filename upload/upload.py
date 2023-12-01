#!/usr/bin/env python

import json
import logging
import os
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from hashlib import md5
from pydantic import BaseModel, Field, ValidationError
from typing import List

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def health_check(request_url: str, data=None):
    try:
        if data:
            requests.post(request_url, data=data)
        else:
            requests.post(request_url)
    except requests.RequestException as e:
        logger.error(f"Health check ping failed {str(e)}")
        raise


def get_upload_definition_settings():
    try:
        upload_settings_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "settings", "settings.json")
        )
        logger.debug(
            f"Attempting to load upload settings file '{upload_settings_file_path}'"
        )
        with open(upload_settings_file_path, "r") as f:
            upload_definition_settings = json.load(f)
            UploadDefinitionList.model_validate(upload_definition_settings)
    except ValidationError:
        logger.error(
            f"Invalid JSON found when attempting to load upload settings file '{upload_settings_file_path}'"
        )
        raise
    except json.JSONDecodeError:
        logger.error(
            f"No valid JSON found when attempting to load upload settings file '{upload_settings_file_path}'"
        )
        raise
    except FileNotFoundError:
        logger.error(f"Upload settings file not found '{upload_settings_file_path}'")
        raise
    else:
        return upload_definition_settings["upload_definitions"]


def is_large_file(file_path: str, file_name: str):
    if os.stat(file_path).st_size >= 10000000000:
        logger.warning(f"File '{file_name}' is larger than 10GB, skipping upload")
        return True
    else:
        return False


def get_file_md5(file_path: str):
    try:
        with open(file_path, "rb") as f:
            file_hash = md5()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
    except FileNotFoundError:
        logger.error(f"File not found '{file_path}'")
        raise
    except Exception as e:
        logger.error(e)
        raise
    else:
        return file_hash.digest()


def get_files_to_upload(paths: List[str]):
    files = []
    for path in paths:
        for root, _, file_names in os.walk(os.path.abspath(path)):
            for file_name in file_names:
                file_path = os.path.join(root, file_name)
                if not is_large_file(file_path, file_name):
                    files.append(
                        {
                            "name": file_name,
                            "path": file_path,
                            "md5_digest": get_file_md5(file_path),
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


class UploadDefinition(BaseModel):
    health_check_url: str
    paths: List[str]
    storage_account_name: str
    storage_container_name: str
    storage_url_suffix: str


class UploadDefinitionList(BaseModel):
    upload_definitions: List[UploadDefinition]


class Upload:
    def __init__(self) -> None:
        self.upload_definition_settings = get_upload_definition_settings()

    def upload_blob(self):
        for upload_definition in self.upload_definition_settings:
            health_check_url = upload_definition["health_check_url"]
            paths = check_paths(upload_definition["paths"])
            storage_account_name = upload_definition["storage_account_name"]
            storage_container_name = upload_definition["storage_container_name"]
            storage_url_suffix = upload_definition["storage_url_suffix"]

            try:
                logger.info(
                    f"Connecting to storage account '{storage_account_name}' and storage container '{storage_container_name}'"
                )
                storage_account_url = (
                    f"https://{storage_account_name}.{storage_url_suffix}"
                )
                blob_service_client = BlobServiceClient(
                    storage_account_url, credential=DefaultAzureCredential()
                )

                logger.info(f"Getting files to upload")
                files_to_upload = get_files_to_upload(paths)
                logger.info(f"Found {len(files_to_upload)} file(s) to upload")
                for file in files_to_upload:
                    blob_client = blob_service_client.get_blob_client(
                        container=storage_container_name,
                        blob=file["name"],
                    )

                    if blob_client.exists():
                        if diff_file_blob_md5(file, blob_client):
                            upload_blob(file, blob_client)
                    else:
                        upload_blob(file, blob_client)

            except Exception as e:
                health_check(health_check_url + "/fail", str(e))
                logger.error(e)
                raise

            else:
                health_check(health_check_url)
