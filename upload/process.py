#!/usr/bin/env python

import json, logging, os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceExistsError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def format_log_message(self, message: str):
    return f"{self.storage_account_name}/{self.storage_container_name} - {message}"


def get_upload_settings():
    upload_settings_file_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "settings", "upload.json")
    )

    logger.debug(
        f"Attempting to load upload settings file '{upload_settings_file_path}'"
    )

    try:
        with open(upload_settings_file_path, "r") as file:
            upload_settings_dict = json.load(file)

    except json.JSONDecodeError:
        logger.error(
            f"No valid JSON data found when attempting to load upload settings file '{upload_settings_file_path}'"
        )
        raise

    except FileNotFoundError:
        logger.error(f"Upload settings file not found '{upload_settings_file_path}'")
        raise

    upload_settings_keys = [
        "directory_path",
        "storage_account_name",
        "storage_container_name",
        "storage_url_suffix",
    ]
    if all(entry in upload_settings_dict for entry in upload_settings_keys):
        logger.debug(
            f"Successfully loaded upload settings from file '{upload_settings_file_path}'"
        )

        return upload_settings_dict

    else:
        logger.error(
            f"Missing upload settings key from file '{upload_settings_file_path}', expected keys '{upload_settings_keys}'"
        )


def get_files_to_upload(directory_path: str):
    try:
        files = []
        for file_name in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file_name)
            if os.path.isfile(file_path):
                files.append({"name": file_name, "path": file_path})

        return files

    except Exception as e:
        logger.error(e)


def upload_blob(self, file: object, container_client: ContainerClient):
    try:
        with open(file=file["path"], mode="rb") as data:
            container_client.upload_blob(name=file["name"], data=data, overwrite=False)

    except ResourceExistsError as e:
        logger.info(
            format_log_message(
                self, f"Blob '{file['name']}' already exists, skipping upload"
            )
        )

    except Exception as e:
        logger.error(e)

    else:
        logger.info(
            format_log_message(self, f"Successfully uploaded blob '{file['name']}'")
        )


class Process:
    def __init__(self):
        logger.info(f"Attempting to get upload settings")

        upload_settings = get_upload_settings()
        self.directory_path = upload_settings["directory_path"]
        self.storage_account_name = upload_settings["storage_account_name"]
        self.storage_container_name = upload_settings["storage_container_name"]
        self.storage_url_suffix = upload_settings["storage_url_suffix"]

    def upload(self):
        try:
            storage_account_url = (
                f"https://{self.storage_account_name}.{self.storage_url_suffix}"
            )
            blob_service_client = BlobServiceClient(
                storage_account_url, credential=DefaultAzureCredential()
            )

            container_client = blob_service_client.get_container_client(
                container=self.storage_container_name
            )

            files = get_files_to_upload(self.directory_path)
            logger.info(
                format_log_message(self, f"Found {len(files)} blobs to upload'")
            )
            for file in files:
                upload_blob(self, file, container_client)

        except Exception as e:
            logger.error(e)
