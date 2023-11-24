#!/usr/bin/env python

import json, logging, os, requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from hashlib import md5


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def format_log_message(self, message: str):
    return f"{self.storage_account_name}/{self.storage_container_name} - {message}"


def health_check(request_url: str, data=None):
    try:
        if data:
            requests.post(request_url, data=data)
        else:
            requests.post(request_url)

    except requests.RequestException as e:
        logger.error(f"Health check ping failed {str(e)}")
        raise


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

    else:
        upload_settings_keys = [
            "directory_path",
            "health_check_url",
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

    else:
        return file_hash.digest()


def get_files_to_upload(self):
    files = []
    for file_name in os.listdir(self.directory_path):
        file_path = os.path.join(self.directory_path, file_name)
        if os.path.isfile(file_path):
            if os.stat(file_path).st_size < 10000000000:
                files.append(
                    {
                        "name": file_name,
                        "path": file_path,
                        "md5_digest": get_file_md5(file_path),
                    }
                )
            else:
                logger.warning(
                    format_log_message(
                        self,
                        f"File '{file_name}' is larger than 10GB, skipping upload",
                    )
                )

    return files


def compare_file_blob_md5(self, file: object, blob_client: BlobClient):
    blob_properties = blob_client.get_blob_properties()
    blob_md5_digest = blob_properties.content_settings.content_md5
    if file["md5_digest"] == blob_md5_digest:
        logger.info(
            format_log_message(
                self,
                f"Blob '{file['name']}' already exists, skipping upload",
            )
        )
        return True
    else:
        logger.info(
            format_log_message(
                self,
                f"Blob '{file['name']}' already exists but file content is different, overwriting blob",
            )
        )
        return False


def upload_blob(self, file: object, blob_client: BlobClient):
    logger.info(format_log_message(self, f"Uploading blob '{file['name']}'"))
    with open(file=file["path"], mode="rb") as data:
        blob_client.upload_blob(
            data,
            content_settings=ContentSettings(content_md5=file["md5_digest"]),
            overwrite=True,
        )


class Upload:
    def __init__(self):
        logger.info(f"Attempting to get upload settings")

        upload_settings = get_upload_settings()
        self.directory_path = upload_settings["directory_path"]
        self.health_check_url = upload_settings["health_check_url"]
        self.storage_account_name = upload_settings["storage_account_name"]
        self.storage_container_name = upload_settings["storage_container_name"]
        self.storage_url_suffix = upload_settings["storage_url_suffix"]

    def upload_blob(self):
        try:
            storage_account_url = (
                f"https://{self.storage_account_name}.{self.storage_url_suffix}"
            )
            blob_service_client = BlobServiceClient(
                storage_account_url, credential=DefaultAzureCredential()
            )

            files = get_files_to_upload(self)
            logger.info(
                format_log_message(self, f"Found {len(files)} blobs to upload'")
            )
            for file in files:
                blob_client = blob_service_client.get_blob_client(
                    container=self.storage_container_name, blob=file["name"]
                )
                if blob_client.exists():
                    if not compare_file_blob_md5(self, file, blob_client):
                        upload_blob(self, file, blob_client)
                else:
                    upload_blob(self, file, blob_client)

        except Exception as e:
            health_check(self.health_check_url + "/fail", str(e))
            logger.error(e)

        else:
            health_check(self.health_check_url)