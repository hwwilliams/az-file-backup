import os, binascii, hashlib
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

PLEX_BACKUP_DIRECTORY_PATH = "./backup-test-files"
STORAGE_ACCOUNT_NAME = "peastusbackup1"
STORAGE_CONTAINER_NAME = "plex-db"


def get_file_md5(file_path: str):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


def get_local_files(directory_path: str):
    local_files = []
    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)
        if os.path.isfile(file_path):
            local_files.append(
                {
                    "name": file_name,
                    "path": file_path,
                    "md5_hash": get_file_md5(file_path),
                }
            )

    return local_files


def get_blobs(container_client: ContainerClient):
    blob_names = []
    blobs = []
    for blob in container_client.list_blobs():
        blob_names.append(blob.name)
        blob_md5_hash_encoded = bytearray(blob.content_settings.content_md5)
        blob_md5_hash_decoded = binascii.hexlify(blob_md5_hash_encoded).decode("utf-8")
        blobs.append(
            {
                "name": blob.name,
                "md5_hash": blob_md5_hash_decoded,
            }
        )

    return blob_names, blobs


def upload_blob(file: object, container_client: ContainerClient):
    print(
        "{0}/{1} - Uploading blob '{2}'".format(
            STORAGE_ACCOUNT_NAME, STORAGE_CONTAINER_NAME, file["name"]
        )
    )
    with open(file=file["path"], mode="rb") as data:
        container_client.upload_blob(name=file["name"], data=data, overwrite=True)


def compare_local_file_blob_hash(file: str, blobs: list):
    for blob in blobs:
        if file["name"] == blob["name"]:
            if file["md5_hash"] == blob["md5_hash"]:
                print(
                    "{0}/{1} - Local file '{2}' already exists as a blob, skipping upload".format(
                        STORAGE_ACCOUNT_NAME,
                        STORAGE_CONTAINER_NAME,
                        file["name"],
                    )
                )
                return True
            else:
                print(
                    "{0}/{1} - Local file '{2}' already exists as a blob but content differs".format(
                        STORAGE_ACCOUNT_NAME,
                        STORAGE_CONTAINER_NAME,
                        file["name"],
                    )
                )
                return False


try:
    storage_account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(
        storage_account_url, credential=DefaultAzureCredential()
    )

    container_client = blob_service_client.get_container_client(
        container=STORAGE_CONTAINER_NAME
    )

    blob_names, blobs = get_blobs(container_client)
    local_files = get_local_files(PLEX_BACKUP_DIRECTORY_PATH)
    for file in local_files:
        if file["name"] in blob_names:
            if not compare_local_file_blob_hash(file, blobs):
                upload_blob(file, container_client)
        else:
            upload_blob(file, container_client)

except Exception as ex:
    print("Exception:")
    print(ex)
