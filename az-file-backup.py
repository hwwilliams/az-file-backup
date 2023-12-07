#!/usr/bin/env python

import logging
import os
import requests
from systemd import journal
from upload.azure import AzBlobServiceClient, Blob
from upload.settings import Settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def configure_logging():
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("__main__")
    journald_handler = journal.JournalHandler()
    journald_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(journald_handler)


def health_check(request_url: str, data=None) -> None:
    try:
        if data:
            requests.post(request_url, data=data)
        else:
            requests.post(request_url)
    except requests.RequestException as e:
        logger.error(f"Health check ping failed {str(e)}")
        raise


def upload():
    for definition in Settings.file().upload_definition:
        try:
            blob_service_client = AzBlobServiceClient(definition.cloud.az)

            for path in definition.paths:
                if not os.path.exists(path):
                    raise FileNotFoundError(f"Failed to path '{path}'")

                logger.info(f"Getting files to upload")
                for root, _, file_names in os.walk(os.path.abspath(path)):
                    logger.info(f"Found {len(file_names)} file(s) to upload")

                    for file_name in file_names:
                        blob_file = Blob(
                            blob_service_client,
                            os.path.join(root, file_name),
                            file_name,
                        )

                        if blob_file.exists():
                            if blob_file.content_differs():
                                logger.info(
                                    f"File '{blob_file.name}' already exists as blob but file content is different, overwriting blob"
                                )
                                blob_file.upload()
                            else:
                                logger.info(
                                    f"File '{blob_file.name}' already exists as blob, skipping upload"
                                )
                        else:
                            blob_file.upload()

        except Exception as e:
            health_check(definition.health_check_url + "/fail", str(e))
            logger.error(e)
            raise

        else:
            health_check(definition.health_check_url)


def main():
    configure_logging()
    upload()


if __name__ == "__main__":
    main()
