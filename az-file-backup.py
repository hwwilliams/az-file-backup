#!/usr/bin/env python

import logging
from systemd import journal
from upload.process import Process


def configure_logging():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("__main__")
    journald_handler = journal.JournalHandler()
    journald_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(journald_handler)

    urllib3_logger = logging.getLogger("urllib3.connectionpool")
    urllib3_logger.setLevel(logging.WARNING)

    azure_logger = logging.getLogger("azure.identity._credentials.environment")
    azure_logger.setLevel(logging.WARNING)

    azure_logger = logging.getLogger("azure.identity._credentials.managed_identity")
    azure_logger.setLevel(logging.WARNING)

    azure_logger = logging.getLogger("azure.identity._credentials.chained")
    azure_logger.setLevel(logging.WARNING)

    azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azure_logger.setLevel(logging.WARNING)


def upload():
    process = Process()
    process.upload()


def main():
    configure_logging()
    upload()


if __name__ == "__main__":
    main()
