#!/usr/bin/env python

import logging
from systemd import journal
from upload.process import Process


def configure_logging():
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("__main__")
    journald_handler = journal.JournalHandler()
    journald_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(journald_handler)


def upload():
    process = Process()
    process.upload()


def main():
    configure_logging()
    upload()


if __name__ == "__main__":
    main()
