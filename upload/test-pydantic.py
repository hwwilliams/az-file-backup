import logging
import json
import os
from datetime import date
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError
from systemd import journal
from typing import List, Tuple


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class UploadDefinition(BaseModel):
    health_check_url: str
    paths: List[str] = Field(
        json_schema_extra={
            "minItems": 1,
        }
    )
    storage_account_name: str
    storage_container_name: str
    storage_url_suffix: str


class UploadDefinitionList(BaseModel):
    upload_definitions: List[UploadDefinition] = Field(
        json_schema_extra={
            "minItems": 1,
        }
    )


def configure_logging():
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("__main__")
    journald_handler = journal.JournalHandler()
    journald_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(journald_handler)


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
        print(upload_definition_settings["upload_definitions"])
        return upload_definition_settings["upload_definitions"]


def main():
    configure_logging()
    upload_definition_settings = get_upload_definition_settings()
    print(json.dumps(UploadDefinitionList.model_json_schema(), indent=2))


if __name__ == "__main__":
    main()
