#!/usr/bin/env python

import os
import pathlib
import json
import logging
from pydantic import BaseModel, ValidationError
from typing import List
from hashlib import md5

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileSizeError(Exception):
    pass


class File:
    def __init__(self, path: str, name: str = None) -> None:
        self.path = path
        self.name = name

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        if os.stat(path).st_size >= 10000000000:  # 10 GB
            raise FileSizeError(path)
        self._path = path

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        if name is None:
            name = os.path.basename(self.path)
        self._name = name

    def get_md5(self) -> str:
        with open(self.path, "rb") as f:
            file_hash = md5()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
        return file_hash.digest()


class CloudAzProperties(BaseModel):
    storage_account_name: str
    storage_container_name: str
    storage_url_suffix: str = "blob.core.windows.net"


class CloudAz(BaseModel):
    az: CloudAzProperties


class UploadDefinition(BaseModel):
    health_check_url: str | None
    paths: List[str]
    cloud: CloudAz


class Settings:
    def __init__(self, *upload_definition: UploadDefinition) -> None:
        self.upload_definition = upload_definition

    @classmethod
    def file(cls):
        project_root_directory = pathlib.Path(pathlib.Path(__file__).parent).parent
        settings_file = File(
            os.path.join(project_root_directory, "settings", "settings.json")
        )

        try:
            with open(settings_file.path, "r") as f:
                loaded_definition = json.load(f)
        except json.JSONDecodeError:
            logger.error(f"File contains invalid JSON '{settings_file.path}'")
            raise
        else:
            return cls(*loaded_definition)

    @classmethod
    def cli(cls):
        ...

    @property
    def upload_definition(self) -> List[UploadDefinition]:
        return self._upload_definition

    @upload_definition.setter
    def upload_definition(self, upload_definition: UploadDefinition) -> None:
        definitions = []
        for definition in upload_definition:
            if not UploadDefinition.model_validate(definition):
                raise ValidationError

            # Remove duplicates while preserving order
            definition["paths"] = list(dict.fromkeys(definition["paths"]))
            definitions.append(UploadDefinition(**definition))
        self._upload_definition = definitions
