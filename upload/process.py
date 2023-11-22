#!/usr/bin/env python

import binascii, hashlib, json, logging, os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from twilio_notifications.messenger import TwilioNotification

logger = logging.getLogger(__name__)
