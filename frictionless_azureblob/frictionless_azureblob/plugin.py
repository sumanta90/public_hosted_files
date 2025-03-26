from __future__ import annotations

import logging
import re

from typing import TYPE_CHECKING, Optional

from frictionless import Plugin
from .control import AzureBlobControl
from .loaders import AzureBlobLoader
from .settings import DEFAULT_AZURE_BLOB_URL_REGEX, DEFAULT_AZURITE_BLOB_URL_REGEX

if TYPE_CHECKING:
    from frictionless import Resource, Loader, Control


class AzureblobPlugin(Plugin):
    """
    Plugin for Azure Storage Blobs
    Note that the plugin is named AzureblobPlugin (lowercase "blob"), not AzureBlobPlugin
    because frictionless looks for the plugin class by uppercasing the name of
    the module and exactly matching. So it has to be exactly this.
    """

    # Hooks

    def detect_resource(self, resource: Resource):
        logging.debug(
            f"Checking for Azure Blob: {resource.path}, {resource.scheme}")
        if resource.scheme == "azureblob":
            # Scheme is already identified (or provided by the user)
            pass
        elif (resource.scheme == "https" or resource.scheme == "http") and resource.path is not None:
            # This might be an Azure blob store URL which is in the format:
            # https://<accountname>.blob.core.windows.net/<container>/<subfolder>
            #
            # NOTE: this also detects azurite test server URLs to allow unit testing
            # See: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
            #
            if re.match(DEFAULT_AZURE_BLOB_URL_REGEX, resource.path) is not None:
                logging.debug(
                    f"Detected Azure Blob: {resource.path}, {resource.scheme}")
                resource.scheme = "azureblob"
            elif re.match(DEFAULT_AZURITE_BLOB_URL_REGEX, resource.path) is not None:
                logging.warning(
                    f"Detected Azurite Test Server Blob: {resource.path}, {resource.scheme}")
                resource.scheme = "azureblob"

    def create_loader(self, resource: Resource) -> Optional[Loader]:
        if resource.scheme == "azureblob":
            return AzureBlobLoader(resource)

    def select_control_class(self, type: Optional[str] = None) -> Optional[Control]:
        if type == "azureblob":
            return AzureBlobControl
