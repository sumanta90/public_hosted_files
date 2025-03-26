from __future__ import annotations

import os

import attrs

from frictionless import Control
from . import settings


@attrs.define(kw_only=True, repr=False)
class AzureBlobControl(Control):
    """
    Azure Storage Blob control representation.

    There are two control parameters:
    
    - `overwrite` (bool): Whether to overwrite the file if it already exists (default: false)
    - `chunk_size` (int): The chunk size to use when reading from Azure Blob Storage (default: 4 MiB. Use -1 to not change the TextIOWrapper default)
    
    Note: if the Azure blob location is not public, then the user must provide the credentials in the form of:
    1. A managed identity
    2. Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    3. Any other configuration that will be picked up automatically by the Azure SDK
    """

    type = "azureblob"
    
    overwrite : bool = False

    # Chunk size for reading from Azure Blob Storage (and the chunk size of the loader's text stream)
    # Default is 4 MiB (which matches the Azure SDK's default chunk size for writing to Azure Blob Storage)
    # Set to -1 to not change the TextIOWrapper default
    # Set to anything >0 to use that chunk size
    chunk_size: int = 4*1024*1024

    # Metadata
    
    metadata_profile_patch = {
        "properties": {
            "overwrite": {"type": "boolean"},
            "chunk_size": {"type": "integer", "minimum": -1},
        },
    }
