from __future__ import annotations

# General

# See https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#storage-account-name
DEFAULT_AZURE_BLOB_URL_REGEX = "https://([a-z0-9]+(-[a-z0-9]+)*)\\.blob\\.core\\.windows\\.net/"
DEFAULT_AZURITE_BLOB_URL_REGEX = "https?://(localhost|127\\.0\\.0\\.1):10000/"
