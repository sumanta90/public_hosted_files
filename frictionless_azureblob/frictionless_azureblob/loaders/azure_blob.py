from __future__ import annotations

import io
import logging
from typing import Any
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential,DefaultAzureCredential
from azure.storage.blob import BlobClient
from frictionless import types, platform, Loader
from ..control import AzureBlobControl

# Create a logger for this file
logger = logging.getLogger("engineb.azure_blob_loader")

class AzureBlobLoader(Loader):
    """
    Azure Storage Blob loader implementation.
    
    Note: if the Azure blob location is not public, then the user must provide the credentials in the form of:
    1. A managed identity
    2. Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    3. Any other configuration that will be picked up automatically by the Azure SDK
    """

    remote = True

    # Read

    def read_byte_stream_create(self):  # type: ignore
        logger.info(f"Reading from Azure Blob: {self.resource.path}")

        # Will pick up managed identity, environment variables etc.
        credential = DefaultAzureCredential()

        # Add credentials
        blob_client = BlobClient.from_blob_url(
            self.resource.path, credential=credential)
        byte_stream = AzureBlobByteStream(blob_client)
        return byte_stream

    # Write

    def write_byte_stream_save(self, byte_stream: types.IByteStream):
        logger.debug(f"Writing to Azure Blob: {self.resource.path}")

        # Will pick up managed identity, environment variables etc.
        credential = DefaultAzureCredential()

        blob_client = BlobClient.from_blob_url(
            self.resource.path, credential=credential)
        
        control = AzureBlobControl.from_dialect(self.resource.dialect)
        logger.debug(f"Overwrite: {control.overwrite}")
        blob_client.upload_blob(byte_stream, overwrite=control.overwrite)

    def read_text_stream(self):
        """Read text stream

        This extends the base class to allow us to optionally set the chunk size of the text stream

        Returns:
            io.TextStream: resource text stream
        """
        # Call the base class
        wrapper = super().read_text_stream()

        # Get the control so we can see if a chunk size has been set
        control = AzureBlobControl.from_dialect(self.resource.dialect)
        if control.chunk_size != -1:
            logger.debug('Setting chunk size to %d', control.chunk_size)
            # Set the chunk size to the value specified in the control
            wrapper._CHUNK_SIZE = control.chunk_size
        return wrapper

# Internal

# Inspired by https://alexwlchan.net/2019/02/working-with-large-s3-objects/
class AzureBlobByteStream(io.RawIOBase):
    def __init__(self, blob_client: Any):
        self.blob_client = blob_client
        self.blob_properties = blob_client.get_blob_properties()
        self.position = 0
        logger.info(f"Blob properties: {self.blob_properties}")

    def __repr__(self):
        return "<%s object=%r>" % (type(self).__name__, self.blob_client)

    @property
    def size(self):
        return self.blob_properties.size

    def readable(self):
        return True

    def writable(self) -> bool:
        return False

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        return self.position

    def read(self, size: int = -1):  # type: ignore
        offset: int = self.position
        length: int = None if size == -1 else size
        logger.debug(f"read() from Azure Blob: {offset=}, {length=}")
        if size == 0:
            # ijson (used by the JSON format) calls read(0) to check if the stream is bytes or string
            # So if called with 0 size, just return an empty byte string
            return b""
        elif size == -1:
            # EOF
            if self.position >= self.size:
                return b""
            # Update the "position" to the end of the file
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size
            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()  # type: ignore
            self.seek(offset=size, whence=io.SEEK_CUR)
        return self.blob_client.download_blob(offset=offset, length=length).readall()

    def read1(self, size: int = -1):  # type: ignore
        return self.read(size)  # type: ignore

    def readinto(self, b) -> int | None:
        """
        Read bytes into a pre-allocated, writable bytes-like object b, and return the number of bytes read.
        e.g. this is used by the ijson yajl2_c backend to read the data from a JSON file

        Note that the Azure SDK StorageStreamDownloader.readinto() expects an IO[T] and not a bytes-like object.
        So we have to work round that a little rather than use the function directly.
        """
        offset: int = self.tell()
        buffer_len = len(b)

        logger.debug(f"readinto() from Azure Blob: {offset=}, {buffer_len=}")

        if offset >= self.size:
            # Already at the end, so don't attempt to read anything
            # Often used to detect the end of the stream
            logger.debug("already at the end of the stream; returning 0")
            return 0

        # Create a stream to read the data into (as required by StorageStreamDownloader.readinto())
        stream = io.BytesIO()

        # Read at most `buffer_len`` bytes into the stream
        num_bytes_stream: int = self.blob_client.download_blob(
            offset=self.position, length=buffer_len
        ).readinto(stream)

        # Write the stream into the buffer
        # Note that we have to reset the stream to the start as the current position is at the end having been
        # written into above.
        stream.seek(0, io.SEEK_SET)
        num_bytes_buffer = stream.readinto(b)

        # Double check just in case something went wrong
        if num_bytes_stream != num_bytes_buffer:
            raise ValueError(
                f"Number of bytes read from stream ({num_bytes_stream}) does not match number of bytes read into buffer ({num_bytes_buffer})")

        # Update our own position
        self.seek(num_bytes_stream, io.SEEK_CUR)

        # Return the number of bytes read into the buffer
        logger.debug(f"readinto() read {num_bytes_stream=} bytes")
        return num_bytes_stream
