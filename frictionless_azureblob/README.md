# Frictionless Azure Blob Module

This adds a Loader plugin to load files from and save files to the Azure blob store.
It differentiates between Azure blobs and generic "remote" files by looking for paths that include blob.core.windows.net.

It uses azure.storage.blob SDK so can work with files accessible to that SDK.

## Credential configuration

`DefaultAzureCredential()` is always used.  This means that the system must provide credentials in a form that is supported by that SDK.  This could be:

1. A managed identity
2. Environment variables named as expected by the SDK (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`)
3. Any other configuration that will be picked up automatically by the Azure SDK

## Runtime Configuration

The AzureBlobControl can be used to control two optional settings of the AzureBlobLoader:

* `chunk_size`: The size of chunks to read from the blob store in bytes.  The default has been increased to 4MiB.  To use the python IOTextWrapper default (8 KiB) set `chunk_size=-1`.  This can be set to other sizes to balance the number of requests made to the blob store versus the memory usage of the loader.  For large files it is recommended to set this to a signifcantly larger size (1 MiB or larger) to reduce the number of requests made to the blob store.
* `overwrite`: If set to `True`, the loader will overwrite any existing file with the same name.  If set to `False`, the loader will raise an exception if the file already exists.  Default is `False`.

Note: the `chunk_size` only affects downloading.  Uploading uses the SDK's defaults, which are currently 4 MiB and thus sufficiently big.

## Warnings

The `chunk_size` option sets the `io.TextIOWraper._CHUNK_SIZE`.  As an _underscore_ variable it is implicitly not part of the public API and could change in future versions of Python.  However it has been present for decades and is unlikely to change.

Increasing the `chunk_size` will also increase the memory usage of the loader, so this should be taken into account when setting the value.

## Usage

```python

resource = TableResource(path="https://${account}.blob.core.windows.net/${container}/path/to/file.csv", 
                             control=AzureBlobControl(name="Large chunk size", chunk_size=1024*1024))

# Infer stats which requires that we read through the whole file to calculate hash, number of rows etc.
resource.infer(stats=True)
print("Resource stats: ", resource.hash, resource.bytes, resource.stats.rows)
```

# Development Setup

Due to issues with adding github dependencies on databricks, the pyproject.toml file does not include frictionless.
For local dev, add frictionless to the pyproject.toml file dependencies list before installing.

Note: DO NOT commit this to the repo.  It is only for local development.

# Release a new wheel

This project is released using wheel packages in ../dependency_wheels for use in Databricks and elsewhere.
To release a new version run the following command (which bumps the minor version number and builds a new wheel package
in the correct location):

```bash
poetry version minor && poetry build -f wheel --output ../dependency_wheels
```