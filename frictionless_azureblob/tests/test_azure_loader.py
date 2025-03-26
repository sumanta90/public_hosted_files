from frictionless_azureblob.control import AzureBlobControl
import pytest
import datetime
import os
import logging

from frictionless import Resource, system, formats, Schema, fields
from frictionless.resources import TableResource


def test_loader_identifies_blob_urls():
    print("test_basic_loader:")
    for plugin in system.plugins:
        print(plugin)
    resource = Resource(
        path='https://accountname.blob.core.windows.net/container/subfolder/file.csv')
    print(resource)
    assert resource.scheme == 'azureblob'
    assert resource.path == 'https://accountname.blob.core.windows.net/container/subfolder/file.csv'
    assert resource.tabular is True  # CSV is tabular (i.e. a TableResource)
    assert resource.format == "csv"


def test_loader_identifies_azurite_blob_urls():
    resource = Resource(
        path='https://127.0.0.1:10000/devstoreaccount1/container1/subfolder/file.csv')
    assert resource.scheme == 'azureblob'
    assert resource.path == 'https://127.0.0.1:10000/devstoreaccount1/container1/subfolder/file.csv'
    assert resource.tabular is True  # CSV is tabular (i.e. a TableResource)
    assert resource.format == "csv"


@pytest.mark.skipif(not os.environ.get('AZURE_CLIENT_ID'), reason="Test requires Azure credentials")
@pytest.mark.parametrize("path,format, control", [
    ("https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data.csv", None, None),
    ("https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data.json", "json", None),
    ("https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data.txt", "csv", AzureBlobControl(name="default-size", chunk_size=-1)),
    ("https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data.xlsx",
     None, formats.ExcelControl(sheet="Sheet2"))
])
def test_loader_can_read_different_file_types(path, format, control):
    # Force date columns to be strings so that e.g. CSV and excel are consistent
    schema = Schema(fields=[fields.StringField(name="Name"), fields.IntegerField(
        name="Age"), fields.DateField(name="DOB")])

    resource = TableResource(path=path, format=format,
                             control=control, schema=schema)

    with resource:
        assert resource.header == ["Name", "Age", "DOB"]
        assert resource.read_rows() == [
            {"Name": "abc", "Age": 20, "DOB": datetime.date(2003, 12, 10)},
            {"Name": "def", "Age": 30, "DOB": None},
            {"Name": "ghi", "Age": None, "DOB": datetime.date(1983, 6, 10)}
        ]

# TODO: Automate the file setup and tidy up

@pytest.mark.skipif(not os.environ.get('AZURE_CLIENT_ID'), reason="Test requires Azure credentials")
@pytest.mark.skipif(True, reason="Requires manual checking and file tidy up")
def test_loader_can_save_file():
    source = TableResource(
        path='https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data.csv',
    )
    target = TableResource(
        path='https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/unit-test-copy.csv',
    )

    # Verify that this can write to the target without throwing an exception
    # Verification that the file is identical happens manually
    # - note that Frictionless seems to drop the microseconfs from timestamps
    #   e.g "2022-07-21T00:00:00.000" => "2022-07-21T00:00:00"
    source.write(target)

@pytest.mark.skipif(not os.environ.get('AZURE_CLIENT_ID'), reason="Test requires Azure credentials")
@pytest.mark.skipif(True, reason="Requires manually deleting the file")
def test_loader_overwrite_file():
    control = AzureBlobControl(overwrite=True)
    source = TableResource(
        path='https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data.csv',
    )
    target = TableResource(
        path='https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data_output.csv',
    )
    target2 = TableResource(
        path='https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/data_output.csv',
        control=control
    )

    # Verify that this can write to the target without throwing an exception
    # Verification that the file is identical happens manually
    # - note that Frictionless seems to drop the microseconfs from timestamps
    #   e.g "2022-07-21T00:00:00.000" => "2022-07-21T00:00:00"

    source.write(target)
    source.write(target2)

@pytest.mark.skipif(not os.environ.get('AZURE_CLIENT_ID'), reason="Test requires Azure credentials")
@pytest.mark.parametrize("path,format, control, hash, bytes, rows", [
    (
        "https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/rt-fo-all-companies-2024-04-16T1543.csv",
        None,
        None,
        'sha256:e77540426b7652e43edd58c0d66ce46d0e4e9fe78be14f1d72f819a611681d2e',
        188718378,
        162081),
        (
        "https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/rt-fo-all-companies-2024-04-16T1543.csv",
        None,
        AzureBlobControl(name="even-larger-chunk-size", chunk_size=8*1024*1024),
        'sha256:e77540426b7652e43edd58c0d66ce46d0e4e9fe78be14f1d72f819a611681d2e',
        188718378,
        162081)
])
def test_loading_large_file_quickly(path, format, control, hash, bytes, rows):
    """
    Test that we can load a large file relatively quickly using the larger chunk size.
    Note that there isn't a test here for a small chunk size as it takes too long (~10 mins for the big file)
    One of the tests in `test_loader_can_read_different_file_types` does use the TextIOWrapper defaults as it's a small file
    """
    resource = TableResource(path=path, format=format,
                             control=control)

    # Infer stats which requires that we read through the whole file to calculate hash, number of rows etc.
    # TODO: Inferring stats doesn't seem to work for xlsx files. To be investigated in a later ticket
    resource.infer(stats=True)
    print("Resource stats: ", resource.hash,
          resource.bytes, resource.stats.rows)
    assert resource.hash == hash
    assert resource.bytes == bytes
    assert resource.stats.rows == rows


if __name__ == "__main__":
    """
    Allow the file to be run as a script for easier debugging
    """
    logging.basicConfig(level=logging.WARN)
    test_loading_large_file_quickly(
        "https://stdemocompanyaccount1.blob.core.windows.net/auditfirma/incoming/frictionless-unit-tests/frictionless-unit-tests-engagement/rt-fo-all-companies-2024-04-16T1543.csv",
        None,
        AzureBlobControl(name="Large chunk size", chunk_size=-1),
        'sha256:e77540426b7652e43edd58c0d66ce46d0e4e9fe78be14f1d72f819a611681d2e',
        188718378,
        162081)
