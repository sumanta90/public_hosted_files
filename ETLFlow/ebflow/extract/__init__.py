from typing import Union, BinaryIO

from ebflow.extract.file_manager import FileManager
from ebflow.utils.schemas import (
    ErpFile,
    EBStandardExtractionResponse,
)


def eb_standard_extract(
    content: Union[str, bytes, BinaryIO],
    content_format: str,
    innerpath: str = None,
    n_rows: int = 100,
    clean_data: bool = False,
    fill_down_options: [dict[str, bool]] = [],
    file_config: ErpFile = None,
    full_validation: bool = False,
) -> EBStandardExtractionResponse:
    fm = FileManager()
    eb_extract = fm.read_and_validated_resource(
        content,
        content_format,
        innerpath,
        n_rows,
        clean_data,
        fill_down_options,
        file_config,
        full_validation,
    )
    return eb_extract
