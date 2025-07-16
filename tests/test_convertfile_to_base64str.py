from unittest import mock
from unittest.mock import mock_open, patch
import pytest

from utils import *
filename = "path"

def test_with_validfile():
    content = b"MOCKED"
    with patch("builtins.open", mock_open(read_data=content), create=True) as mock_file:
        result = convertfile_to_base64str(filename)
    mock_file.assert_called_once_with(filename, "rb")
    mock_file.return_value.read.assert_called_once()
    assert(result == b64encode(content))

def test_with_emptyfile():
    content = b""
    with patch("builtins.open", mock_open(read_data=content), create=True) as mock_file:
        result = convertfile_to_base64str(filename)
    mock_file.assert_called_once_with(filename, "rb")
    mock_file.return_value.read.assert_called_once()
    assert(result == b64encode(content))

def test_with_raisingFileNotFoundError():
    content = b"MOCKED"
    excep = FileNotFoundError
    with patch("builtins.open", mock_open(read_data=content), create=True) as mock_file:
        mock_file.side_effect = excep
        with pytest.raises(excep):
            result = convertfile_to_base64str(filename)
