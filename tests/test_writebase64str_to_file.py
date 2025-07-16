from unittest.mock import mock_open, patch
import binascii
from base64 import b64encode
import pytest

from utils import *

content = b"Hello World!"
contentb64 = b"SGVsbG8gV29ybGQh"
not_binary_text = "SGVsbG8gV29ybGQh"
empty_text = b""
empty_text_notbinary = ""

filename = "FILE"

@pytest.mark.parametrize('valid_text', 
                         [b'Hello World!', b'no', b'1234'])
def test_with_validtext(valid_text):
    encoded = b64encode(valid_text)
    with patch("builtins.open", mock_open(), create=True) as mock_file:
        write_base64str_obj_to_file(encoded, filename)
    mock_file.assert_called_once_with(filename, "wb")
    mock_file.return_value.write.assert_called_once_with(valid_text)

@pytest.mark.skip()
def test_with_NotBinaryText():
    with pytest.raises(binascii.Error):
        write_base64str_obj_to_file(not_binary_text, filename)

def test_with_EmptyText():
    encoded = b64encode(b'')
    with patch("builtins.open", mock_open(), create=True) as mock_file:
        write_base64str_obj_to_file(encoded, filename)
    mock_file.assert_called_once_with(filename, "wb")
    mock_file.return_value.write.assert_called_once_with(b'')

def test_with_NotBase64Text():
    pass

# def test_with_FileExistsError():
#     with patch("builtins.open", mock_open(), create=True) as mock_file:
#         mock_open.side_effect = FileExistsError
#         with pytest.raises(FileExistsError):
#             write_base64str_obj_to_file(contentb64, filename)
#     mock_file.assert_called_once_with(filename, "wb")
#     mock_file.return_value.write.assert_called_once_with(content)

