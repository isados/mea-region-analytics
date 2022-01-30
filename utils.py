import yaml
from base64 import b64encode, b64decode

def convertfile_to_base64str(filename: str) -> str :
    try:
        with open(filename, "rb") as file:
            content = file.read()
            return b64encode(content)
    except FileNotFoundError as ec:
        raise ec

def write_base64str_obj_to_file(text: str, filename: str) -> None:
    try:
        with open(filename, 'wb') as file:
            file.write(b64decode(text))
    except FileExistsError:
        print("Error in conversion")
    return None

def read_text_fromfile(path: str) -> str:
    with open(path, "r") as file:
        text = file.read()
    return text

def get_config(config_file: str="config.yml") -> dict:
    try:
        with open(config_file, 'r', newline='') as f:
            return yaml.load(f, Loader=yaml.Loader)
    except yaml.YAMLError as ymlexcp:
        print(ymlexcp)
        return None