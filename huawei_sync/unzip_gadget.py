import json
import zipfile
from pathlib import Path


config_file = Path('file_config.json')

if not config_file.exists():
    config_file = Path(r'huawei_sync\file_config.json')

if config_file.exists():
    with open(config_file, 'r') as f:
        config = json.load(f)
        ZIP_LOCATION = config.get('zip_location', '')
        UNZIP_LOCATION = config.get('unzip_location', '')

with zipfile.ZipFile(ZIP_LOCATION, 'r') as zip_ref:
    zip_ref.extractall(UNZIP_LOCATION)
