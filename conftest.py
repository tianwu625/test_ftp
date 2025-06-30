import pytest
import os
import json

from . import GLOBAL_TIMEOUT

def pytest_addoption(parser):
    parser.addoption('--user_config_path', action='store', default='', help='user config json path')

@pytest.fixture(scope="session", autouse=True)
def user_config(request):
    config_path = request.config.getoption('--user_config_path')
    assert(config_path != '' and os.path.exists(config_path))
    if not os.path.isabs(config_path):
        config_path = os.path.abspath(config_path)
    with open(config_path, 'r') as f:
        uconfig = json.load(f)
    return uconfig

@pytest.fixture(scope="class", autouse=True)
def set_user_config(request, user_config):
    request.cls.uconfig = user_config
