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

'''
@pytest.fixture(scope="function")
def login_context(request, user_config):
    def setUp():
        print('fixture setup')
        tc_self = request.instance #tc_self: tes_class_self
        uconfig = user_config
        server_host = uconfig.get('server_host')
        server_port = uconfig.get('server_port', 21)
        server_user = uconfig.get('server_user')
        server_password = uconfig.get('server_password')
        timeout = uconfig.get('global_timeout', 5)
        assert(server_host != None and server_user != None and server_password != None)
        tc_self.client = tc_self.client_class(timeout=timeout)
        tc_self.client.connect(server_host, server_port)
        tc_self.client.login(server_user,server_password)
    setUp()
    yield
    def tearDown():
        print('fixture teardown')
        tc_self = request.instance
        tc_self.client.close()
    tearDown()
'''


