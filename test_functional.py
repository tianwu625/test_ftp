import unittest
import ftplib
import pytest

from . import GLOBAL_TIMEOUT

class TestFtpFsOperations(unittest.TestCase):
    """Test: PWD, CWD, CDUP, SIZE, RNFR, RNTO, DELE, MKD, RMD, MDTM,
    STAT, MFMT.
    """
    client_class = ftplib.FTP

    def setUp(self):
        super().setUp()
        server_host = self.uconfig.get('server_host')
        server_port = self.uconfig.get('server_port', 21)
        server_user = self.uconfig.get('server_user')
        server_password = self.uconfig.get('server_password')
        timeout = self.uconfig.get('global_timeout', GLOBAL_TIMEOUT)
        assert(server_host != None and server_user != None and server_password != None)
        self.client = self.client_class(timeout=timeout)
        self.client.connect(server_host, server_port)
        self.client.login(server_user,server_password)


    def tearDown(self):
        self.client.close()
        super().tearDown()

    def test_cwd(self):
        share_name = self.uconfig.get('share_name')
        assert share_name != None
        self.client.cwd(share_name)
        assert self.client.pwd() == '/' + share_name
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd('subtempdir')
        # cwd provided with no arguments is supposed to move us to the
        # root directory
        self.client.sendcmd('cwd /')
        assert self.client.pwd() == '/'

    def test_pwd(self):
        share_name = self.uconfig.get('share_name')
        assert share_name != None
        assert self.client.pwd() == '/'
        self.client.cwd(share_name)
        assert self.client.pwd() == '/' + share_name
