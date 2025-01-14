import unittest
import ftplib
import pytest

class TestFtpFsOperations(unittest.TestCase):
    """Test: PWD, CWD, CDUP, SIZE, RNFR, RNTO, DELE, MKD, RMD, MDTM,
    STAT, MFMT.
    """

    client_class = ftplib.FTP

    def setUp(self):
        super().setUp()
        self.client = self.client_class(timeout=GLOBAL_TIMEOUT)
        self.client.connect(self.server.host, self.server.port)
        self.client.login(USER, PASSWD)

    def tearDown(self):
        close_client(self.client)
        super().tearDown()

    def test_cwd(self):
        self.client.cwd(self.tempdir)
        assert self.client.pwd() == '/' + self.tempdir
        with pytest.raises(ftplib.error_perm, match="No such file"):
            self.client.cwd('subtempdir')
        # cwd provided with no arguments is supposed to move us to the
        # root directory
        self.client.sendcmd('cwd')
        assert self.client.pwd() == '/'
