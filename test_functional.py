import unittest
import ftplib
import pytest
import os
import time
import io
import contextlib
import threading
import re

from . import GLOBAL_TIMEOUT, BUFSIZE, INTERRUPTED_TRANSF_SIZE
from . import get_tmpfilename
from . import touch_filename

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
        self.work_dir = self.uconfig.get('work_dir')
        self.share_name = self.uconfig.get('share_name')
        self.temp_dir_path = self.make_tmp_dir()
        self.temp_file_path = self.make_tmp_file()

    def tearDown(self):
        self.clean_tmp_dir(self.temp_dir_path)
        self.clean_tmp_file(self.temp_file_path)
        self.client.close()
        super().tearDown()

    def generate_valid_path(self, *args):
        p = os.path.normpath('/'.join(args))
        if p.startswith('//'):
            return p.replace('//', '/')
        else:
            return p

    def get_share_path(self):
        return self.generate_valid_path(self.work_dir, self.share_name)

    def get_work_path(self):
        return self.generate_valid_path(self.work_dir)

    def test_cwd_ok(self):
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        assert os.path.normpath(self.client.pwd()) == share_path

    def test_cwd_enoent(self):
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            subdir_tmp_path = self.get_tmp_path()
            self.client.cwd(subdir_tmp_path)

    def test_cwd_notdir(self):
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd(self.temp_file_path)

    def test_cwd_symlink_ok(self):
        symlink_name = self.uconfig.get("symlink_dir_name")
        symlink_dst = self.uconfig.get("symlink_dir_dst")
        assert symlink_name != None and symlink_dst != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        symlink_dst_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_dst)
        self.client.cwd(symlink_name_path)
        assert os.path.normpath(self.client.pwd()) == symlink_dst_path

    def test_cwd_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_dir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name)
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd(noperm_dir_path)

    def test_cwd_symlink_notdir(self):
        symlink_name = self.uconfig.get("symlink_file_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd(symlink_name_path)

    def test_xcwd_ok(self):
        share_path = self.get_share_path()
        cmd = f"xcwd {share_path}"
        self.client.sendcmd(cmd)
        assert os.path.normpath(self.client.pwd()) == share_path

    def test_cwd_longpath(self):
        long_path = "../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../.     ./../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../     ../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../..     /../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../.     ./../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../     ../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../..     /../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../.     ./../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../"
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        assert os.path.normpath(self.client.pwd()) == share_path
        self.client.cwd(long_path)
        assert os.path.normpath(self.client.pwd()) == '/'

    def test_cwd_tilde_ok(self):
        self.client.cwd('~')
        assert os.path.normpath(self.client.pwd()) == self.get_work_path()

    def test_cwd_tilde_path_ok(self):
        cwd_path = self.generate_valid_path('~', self.share_name, os.path.basename(self.temp_dir_path))
        self.client.cwd(cwd_path)
        assert os.path.normpath(self.client.pwd()) == self.temp_dir_path

    def test_pwd(self):
        work_path = self.get_work_path()
        assert os.path.normpath(self.client.pwd()) == work_path
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        assert os.path.normpath(self.client.pwd()) == share_path

    def test_xpwd(self):
        work_path = self.get_work_path()
        return_code, current_path = self.client.sendcmd("xpwd").split(" ", 1)
        pattern = r'"[^"]*"'
        match = re.search(pattern, current_path)
        m_path = match.group(0).strip("\"") if match else ""
        assert return_code == "257" and work_path == m_path
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        return_code, current_path = self.client.sendcmd("xpwd").split(" ", 1)
        match = re.search(pattern, current_path)
        m_path = match.group(0).strip("\"") if match else ""
        assert return_code == "257" and share_path == m_path

    def get_tmp_path(self, tmp_file=None):
        if tmp_file == None:
            tmp_file = get_tmpfilename()
        return self.generate_valid_path(self.work_dir, self.share_name, tmp_file)

    def make_tmp_dir(self):
        subpath = self.get_tmp_path()
        dirname = self.client.mkd(subpath)
        #assert dirname == subpath
        return subpath

    def clean_tmp_dir(self, subpath):
        try:
            self.client.rmd(subpath)
        except Exception as e:
            pass

    def test_cdup_ok(self):
        self.client.cwd(self.temp_dir_path)
        assert os.path.normpath(self.client.pwd()) == self.temp_dir_path
        self.client.sendcmd('cdup')
        assert os.path.normpath(self.client.pwd()) == os.path.dirname(self.temp_dir_path)
        self.client.sendcmd('cdup')
        assert self.client.pwd() == os.path.dirname(os.path.dirname(self.temp_dir_path))

    def test_cdup_rootdirectory(self):
        # make sure we can't escape from root directory
        self.client.cwd('/')
        self.client.sendcmd('cdup')
        assert self.client.pwd() == '/'

    def test_xcup_ok(self):
        self.client.cwd(self.temp_dir_path)
        assert os.path.normpath(self.client.pwd()) == self.temp_dir_path
        self.client.sendcmd('xcup')
        assert os.path.normpath(self.client.pwd()) == os.path.dirname(self.temp_dir_path)
        self.client.sendcmd('xcup')
        assert self.client.pwd() == os.path.dirname(os.path.dirname(self.temp_dir_path))

    def test_xcup_rootdirectory(self):
        # make sure we can't escape from root directory
        self.client.cwd('/')
        self.client.sendcmd('xcup')
        assert self.client.pwd() == '/'

    def test_mkd_ok(self):
        subpath = self.make_tmp_dir()
        self.clean_tmp_dir(subpath)

    def test_mkd_exists(self):
        subpath = self.make_tmp_dir()
        # make sure we can't create directories which already exist
        # (probably not really necessary);
        # let's use a try/except statement to avoid leaving behind
        # orphaned temporary directory in the event of a test failure.
        with pytest.raises(ftplib.error_perm, match="Create directory operation failed"):
            self.client.mkd(subpath)

        self.clean_tmp_dir(subpath)

    def test_mkd_symlink_exist(self):
        symlink_name = self.unconfig.get("symlink_dir_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        with pytest.raises(ftplib.error_perm, match="Create directory operation failed"):
            self.client.mkd(symlink_name_path)

    def test_mkd_with_spaces(self):
        spaces_path = self.generate_valid_path(self.work_dir, self.share_name, "dir spaces")
        r_path = self.client.mkd(spaces_path)
        #assert r_path == spaces_path
        self.clean_tmp_dir(spaces_path)

    def test_mkd_not_uft8_with_spaces(self):
        spaces_path = self.generate_valid_path(self.work_dir, self.share_name, "olá çim poi...!#$%#%$>= ü")
        with pytest.raises(ftplib.error_perm, match="Invalid request"):
            r_path = self.client.mkd(spaces_path)

    def test_mkd_rooted(self):
        root_path = self.generate_valid_path("/", "sharename")
        with pytest.raises(ftplib.error_perm, match="Invalid path|Create directory operation failed"):
            self.client.mkd(root_path)

    def test_mkd_with_cwd(self):
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        tmpdir = get_tmpfilename()
        res_path = self.client.mkd(tmpdir)
        except_path = self.generate_valid_path(share_path, tmpdir)
        #assert res_path == except_path
        self.clean_tmp_dir(except_path)

    def test_mkd_enoent(self):
        subdirpath = self.get_tmp_path()
        subsubdir = get_tmpfilename()
        subsubpath = self.generate_valid_path(subdirpath, subsubdir)
        with pytest.raises(ftplib.error_perm, match="Create directory operation failed"):
            self.client.mkd(subsubpath)

    def test_mkd_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_subdir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name, get_tmpfilename())
        with pytest.raises(ftplib.error_perm, match="Create directory opertion failed"):
            self.client.mkd(noperm_subdir_path)

    def test_xmkd_ok(self):
        subpath = self.get_tmp_path()
        dirname = self.client.sendcmd(f'xmkd {subpath}')
        self.clean_tmp_dir(subpath)

    def test_mkd_digits(self):
        self.client.cwd(self.get_share_path())
        self.client.mkd("00001")
        self.clean_tmp_dir(self.generate_valid_path(self.get_share_path(), "00001"))

    def test_mkd_embedded_tab(self):
        subpath = self.generate_valid_path(self.get_share_path(), "ab\tcd")
        r_path = self.client.mkd(subpath)
        #assert subpath == r_path
        self.clean_tmp_dir(subpath)

    def test_rmd_ok(self):
        subpath = self.make_tmp_dir()
        self.client.rmd(subpath)

    def test_rmd_enoent(self):
        subpath = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(subpath)
        # make sure we can't remove the root directory
    def test_rmd_rooted(self):
        with pytest.raises(
            ftplib.error_perm, match="Remove directory operation failed|Invalid path"
        ):
            self.client.rmd('/')

    def test_rmd_symlink(self):
        symlink_dst = self.uconfig.get("symlink_rmdir_dst")
        assert symlink_dst != None
        symlink_dst_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_dst)
        self.client.rmd(symlink_dst_path)

    def test_rmd_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_dir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name)
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(noperm_dir_path)

    def test_rmd_notdir(self):
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(self.temp_file_path)

    def test_rmd_notempty(self):
        subpath = self.make_tmp_dir()
        subsubname = get_tmpfilename()
        subsubpath = self.generate_valid_path(subpath, subsubname)
        self.client.mkd(subsubpath)
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(subpath)
        self.clean_tmp_dir(subsubpath)
        self.clean_tmp_dir(subpath)

    def test_xrmd_ok(self):
        subpath = self.make_tmp_dir()
        self.client.sendcmd(f'xrmd {subpath}')

    def test_rmd_with_spaces(self):
        subpath = self.generate_valid_path(self.work_dir, self.share_name, "foo bar zoo")
        self.client.mkd(subpath)
        self.client.rmd(subpath)

    def make_tmp_file(self):
        tmpfile = get_tmpfilename()
        tmp_path = self.get_tmp_path(tmpfile)
        touch_filename(tmpfile)
        with open(tmpfile, 'rb') as f:
            self.client.storbinary('stor ' + tmp_path, f)
        os.remove(tmpfile)
        return tmp_path

    def clean_tmp_file(self, subpath):
        try:
            self.client.delete(subpath)
        except Exception as e:
            pass

    def test_dele(self):
        # upload an empty file to ftp server
        tmp_path = self.make_tmp_file()
        # delete empty file before upload
        self.client.delete(tmp_path)
        with pytest.raises(ftplib.error_perm):
            self.client.delete(self.share_name)

    def test_rnfr_rnto(self):
        temp_file_path = self.get_tmp_path()
        self.client.rename(self.temp_file_path, temp_file_path)
        self.client.rename(temp_file_path, self.temp_file_path)
        # rename dir
        temp_dir_path = self.get_tmp_path()
        self.client.rename(self.temp_dir_path, temp_dir_path)
        self.client.rename(temp_dir_path, self.temp_dir_path)
        # rnfr/rnto over non-existing paths
        with pytest.raises(ftplib.error_perm, match="RNFR command failed"):
            self.client.rename(temp_file_path, self.temp_file_path)
        with pytest.raises(ftplib.error_perm):
            self.client.rename(self.temp_file_path, '/')
        # rnto sent without first specifying the source
        with pytest.raises(ftplib.error_perm, match="RNFR required first"):
            self.client.sendcmd('rnto ' + self.temp_file_path)
        # make sure we can't rename root directory
        with pytest.raises(
            ftplib.error_perm, match="Rename failed|Invalid path"
        ):
            self.client.rename(self.get_work_path(), '/x')

    def test_mdtm(self):
        self.client.sendcmd('mdtm ' + self.temp_file_path)
        temp_file_path = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="Could not get file modification time"):
            self.client.sendcmd('mdtm ' + temp_file_path)
        # make sure we can't use mdtm against directories
        with pytest.raises(ftplib.error_perm, match="Could not get file modification time"):
            self.client.sendcmd('mdtm ' + self.temp_dir_path)

    @pytest.mark.notsupport
    def test_mfmt(self):
        # making sure MFMT is able to modify the timestamp for the file
        test_timestamp = "20170921013410"
        with pytest.raises(ftplib.error_perm, match="Unknown command"):
            self.client.sendcmd('mfmt ' + test_timestamp + ' ' + self.temp_file_path)
        #resp_time = self.client.sendcmd('mdtm ' + self.temp_file_path)
        #resp_time_str = time.strftime('%Y%m%d%H%M%S', time.gmtime(resp_time))
        #assert test_timestamp in resp_time_str

    '''
    def test_mfmt(self):
        # making sure MFMT is able to modify the timestamp for the file
        test_timestamp = "20170921013410"
        self.client.sendcmd('mfmt ' + test_timestamp + ' ' + self.temp_file_path)
        resp_time = self.client.sendcmd('mdtm ' + self.temp_file_path)
        resp_time_str = time.strftime('%Y%m%d%H%M%S', time.gmtime(resp_time))
        assert test_timestamp in resp_time_str

    def test_invalid_mfmt_timeval(self):
        # testing MFMT with invalid timeval argument
        test_timestamp_with_chars = "B017092101341A"
        test_timestamp_invalid_length = "20170921"
        with pytest.raises(ftplib.error_perm, match="Invalid time format"):
            self.client.sendcmd(
                'mfmt ' + test_timestamp_with_chars + ' ' + self.temp_file_path
            )
        with pytest.raises(ftplib.error_perm, match="Invalid time format"):
            self.client.sendcmd(
                'mfmt ' + test_timestamp_invalid_length + ' ' + self.temp_file_path
            )

    def test_missing_mfmt_timeval_arg(self):
        # testing missing timeval argument
        with pytest.raises(ftplib.error_perm, match="Syntax error"):
            self.client.sendcmd('mfmt ' + self.temp_file_path)
    '''

    @pytest.mark.eftp
    def test_size(self):
        s = self.client.size(self.temp_file_path)
        assert s == 0
        # make sure we can't use size against directories
        with pytest.raises(ftplib.error_perm, match="Could not get file size"):
            self.client.sendcmd('size ' + self.temp_dir_path)

class TestFtpStoreData(unittest.TestCase):
    """Test STOR, STOU, APPE, REST, TYPE."""

    client_class = ftplib.FTP

    def make_client(self):
        server_host = self.uconfig.get('server_host')
        server_port = self.uconfig.get('server_port', 21)
        server_user = self.uconfig.get('server_user')
        server_password = self.uconfig.get('server_password')
        timeout = self.uconfig.get('global_timeout', GLOBAL_TIMEOUT)
        assert(server_host != None and server_user != None and server_password != None)
        self.client = self.client_class(timeout=timeout)
        self.client.connect(server_host, server_port)
        self.client.login(server_user,server_password)

    def setUp(self):
        super().setUp()
        self.make_client()
        self.work_dir = self.uconfig.get('work_dir')
        self.share_name = self.uconfig.get('share_name')
        self.dummy_recvfile = io.BytesIO()
        self.dummy_sendfile = io.BytesIO()
        self.temp_file_path = None

    def tearDown(self):
        try:
            if self.temp_file_path != None:
                self.client.delete(self.temp_file_path)
        except Exception as e:
            pass
        self.client.close()
        self.dummy_recvfile.close()
        self.dummy_sendfile.close()
        super().tearDown()

    def get_tmp_file_path(self):
        p = os.path.normpath('/'.join([self.work_dir, self.share_name, get_tmpfilename()]))
        if p.startswith('//'):
            return p.replace('//', '/')
        else:
            return p

    def test_stor(self):
        data = b'abcde12345' * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
        self.client.retrbinary(
            'retr ' + self.temp_file_path, self.dummy_recvfile.write
        )
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(data) == len(datafile)
        assert hash(data) == hash(datafile)

    def test_stor_active(self):
        # Like test_stor but using PORT
        self.client.set_pasv(False)
        self.test_stor()

    def test_stor_ascii(self):
        # Test STOR in ASCII mode

        def store(cmd, fp, blocksize=8192):
            # like storbinary() except it sends "type a" instead of
            # "type i" before starting the transfer
            self.client.voidcmd('type a')
            with contextlib.closing(self.client.transfercmd(cmd)) as conn:
                while True:
                    buf = fp.read(blocksize)
                    if not buf:
                        break
                    conn.sendall(buf)
            return self.client.voidresp()

        self.temp_file_path = self.get_tmp_file_path()
        data = b'abcde12345\r\n' * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)
        store('stor ' + self.temp_file_path, self.dummy_sendfile)
        self.client.retrbinary(
            'retr ' + self.temp_file_path, self.dummy_recvfile.write
        )
        expected = data.replace(b'\r\n', bytes(os.linesep, "ascii"))
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(expected) == len(datafile)
        assert hash(expected) == hash(datafile)

    @pytest.mark.notsupport
    def test_stou(self):
        self.client.set_pasv(True)
        self.client.voidcmd('TYPE I')
        # filename comes in as "1xx FILE: <filename>"
        with pytest.raises((ftplib.error_perm,ftplib.error_temp), match="Unknown command|STOU is not supported|Use PORT or PASV first"):
            filename = self.client.sendcmd('stou').split('FILE: ')[1]
    '''
    def test_stou(self):
        data = b'abcde12345' * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)

        self.client.set_pasv(True)
        self.client.voidcmd('TYPE I')
        # filename comes in as "1xx FILE: <filename>"
        filename = self.client.sendcmd('stou').split('FILE: ')[1]
        try:
            with contextlib.closing(self.client.makeport()) as sock:
                conn, _ = sock.accept()
                with contextlib.closing(conn):
                    conn.settimeout(GLOBAL_TIMEOUT)
                    while True:
                        buf = self.dummy_sendfile.read(8192)
                        if not buf:
                            break
                        conn.sendall(buf)
            # transfer finished, a 226 response is expected
            assert self.client.voidresp()[:3] == '226'
            self.client.retrbinary(
                'retr ' + filename, self.dummy_recvfile.write
            )
            self.dummy_recvfile.seek(0)
            datafile = self.dummy_recvfile.read()
            assert len(data) == len(datafile)
            assert hash(data) == hash(datafile)
        finally:
            # We do not use os.remove() because file could still be
            # locked by ftpd thread.  If DELE through FTP fails try
            # os.remove() as last resort.
            if os.path.exists(filename):
                try:
                    self.client.delete(filename)
                except (ftplib.Error, EOFError, OSError):
                    safe_rmpath(filename)
    def test_stou_rest(self):
        # Watch for STOU preceded by REST, which makes no sense.
        self.client.sendcmd('type i')
        self.client.sendcmd('rest 10')
        with pytest.raises(ftplib.error_temp, match="Can't STOU while REST"):
            self.client.sendcmd('stou')

    def test_stou_orphaned_file(self):
        # Check that no orphaned file gets left behind when STOU fails.
        # Even if STOU fails the file is first created and then erased.
        # Since we can't know the name of the file the best way that
        # we have to test this case is comparing the content of the
        # directory before and after STOU has been issued.
        # Assuming that testfn is supposed to be a "reserved" file
        # name we shouldn't get false positives.
        # login as a limited user in order to make STOU fail
        self.client.login('anonymous', '@nopasswd')
        before = os.listdir(HOME)
        with pytest.raises(ftplib.error_perm, match="Not enough privileges"):
            self.client.sendcmd('stou ' + self.testfn)
        after = os.listdir(HOME)
        if before != after:
            for file in after:
                assert not file.startswith(self.testfn)

    '''
    def test_appe(self):
        data1 = b'abcde12345' * 100000
        self.dummy_sendfile.write(data1)
        self.dummy_sendfile.seek(0)
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)

        data2 = b'fghil67890' * 100000
        self.dummy_sendfile.write(data2)
        self.dummy_sendfile.seek(len(data1))
        self.client.storbinary('appe ' + self.temp_file_path, self.dummy_sendfile)

        self.client.retrbinary(
            "retr " + self.temp_file_path, self.dummy_recvfile.write
        )
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(data1 + data2) == len(datafile)
        assert hash(data1 + data2) == hash(datafile)

    def test_appe_rest(self):
        # Watch for APPE preceded by REST, which makes no sense.
        self.client.sendcmd('type i')
        self.client.sendcmd('rest 10')
        with pytest.raises(ftplib.error_temp, match="Use PORT or PASV first"):
            self.client.sendcmd('appe x')

    def test_rest_on_stor(self):
        # Test STOR preceded by REST.
        data = b'abcde12345' * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)

        self.temp_file_path = self.get_tmp_file_path()

        self.client.voidcmd('TYPE I')
        with contextlib.closing(
            self.client.transfercmd('stor ' + self.temp_file_path)
        ) as conn:
            bytes_sent = 0
            while True:
                chunk = self.dummy_sendfile.read(BUFSIZE)
                conn.sendall(chunk)
                bytes_sent += len(chunk)
                # stop transfer while it isn't finished yet
                if bytes_sent >= INTERRUPTED_TRANSF_SIZE or not chunk:
                    break
        # transfer wasn't finished yet but server can't know this,
        # hence expect a 226 response
        assert self.client.voidresp()[:3] == '226'
        # resuming transfer by using a marker value greater than the
        # file size stored on the server should result in an error
        # on stor
        file_size = self.client.size(self.temp_file_path)
        assert file_size == bytes_sent
        self.client.sendcmd(f'rest {file_size + 1}')
        with pytest.raises(ftplib.error_temp, match="425 Use PORT or PASV first"):
            self.client.sendcmd('stor ' + self.temp_file_path)
        self.client.sendcmd(f'rest {bytes_sent}')
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)

        self.client.retrbinary(
            'retr ' + self.temp_file_path, self.dummy_recvfile.write
        )
        self.dummy_sendfile.seek(0)
        self.dummy_recvfile.seek(0)

        data_sendfile = self.dummy_sendfile.read()
        data_recvfile = self.dummy_recvfile.read()
        assert len(data_sendfile) == len(data_recvfile)
        assert len(data_sendfile) == len(data_recvfile)

    '''
    def test_failing_rest_on_stor(self):
        # Test REST -> STOR against a non existing file.
        self.client.sendcmd('type i')
        self.client.sendcmd('rest 10')
        self.temp_file_path = self.get_tmp_file_path()
        with pytest.raises(ftplib.error_perm, match="No such file"):
            self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
        # if the first STOR failed because of REST, the REST marker
        # is supposed to be resetted to 0
        self.dummy_sendfile.write(b'x' * 4096)
        self.dummy_sendfile.seek(0)
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
    '''
    def test_quit_during_transfer(self):
        # RFC-959 states that if QUIT is sent while a transfer is in
        # progress, the connection must remain open for result response
        # and the server will then close it.
        def send_quit_function():
            self.client.sendcmd('quit')
        self.temp_file_path = self.get_tmp_file_path()
        with contextlib.closing(
            self.client.transfercmd('stor ' + self.temp_file_path)
        ) as conn:
            conn.sendall(b'abcde12345' * 50000)
            t1 = threading.Thread(target=send_quit_function)
            t1.start()
            conn.sendall(b'abcde12345' * 50000)
        # expect the response (transfer ok)
        try:
            assert self.client.voidresp()[:3] == '226' or '221'
        except Exception as e:
            if isinstance(e, EOFError):
                pass
            else:
                raise e
        # Make sure client has been disconnected.
        # OSError (Windows) or EOFError (Linux) exception is supposed
        # to be raised in such a case.
        t1.join()
        self.client.sock.settimeout(0.1)
        with pytest.raises((OSError, EOFError)):
            self.client.sendcmd('noop')
        #reconnect ftp server for clean
        self.make_client()

    def test_stor_empty_file(self):
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
        assert 0 == self.client.size(self.temp_file_path)

class TestFtpRetrieveData(unittest.TestCase):
    """Test RETR, REST, TYPE."""

    client_class = ftplib.FTP

    def retrieve_ascii(self, cmd, callback, blocksize=8192, rest=None):
        """Like retrbinary but uses TYPE A instead."""
        self.client.voidcmd('type a')
        with contextlib.closing(self.client.transfercmd(cmd, rest)) as conn:
            conn.settimeout(GLOBAL_TIMEOUT)
            while True:
                data = conn.recv(blocksize)
                if not data:
                    break
                callback(data)
        return self.client.voidresp()

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
        self.work_dir = self.uconfig.get('work_dir')
        self.share_name = self.uconfig.get('share_name')
        self.dummy_recvfile = io.BytesIO()
        self.dummy_sendfile = io.BytesIO()
        self.temp_file_path = None

    def tearDown(self):
        try:
            if self.temp_file_path != None:
                self.client.delete(self.temp_file_path)
        except Exception as e:
            pass
        self.client.close()
        self.dummy_recvfile.close()
        self.dummy_sendfile.close()
        super().tearDown()

    def get_tmp_file_path(self):
        p = os.path.normpath('/'.join([self.work_dir, self.share_name, get_tmpfilename()]))
        if p.startswith('//'):
            return p.replace('//', '/')
        else:
            return p

    def test_retr(self):
        data = b'abcde12345' * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
        self.client.retrbinary("retr " + self.temp_file_path, self.dummy_recvfile.write)
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(data) == len(datafile)
        assert hash(data) == hash(datafile)

        # attempt to retrieve a file which doesn't exist
        bogus = self.get_tmp_file_path()
        with pytest.raises(ftplib.error_perm, match="Failed to open file"):
            self.client.retrbinary("retr " + bogus, self.dummy_recvfile.write)

    def test_retr_ascii(self):
        # Test RETR in ASCII mode.
        data = (b'abcde12345' + bytes(os.linesep, "ascii")) * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
        self.retrieve_ascii("retr " + self.temp_file_path, self.dummy_recvfile.write)
        expected = data.replace(bytes(os.linesep, "ascii"), b'\r\n')
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(expected) == len(datafile)
        assert hash(expected) == hash(datafile)

    def test_retr_ascii_already_crlf(self):
        # Test ASCII mode RETR for data with CRLF line endings.
        data = b'abcde12345\r\n' * 100000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)
        self.retrieve_ascii("retr " + self.temp_file_path, self.dummy_recvfile.write)
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(data) == len(datafile)
        assert hash(data) == hash(datafile)

    def test_restore_on_retr(self):
        data = b'abcde12345' * 1000000
        self.dummy_sendfile.write(data)
        self.dummy_sendfile.seek(0)
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + self.temp_file_path, self.dummy_sendfile)

        received_bytes = 0
        self.client.voidcmd('TYPE I')
        with contextlib.closing(
            self.client.transfercmd('retr ' + self.temp_file_path)
        ) as conn:
            conn.settimeout(GLOBAL_TIMEOUT)
            while True:
                chunk = conn.recv(BUFSIZE)
                if not chunk:
                    break
                self.dummy_recvfile.write(chunk)
                received_bytes += len(chunk)
                if received_bytes >= INTERRUPTED_TRANSF_SIZE:
                    break

        # transfer wasn't finished yet so we expect a 426 response
        assert self.client.getline()[:3] == "426"

        # resuming transfer by using a marker value greater than the
        # file size stored on the server should result in an error
        # on retr (RFC-1123)
        file_size = self.client.size(self.temp_file_path)
        self.client.sendcmd(f'rest {file_size + 1}')
        with pytest.raises(
            ftplib.error_temp, match="Use PORT or PASV first"
        ):
            self.client.sendcmd('retr ' + self.temp_file_path)
        # test resume
        self.client.sendcmd(f'rest {received_bytes}')
        self.client.retrbinary("retr " + self.temp_file_path, self.dummy_recvfile.write)
        self.dummy_recvfile.seek(0)
        datafile = self.dummy_recvfile.read()
        assert len(data) == len(datafile)
        assert hash(data) == hash(datafile)

    def test_retr_empty_file(self):
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary("stor " + self.temp_file_path, self.dummy_sendfile)
        self.client.retrbinary("retr " + self.temp_file_path, self.dummy_recvfile.write)
        self.dummy_recvfile.seek(0)
        assert self.dummy_recvfile.read() == b""
