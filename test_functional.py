import unittest
import ftplib
import pytest
import os
import time
import io
import contextlib
import threading
import re
import socket

from . import GLOBAL_TIMEOUT, BUFSIZE, INTERRUPTED_TRANSF_SIZE, TEST_PREFIX
from . import get_tmpfilename
from . import touch_filename

class TestFtpFsOperations(unittest.TestCase):
    """Test: PWD, CWD, CDUP, SIZE, RNFR, RNTO, DELE, MKD, RMD, MDTM,
    STAT, MFMT.
    """
    client_class = ftplib.FTP

    def create_client(self):
        server_host = self.uconfig.get('server_host')
        server_port = self.uconfig.get('server_port', 21)
        server_user = self.uconfig.get('server_user')
        server_password = self.uconfig.get('server_password')
        timeout = self.uconfig.get('global_timeout', GLOBAL_TIMEOUT)
        assert(server_host != None and server_user != None and server_password != None)
        client = self.client_class(timeout=timeout)
        client.connect(server_host, server_port)
        client.login(server_user,server_password)
        return client

    def setUp(self):
        super().setUp()
        self.client = self.create_client()
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

    @pytest.mark.base
    @pytest.mark.cwd
    def test_cwd_ok(self):
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        assert os.path.normpath(self.client.pwd()) == share_path

    @pytest.mark.base
    @pytest.mark.cwd
    def test_cwd_enoent(self):
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            subdir_tmp_path = self.get_tmp_path()
            self.client.cwd(subdir_tmp_path)

    @pytest.mark.base
    @pytest.mark.cwd
    def test_cwd_notdir(self):
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd(self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.perm
    @pytest.mark.cwd
    def test_cwd_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_dir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name)
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd(noperm_dir_path)

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.cwd
    def test_cwd_symlink_notdir(self):
        symlink_name = self.uconfig.get("symlink_file_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        with pytest.raises(ftplib.error_perm, match="Failed to change directory"):
            self.client.cwd(symlink_name_path)

    @pytest.mark.base
    @pytest.mark.cwd
    def test_xcwd_ok(self):
        share_path = self.get_share_path()
        cmd = f"xcwd {share_path}"
        self.client.sendcmd(cmd)
        assert os.path.normpath(self.client.pwd()) == share_path

    @pytest.mark.base
    @pytest.mark.cwd
    def test_cwd_longpath(self):
        long_path = "../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../.     ./../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../     ../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../..     /../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../.     ./../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../     ../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../..     /../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../.     ./../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../../"
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        assert os.path.normpath(self.client.pwd()) == share_path
        self.client.cwd(long_path)
        assert os.path.normpath(self.client.pwd()) == '/'

    @pytest.mark.base
    @pytest.mark.cwd
    def test_cwd_tilde_ok(self):
        self.client.cwd('~')
        assert os.path.normpath(self.client.pwd()) == self.get_work_path()

    @pytest.mark.base
    @pytest.mark.cwd
    def test_cwd_tilde_path_ok(self):
        cwd_path = self.generate_valid_path('~', self.share_name, os.path.basename(self.temp_dir_path))
        self.client.cwd(cwd_path)
        assert os.path.normpath(self.client.pwd()) == self.temp_dir_path

    @pytest.mark.base
    @pytest.mark.pwd
    def test_pwd(self):
        work_path = self.get_work_path()
        assert os.path.normpath(self.client.pwd()) == work_path
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        assert os.path.normpath(self.client.pwd()) == share_path

    @pytest.mark.base
    @pytest.mark.pwd
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
            tmp_file = get_tmpfilename('-{}'.format(self._testMethodName))
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

    @pytest.mark.base
    @pytest.mark.cdup
    def test_cdup_ok(self):
        self.client.cwd(self.temp_dir_path)
        assert os.path.normpath(self.client.pwd()) == self.temp_dir_path
        self.client.sendcmd('cdup')
        assert os.path.normpath(self.client.pwd()) == os.path.dirname(self.temp_dir_path)
        self.client.sendcmd('cdup')
        assert self.client.pwd() == os.path.dirname(os.path.dirname(self.temp_dir_path))

    @pytest.mark.base
    @pytest.mark.cdup
    def test_cdup_rootdirectory(self):
        # make sure we can't escape from root directory
        self.client.cwd('/')
        self.client.sendcmd('cdup')
        assert self.client.pwd() == '/'

    @pytest.mark.base
    @pytest.mark.cdup
    def test_xcup_ok(self):
        self.client.cwd(self.temp_dir_path)
        assert os.path.normpath(self.client.pwd()) == self.temp_dir_path
        self.client.sendcmd('xcup')
        assert os.path.normpath(self.client.pwd()) == os.path.dirname(self.temp_dir_path)
        self.client.sendcmd('xcup')
        assert self.client.pwd() == os.path.dirname(os.path.dirname(self.temp_dir_path))

    @pytest.mark.base
    @pytest.mark.cdup
    def test_xcup_rootdirectory(self):
        # make sure we can't escape from root directory
        self.client.cwd('/')
        self.client.sendcmd('xcup')
        assert self.client.pwd() == '/'

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_ok(self):
        subpath = self.make_tmp_dir()
        self.clean_tmp_dir(subpath)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_exists(self):
        subpath = self.make_tmp_dir()
        # make sure we can't create directories which already exist
        # (probably not really necessary);
        # let's use a try/except statement to avoid leaving behind
        # orphaned temporary directory in the event of a test failure.
        with pytest.raises(ftplib.error_perm, match="Create directory operation failed"):
            self.client.mkd(subpath)

        self.clean_tmp_dir(subpath)

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.mkd
    def test_mkd_symlink_exist(self):
        symlink_name = self.uconfig.get("symlink_dir_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        with pytest.raises(ftplib.error_perm, match="Create directory operation failed"):
            self.client.mkd(symlink_name_path)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_with_spaces(self):
        spaces_path = self.generate_valid_path(self.work_dir, self.share_name, "dir spaces")
        r_path = self.client.mkd(spaces_path)
        #assert r_path == spaces_path
        self.clean_tmp_dir(spaces_path)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_not_uft8_with_spaces(self):
        spaces_path = self.generate_valid_path(self.work_dir, self.share_name, "olá çim poi...!#$%#%$>= ü")
        with pytest.raises(ftplib.error_perm, match="Invalid request"):
            r_path = self.client.mkd(spaces_path)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_rooted(self):
        root_path = self.generate_valid_path("/", "sharename")
        with pytest.raises(ftplib.error_perm, match="Invalid path|Create directory operation failed"):
            self.client.mkd(root_path)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_with_cwd(self):
        share_path = self.get_share_path()
        self.client.cwd(share_path)
        tmpdir = get_tmpfilename('-{}'.format(self._testMethodName))
        res_path = self.client.mkd(tmpdir)
        except_path = self.generate_valid_path(share_path, tmpdir)
        #assert res_path == except_path
        self.clean_tmp_dir(except_path)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_enoent(self):
        subdirpath = self.get_tmp_path()
        subsubdir = get_tmpfilename('-{}'.format(self._testMethodName))
        subsubpath = self.generate_valid_path(subdirpath, subsubdir)
        with pytest.raises(ftplib.error_perm, match="Create directory operation failed"):
            self.client.mkd(subsubpath)

    @pytest.mark.base
    @pytest.mark.perm
    @pytest.mark.mkd
    def test_mkd_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_subdir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name, get_tmpfilename('-{}'.format(self._testMethodName)))
        with pytest.raises(ftplib.error_perm, match="Create directory opertion failed"):
            self.client.mkd(noperm_subdir_path)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_xmkd_ok(self):
        subpath = self.get_tmp_path()
        dirname = self.client.sendcmd(f'xmkd {subpath}')
        self.clean_tmp_dir(subpath)

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_digits(self):
        self.client.cwd(self.get_share_path())
        self.client.mkd("00001")
        self.clean_tmp_dir(self.generate_valid_path(self.get_share_path(), "00001"))

    @pytest.mark.base
    @pytest.mark.mkd
    def test_mkd_embedded_tab(self):
        subpath = self.generate_valid_path(self.get_share_path(), "ab\tcd")
        r_path = self.client.mkd(subpath)
        #assert subpath == r_path
        self.clean_tmp_dir(subpath)

    @pytest.mark.base
    @pytest.mark.rmd
    def test_rmd_ok(self):
        subpath = self.make_tmp_dir()
        self.client.rmd(subpath)

    @pytest.mark.base
    @pytest.mark.rmd
    def test_rmd_enoent(self):
        subpath = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(subpath)

    @pytest.mark.base
    @pytest.mark.rmd
    def test_rmd_rooted(self):
        with pytest.raises(
            ftplib.error_perm, match="Remove directory operation failed|Invalid path"
        ):
            self.client.rmd('/')

    @pytest.mark.base
    @pytest.mark.perm
    @pytest.mark.rmd
    def test_rmd_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_dir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name)
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(noperm_dir_path)

    @pytest.mark.base
    @pytest.mark.rmd
    def test_rmd_notdir(self):
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.rmd
    def test_rmd_notempty(self):
        subpath = self.make_tmp_dir()
        subsubname = get_tmpfilename('-{}'.format(self._testMethodName))
        subsubpath = self.generate_valid_path(subpath, subsubname)
        self.client.mkd(subsubpath)
        with pytest.raises(ftplib.error_perm, match="Remove directory operation failed"):
            self.client.rmd(subpath)
        self.clean_tmp_dir(subsubpath)
        self.clean_tmp_dir(subpath)

    @pytest.mark.base
    @pytest.mark.rmd
    def test_xrmd_ok(self):
        subpath = self.make_tmp_dir()
        self.client.sendcmd(f'xrmd {subpath}')

    @pytest.mark.base
    @pytest.mark.rmd
    def test_rmd_with_spaces(self):
        subpath = self.generate_valid_path(self.work_dir, self.share_name, "foo bar zoo")
        self.client.mkd(subpath)
        self.client.rmd(subpath)

    def make_tmp_file(self):
        tmpfile = get_tmpfilename('-{}'.format(self._testMethodName))
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

    @pytest.mark.base
    @pytest.mark.dele
    def test_dele_ok(self):
        # upload an empty file to ftp server
        tmp_path = self.make_tmp_file()
        # delete empty file before upload
        self.client.delete(tmp_path)

    @pytest.mark.base
    @pytest.mark.dele
    def test_dele_enoent(self):
        tmp_path = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="Delete operation failed"):
            self.client.delete(tmp_path)

    @pytest.mark.base
    @pytest.mark.perm
    @pytest.mark.dele
    def test_dele_eperm(self):
        noperm_file_name = self.uconfig.get("noperm_file_name")
        assert noperm_file_name != None
        noperm_file_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name)
        with pytest.raises(ftplib.error_perm, match="Delete operation failed"):
            self.client.delete(noperm_file_path)

    @pytest.mark.base
    @pytest.mark.dele
    def test_dele_notfile(self):
        with pytest.raises(ftplib.error_perm, match="Delete operation failed"):
            self.client.delete(self.temp_dir_path)

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.dele
    def test_dele_symlink(self):
        symlink_name = self.uconfig.get("symlink_delete_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        self.client.delete(symlink_name_path)

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_file(self):
        temp_file_path = self.get_tmp_path()
        self.client.rename(self.temp_file_path, temp_file_path)
        self.client.rename(temp_file_path, self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_dir(self):
        temp_dir_path = self.get_tmp_path()
        self.client.rename(self.temp_dir_path, temp_dir_path)
        self.client.rename(temp_dir_path, self.temp_dir_path)

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_itself(self):
        self.client.rename(self.temp_file_path, self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.rename
    def test_rnfr_rnto_symlinkdir(self):
        symlink_name = self.uconfig.get("symlink_dir_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        temp_file_path = self.get_tmp_path()
        self.client.rename(symlink_name_path, temp_file_path)
        try:
            self.client.rename(temp_file_path, symlink_name_path)
        except Exception as e:
            pytest.exit("symlink_dir_name nonexist because of rename failed, terminate session")

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.rename
    def test_rnfr_rnto_symlinkfile(self):
        symlink_name = self.uconfig.get("symlink_file_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        temp_file_path = self.get_tmp_path()
        self.client.rename(symlink_name_path, temp_file_path)
        try:
            self.client.rename(temp_file_path, symlink_name_path)
        except Exception as e:
            pytest.exit("symlink_file_name nonexist because of rename failed, terminate session")

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_enoent(self):
        temp_file_path = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="RNFR command failed"):
            self.client.rename(temp_file_path, self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_rooted(self):
        with pytest.raises(ftplib.error_perm):
            self.client.rename(self.temp_file_path, '/')

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnto_only(self):
        with pytest.raises(ftplib.error_perm, match="RNFR required first"):
            self.client.sendcmd('rnto ' + self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_user_rooted(self):
        with pytest.raises(
            ftplib.error_perm, match="Rename failed|Invalid path"
        ):
            self.client.rename(self.get_work_path(), '/x')

    @pytest.mark.base
    @pytest.mark.rename
    def test_rnfr_rnto_with_xfer(self):
        data = b'abcde12345' * 100000
        dummy_sendfile = io.BytesIO()
        dummy_sendfile.write(data)
        dummy_sendfile.seek(0)
        #dummy_recvfile = io.BytesIO()
        temp_file_path = self.get_tmp_path()
        temp_file_path2 = self.get_tmp_path()
        self.client.storbinary("stor " + temp_file_path, dummy_sendfile)
        do_rename = False
        def do_rename_function():
            client2 = self.create_client()
            try:
                client2.rename(temp_file_path, temp_file_path2)
            except Exception as e:
                if not re.search("226", str(e)):
                    pytest.fail(str(e))
            finally:
                client2.close()

        with contextlib.closing(self.client.transfercmd("retr " + temp_file_path, None)) as conn:
            conn.settimeout(GLOBAL_TIMEOUT)
            while True:
                data = conn.recv(8192)
                if not data:
                    break
                if not do_rename:
                    t1 = threading.Thread(target=do_rename_function)
                    t1.start()
                    do_rename = True
        try:
            self.client.voidresp()
        except Exception as e:
            if not re.search("350", str(e)):
                pytest.fail(str(e))
        t1.join()
        dummy_sendfile.close()
        self.clean_tmp_file(temp_file_path)
        self.clean_tmp_file(temp_file_path2)

    @pytest.mark.base
    @pytest.mark.mdtm
    def test_mdtm_ok(self):
        self.client.sendcmd('mdtm ' + self.temp_file_path)

    @pytest.mark.base
    @pytest.mark.mdtm
    def test_mdtm_enoent(self):
        temp_file_path = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="Could not get file modification time"):
            self.client.sendcmd('mdtm ' + temp_file_path)

    @pytest.mark.base
    @pytest.mark.mdtm
    def test_mdtm_notfile(self):
        # make sure we can't use mdtm against directories
        with pytest.raises(ftplib.error_perm, match="Could not get file modification time"):
            self.client.sendcmd('mdtm ' + self.temp_dir_path)

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.mdtm
    def test_mdtm_symlink(self):
        symlink_name = self.uconfig.get("symlink_file_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        self.client.sendcmd('mdtm ' + symlink_name_path)

    @pytest.mark.base
    @pytest.mark.mfmt
    def test_mfmt_notsupport(self):
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
    @pytest.mark.base
    @pytest.mark.size
    def test_size_ok(self):
        s = self.client.size(self.temp_file_path)
        assert s == 0

    @pytest.mark.eftp
    @pytest.mark.base
    @pytest.mark.size
    def test_size_notfile(self):
        with pytest.raises(ftplib.error_perm, match="Could not get file size"):
            self.client.sendcmd('size ' + self.temp_dir_path)

    @pytest.mark.eftp
    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.size
    def test_size_symlink_ok(self):
        symlink_name = self.uconfig.get("symlink_file_name")
        assert symlink_name != None
        symlink_name_size = self.uconfig.get("symlink_file_size")
        assert symlink_name_size != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        s = self.client.size(symlink_name_path)
        assert s == int(symlink_name_size)

    @pytest.mark.eftp
    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.size
    def test_size_symlink_dir(self):
        symlink_name = self.uconfig.get("symlink_dir_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        with pytest.raises(ftplib.error_perm, match="Could not get file size"):
            self.client.size(symlink_name_path)

    @pytest.mark.eftp
    @pytest.mark.base
    @pytest.mark.size
    def test_size_ascii(self):
        data = b'abcde12345\r\n' * 100000
        dummy_sendfile = io.BytesIO()
        dummy_sendfile.write(data)
        dummy_sendfile.seek(0)
        temp_file_path = self.get_tmp_path()
        self.client.storlines('stor ' + temp_file_path, dummy_sendfile)
        self.client.sendcmd('type a')
        s = self.client.size(temp_file_path)
        expect = data.replace(b'\r\n', bytes(os.linesep, "ascii"))
        assert s == len(expect)
        dummy_sendfile.close()
        self.clean_tmp_file(temp_file_path)

    @pytest.mark.eftp
    @pytest.mark.base
    @pytest.mark.size
    def test_size_enoent(self):
        temp_file_path = self.get_tmp_path()
        with pytest.raises(ftplib.error_perm, match="Could not get file size"):
            self.client.size(temp_file_path)

    @pytest.mark.eftp
    @pytest.mark.base
    @pytest.mark.perm
    @pytest.mark.size
    def test_size_eperm(self):
        noperm_file_name = self.uconfig.get("noperm_file_name")
        assert noperm_file_name != None
        noperm_file_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_file_name)
        with pytest.raises(ftplib.error_perm, match="Could not get file size"):
            self.client.size(noperm_file_path)

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
        p = os.path.normpath('/'.join([self.work_dir, self.share_name, get_tmpfilename('-{}'.format(self._testMethodName))]))
        if p.startswith('//'):
            return p.replace('//', '/')
        else:
            return p

    @pytest.mark.base
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

    @pytest.mark.base
    def test_stor_active(self):
        # Like test_stor but using PORT
        self.client.set_pasv(False)
        self.test_stor()

    @pytest.mark.base
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

    @pytest.mark.base
    @pytest.mark.stou
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
    @pytest.mark.base
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

    @pytest.mark.base
    def test_appe_rest(self):
        # Watch for APPE preceded by REST, which makes no sense.
        self.client.sendcmd('type i')
        self.client.sendcmd('rest 10')
        with pytest.raises(ftplib.error_temp, match="Use PORT or PASV first"):
            self.client.sendcmd('appe x')

    @pytest.mark.base
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
    @pytest.mark.base
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

    @pytest.mark.base
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
        p = os.path.normpath('/'.join([self.work_dir, self.share_name, get_tmpfilename('-{}'.format(self._testMethodName))]))
        if p.startswith('//'):
            return p.replace('//', '/')
        else:
            return p

    @pytest.mark.base
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

    @pytest.mark.base
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

    @pytest.mark.base
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

    @pytest.mark.base
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

    @pytest.mark.base
    def test_retr_empty_file(self):
        self.temp_file_path = self.get_tmp_file_path()
        self.client.storbinary("stor " + self.temp_file_path, self.dummy_sendfile)
        self.client.retrbinary("retr " + self.temp_file_path, self.dummy_recvfile.write)
        self.dummy_recvfile.seek(0)
        assert self.dummy_recvfile.read() == b""

class TestFtpnonFsOperations(unittest.TestCase):
    """Test: TYPE, STRU, MODE, NOOP, SYST, ALLO, HELP, SITE HELP."""
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

    def tearDown(self):
        self.client.close()
        super().tearDown()

    @pytest.mark.base
    @pytest.mark.type
    def test_type_acsii(self):
        return_code, return_message = self.client.sendcmd('type a').split(" ", 1)
        assert return_code == "200" and re.search("Switching to ASCII mode", return_message) != None

    @pytest.mark.base
    @pytest.mark.type
    def test_type_binary(self):
        return_code, return_message = self.client.sendcmd('type i').split(" ", 1)
        assert return_code == "200" and re.search("Switching to Binary mode", return_message) != None

    @pytest.mark.base
    @pytest.mark.type
    def test_type_unkown(self):
        with pytest.raises(ftplib.error_perm, match="Unrecognised TYPE command"):
            self.client.sendcmd('type ?!?')

    @pytest.mark.base
    @pytest.mark.stru
    def test_stru_file(self):
        return_code, return_message = self.client.sendcmd('stru f').split(" ", 1)
        assert return_code == "200" and re.search("Structure set to F", return_message) != None

    @pytest.mark.base
    @pytest.mark.stru
    def test_stru_record(self):
        with pytest.raises(ftplib.error_perm, match="Bad STRU command"):
            self.client.sendcmd('stru r')

    @pytest.mark.base
    @pytest.mark.stru
    def test_stru_page(self):
        with pytest.raises(ftplib.error_perm, match="Bad STRU command"):
            self.client.sendcmd('stru p')

    @pytest.mark.base
    @pytest.mark.stru
    def test_stru_unkown(self):
        with pytest.raises(ftplib.error_perm, match="Bad STRU command"):
            self.client.sendcmd('stru ?!?')

    @pytest.mark.base
    @pytest.mark.mode
    def test_mode_stream(self):
        return_code, return_message = self.client.sendcmd('mode s').split(" ", 1)
        assert return_code == "200" and re.search("Mode set to S", return_message) != None

    @pytest.mark.base
    @pytest.mark.mode
    def test_mode_block(self):
        with pytest.raises(ftplib.error_perm, match="Bad MODE command"):
            self.client.sendcmd('mode b')

    @pytest.mark.base
    @pytest.mark.mode
    def test_mode_compress(self):
        with pytest.raises(ftplib.error_perm, match="Bad MODE command"):
            self.client.sendcmd("mode c")

    @pytest.mark.base
    @pytest.mark.mode
    def test_mode_unkown(self):
        with pytest.raises(ftplib.error_perm, match="Bad MODE command"):
            self.client.sendcmd('mode ?!?')

    @pytest.mark.base
    @pytest.mark.noop
    def test_noop_ok(self):
        return_code, return_message = self.client.sendcmd('noop').split(" ", 1)
        assert return_code == "200" and re.search("NOOP ok", return_message) != None

    @pytest.mark.base
    @pytest.mark.syst
    def test_syst_ok(self):
        return_code, return_message = self.client.sendcmd('syst').split(" ", 1)
        assert return_code == "215" and re.search("UNIX Type: L8", return_message) != None

    @pytest.mark.base
    @pytest.mark.allo
    def test_allo_ok(self):
        return_code, return_message = self.client.sendcmd('allo 8192').split(" ", 1)
        assert return_code == "202" and re.search("ALLO command ignored", return_message) != None

    @pytest.mark.base
    @pytest.mark.allo
    def test_allo_unkown(self):
        try:
            return_code, return_message = self.client.sendcmd('allo x').split(" ", 1)
            assert return_code == "202" and re.search("ALLO command ignored", return_message) != None
        except Exception as e:
            if not isinstance(e, ftplib.error_perm):
                raise e
    @pytest.mark.base
    @pytest.mark.quit
    def test_quit_ok(self):
        return_code, return_message = self.client.sendcmd('quit').split(" ", 1)
        assert return_code == "221" and re.search("Goodbye", return_message) != None

    @pytest.mark.base
    @pytest.mark.help
    def test_help_ok(self):
        return_message = self.client.sendcmd('help')
        assert re.search("214 Help OK", return_message) != None

    @pytest.mark.base
    @pytest.mark.site
    def test_site_invalid(self):
        with pytest.raises(ftplib.error_perm, match="Unknown SITE command"):
            self.client.sendcmd('site')

    @pytest.mark.base
    @pytest.mark.site
    def test_site_unkown(self):
        with pytest.raises(ftplib.error_perm, match="Unknown SITE command"):
            self.client.sendcmd('site ?!?')

    @pytest.mark.base
    @pytest.mark.site_help
    def test_site_help_ok(self):
        return_code, return_message = self.client.sendcmd('site help').split(" ", 1)
        assert return_code == "214" and re.search("HELP", return_message)

    @pytest.mark.base
    @pytest.mark.site_help
    def test_site_help_help(self):
        return_code, return_message = self.client.sendcmd('site help help').split(" ", 1)
        assert return_code == "214" and re.search("HELP", return_message)

    @pytest.mark.base
    @pytest.mark.site_help
    def test_site_help_unkown(self):
        return_code, return_message = self.client.sendcmd('site help ?!?').split(" ", 1)
        assert return_code == "214" and re.search("HELP", return_message)

    @pytest.mark.base
    @pytest.mark.rest
    def test_rest_ok(self):
        self.client.sendcmd('type i')
        return_code, return_message = self.client.sendcmd('rest 1024').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(1024\)", return_message)

    @pytest.mark.base
    @pytest.mark.rest
    @pytest.mark.should_fail
    def test_rest_invalid(self):
        self.client.sendcmd('type i')
        return_code, return_message = self.client.sendcmd('rest').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(0\)", return_message)

        return_code, return_message = self.client.sendcmd('rest str').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(0\)", return_message)

        return_code, return_message = self.client.sendcmd('rest ?!?').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(0\)", return_message)

        return_code, return_message = self.client.sendcmd('rest -1').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(0\)", return_message)

        return_code, return_message = self.client.sendcmd('rest 10.1').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(0\)", return_message)

    @pytest.mark.base
    @pytest.mark.rest
    @pytest.mark.should_fail
    def test_rest_ascii(self):
        self.client.sendcmd('type a')

        return_code, return_message = self.client.sendcmd('rest 1024').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \(1024\)", return_message)

    @pytest.mark.base
    @pytest.mark.rest
    def test_rest_2gb(self):
        self.client.sendcmd('type i')
        test_len = (2 ** 31) + 24
        return_code, return_message = self.client.sendcmd(f'rest {test_len - 1}').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \({}\)".format(test_len-1), return_message)

    @pytest.mark.base
    @pytest.mark.rest
    def test_rest_4gb(self):
        self.client.sendcmd('type i')
        test_len = (2 ** 32) + 24
        return_code, return_message = self.client.sendcmd(f'rest {test_len - 1}').split(" ", 1)
        assert return_code == "350" and re.search(r"Restart position accepted \({}\)".format(test_len-1), return_message)

    @pytest.mark.base
    @pytest.mark.feat
    def test_feat_ok(self):
        return_message = self.client.sendcmd('feat')
        assert re.search("211-Features", return_message) != None and re.search("211 End", return_message) != None and \
                re.search("TVFS", return_message) != None and re.search("UTF8", return_message) != None

    @pytest.mark.base
    @pytest.mark.opts
    def test_opts_utf8(self):
        return_code, return_message = self.client.sendcmd('opts utf8 on').split(" ", 1)
        assert return_code == "200" and re.search("Always in UTF8 mode", return_message)

    @pytest.mark.base
    @pytest.mark.opts
    def test_opts_prot(self):
        with pytest.raises(ftplib.error_perm, match="Option not understood"):
            self.client.sendcmd('opts prot p')

    @pytest.mark.base
    @pytest.mark.opts
    def test_opts_ccc(self):
        with pytest.raises(ftplib.error_perm, match="Option not understood"):
            self.client.sendcmd('opts ccc')

    @pytest.mark.base
    @pytest.mark.opts
    def test_opts_noop(self):
        with pytest.raises(ftplib.error_perm, match="Option not understood"):
            self.client.sendcmd('opts noop')

    @pytest.mark.base
    @pytest.mark.rein
    def test_rein_not_support(self):
        with pytest.raises(ftplib.error_perm, match="REIN not implemented"):
            self.client.sendcmd('rein')


class TestFtpCmdsSemantic(unittest.TestCase):
    client_class = ftplib.FTP
    arg_cmds = [
        'appe',
        'dele',
        'eprt',
        'mdtm',
        'mfmt',
        'mkd',
        'mode',
        'opts',
        'port',
        'rest',
        'retr',
        'rmd',
        'rnfr',
        'rnto',
        'site chmod',
        'site',
        'size',
        'stor',
        'stru',
        'type',
        'user',
        'xmkd',
        'xrmd',
    ]
    all_cmds = [
        'abor',
        'allo',
        'appe',
        'cdup',
        'cwd',
        'dele',
        'eprt',
        'epsv',
        'feat',
        'help',
        'list',
        'mdtm',
        'mfmt',
        'mlsd',
        'mlst',
        'mode',
        'mkd',
        'nlst',
        'noop',
        'opts',
        'pass',
        'pasv',
        'port',
        'pwd',
        'quit',
        'rein',
        'rest',
        'retr',
        'rmd',
        'rnfr',
        'rnto',
        'site',
        'site help',
        'site chmod',
        'size',
        'stat',
        'stor',
        'stou',
        'stru',
        'syst',
        'type',
        'user',
        'xcup',
        'xcwd',
        'xmkd',
        'xpwd',
        'xrmd'
    ]

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

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def connect_and_not_login(self, quit_first = True):
        if quit_first:
            self.client.quit()
        server_host = self.uconfig.get('server_host')
        server_port = self.uconfig.get('server_port', 21)
        timeout = self.uconfig.get('global_timeout', GLOBAL_TIMEOUT)
        self.client = self.client_class(timeout=timeout)
        self.client.connect(server_host, server_port)

    @pytest.mark.base
    def test_auth_cmds(self):
        # Test those commands requiring client to be authenticated.
        expected = "530 Please login with USER and PASS.|530 Login with USER and PASS please."
        self.connect_and_not_login()
        for cmd in self.all_cmds:
            if cmd in (
                'feat',
                'user',
                'pass',
                'quit',
                'site',
                'pbsz',
                'auth',
                'prot',
                'ccc',
                'opts',
            ):
                continue
            if cmd in self.arg_cmds:
                cmd += ' arg'
            self.client.putcmd(cmd)
            resp = self.client.getmultiline()
            assert re.search(expected, resp) != None

    @pytest.mark.base
    def test_noauth_cmds(self):
        self.connect_and_not_login()
        for cmd in ('feat',):
            self.client.sendcmd(cmd)

class TestNetWorkProtocols(unittest.TestCase):
    client_class = ftplib.FTP
    def setUp(self):
        super().setUp()
        self.server_host = self.uconfig.get('server_host')
        self.server_port = self.uconfig.get('server_port', 21)
        server_user = self.uconfig.get('server_user')
        server_password = self.uconfig.get('server_password')
        timeout = self.uconfig.get('global_timeout', GLOBAL_TIMEOUT)
        assert(self.server_host != None and server_user != None and server_password != None)
        self.client = self.client_class(timeout=timeout)
        self.client.connect(self.server_host)
        self.client.login(server_user, server_password)
        if self.client.af == socket.AF_INET:
            self.proto = "1"
            self.other_proto = "2"
        else:
            self.proto = "2"
            self.other_proto = "1"

    def tearDown(self):
        self.client.close()
        super().tearDown()

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_vertical_line_4(self):
        # len('|') > 3
        with pytest.raises(ftplib.error_perm, match="Bad EPRT protocol"):
            self.client.sendcmd('eprt ||||')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_vertical_line_2(self):
        # len('|') < 3
        with pytest.raises(ftplib.error_perm, match="Bad EPRT protocol"):
            self.client.sendcmd('eprt ||')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_port_65536(self):
        # port > 65535
        with pytest.raises(ftplib.error_perm, match="Bad EPRT command"):
            self.client.sendcmd(f'eprt |{self.proto}|{self.server_host}|65536|')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_port_negative(self):
        # port = -1
        with pytest.raises(ftplib.error_perm, match="Bad EPRT command"):
            self.client.sendcmd(f'eprt |{self.proto}|{self.server_host}|-1|')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_port_le_1024(self):
        msg = "500 Illegal EPRT command."
        cmd = f'eprt |{self.proto}|{self.server_host}|888|'
        try:
            resp = self.client.sendcmd(cmd)
            re.search("200 EPRT command successful", resp) != None
        except ftplib.error_perm as e:
            assert re.search(msg, str(e))
        except Exception as e:
            raise e

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_proto_3(self):
        with pytest.raises(ftplib.error_perm):
            self.client.sendcmd(f'eprt |3|{self.server_host}|888|')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_ip_ge_4(self):
        with pytest.raises(ftplib.error_perm, match="Bad EPRT command"):
            self.client.sendcmd(f'eprt |1|1.2.3.4.5|2048|')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_ip_ge_255(self):
        with pytest.raises(ftplib.error_perm, match="Bad EPRT command"):
            self.client.sendcmd(f'eprt |1|1.2.3.256|2048|')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_proto_not_match(self):
        with pytest.raises(ftplib.error_perm, match="Illegal EPRT command|Bad EPRT protocol"):
            self.client.sendcmd(f'eprt |2|1.2.3.255|2048|')

    @pytest.mark.base
    @pytest.mark.eprt
    def test_eprt_connection(self):
        with contextlib.closing(socket.socket(self.client.af)) as sock:
            sock.bind((self.client.sock.getsockname()[0], 0))
            sock.listen(5)
            sock.settimeout(GLOBAL_TIMEOUT)
            ip, port = sock.getsockname()[:2]
            self.client.set_pasv(False)
            resp = self.client.sendcmd(f'eprt |{self.proto}|{ip}|{port}|')
            try:
                s = sock.accept()
                s[0].close()
            except socket.timeout:
                self.fail("Server didn't connect to passive socket")

    @pytest.mark.base
    @pytest.mark.port
    def test_port_connect(self):
        with contextlib.closing(self.client.makeport()):
            self.client.sendcmd('abor')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_sep_not_comma(self):
        local_ip = self.client.sock.getsockname()[0]
        comma_ip = ','.join(local_ip.split('.'))
        #port is 12345
        comma_port = '48.57'
        port_arg = comma_ip + ',' + comma_ip
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd(f'port {port_arg}')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_ip_not_int(self):
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd('port X,0,0,1,48,57')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_len_7(self):
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd('port 127,0,0,1,1,1,1')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_len_5(self):
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd('port 127,0,0,1,1')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_ip_256(self):
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd('port 256,0,0,1,1,1')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_port_65536(self):
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd('port 127,0,0,1,256,1')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_port_negative(self):
        with pytest.raises(ftplib.error_perm, match="Illegal PORT command"):
            self.client.sendcmd('port 127,0,0,1,-1,0')

    @pytest.mark.base
    @pytest.mark.port
    def test_port_port_le_1024(self):
        msg = "500 Illegal EPRT command."
        try:
            port_arg = ','.join(self.client.sock.getsockname()[0].split('.') + ['1', '1'])
            resp = self.client.sendcmd(f'port {port_arg}')
            re.search("200 EPRT command successful", resp) != None
        except ftplib.error_perm as e:
            assert re.search(msg, str(e))
        except Exception as e:
            raise e

    @pytest.mark.base
    @pytest.mark.port
    def test_port_ok(self):
        #port is 12345
        port_arg = ','.join(self.client.sock.getsockname()[0].split('.') + ['48', '57'])
        resp = self.client.sendcmd(f'port {port_arg}')
        assert re.search('200', resp) != None

    @pytest.mark.base
    @pytest.mark.epsv
    @pytest.mark.should_fail
    def test_epsv_other_proto(self):
        try:
            resp = self.client.sendcmd('epsv ' + self.other_proto)
            re.search("229", resp)
        except ftplib.error_perm as e:
            re.search("Bad network protocol", str(e))
        except Exception as e:
            raise e

    @pytest.mark.base
    @pytest.mark.epsv
    def test_epsv_invalid_proto(self):
        with pytest.raises(ftplib.error_perm):
            self.client.sendcmd('epsv 3')

    @pytest.mark.base
    @pytest.mark.epsv
    def test_epsv_connect(self):
        for cmd in ('EPSV', 'EPSV ' + self.proto):
            host, port = ftplib.parse229(
                    self.client.sendcmd(cmd), self.client.sock.getpeername())
            with contextlib.closing(
                    socket.socket(self.client.af, socket.SOCK_STREAM)
                    ) as s:
                s.settimeout(GLOBAL_TIMEOUT)
                s.connect((host, port))
                self.client.sendcmd('abor')

    @pytest.mark.base
    @pytest.mark.epsv
    def test_epsv_all(self):
        self.client.sendcmd('epsv all')
        with pytest.raises(ftplib.error_perm):
            self.client.sendcmd('pasv')
        with pytest.raises(ftplib.error_perm):
            self.client.sendport(self.server_host, 2000)

    @pytest.mark.base
    @pytest.mark.pasv
    def test_pasv_connect(self):
        host, port = ftplib.parse227(self.client.sendcmd('pasv'))
        with contextlib.closing(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ) as s:
            s.settimeout(GLOBAL_TIMEOUT)
            s.connect((host, port))
            self.client.sendcmd('abor')

class TestFtpAbort(unittest.TestCase):
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

    def tearDown(self):
        self.client.close()
        super().tearDown()

    @pytest.mark.base
    @pytest.mark.abor
    def test_abor_no_data(self):
        resp = self.client.sendcmd('ABOR')
        assert resp == '225 No transfer to ABOR.'
        resp = self.client.retrlines('list', [].append)

    @pytest.mark.base
    @pytest.mark.abor
    def tset_abor_pasv(self):
        self.client.makepasv()
        respcode = self.client.sendcmd('ABOR')[:3]
        assert respcode == '225'
        self.client.retrlines('list', [].append)

    @pytest.mark.base
    @pytest.mark.abor
    def test_abor_port(self):
        # Case 3: data channel opened with PASV or PORT, but ABOR sent
        # before a data transfer has been started: close data channel,
        # respond with 225
        self.client.set_pasv(0)
        with contextlib.closing(self.client.makeport()):
            respcode = self.client.sendcmd('ABOR')[:3]
        assert respcode == '225'
        self.client.retrlines('list', [].append)

    def get_tmp_file_path(self):
        p = os.path.normpath('/'.join([self.work_dir, self.share_name, get_tmpfilename('-{}'.format(self._testMethodName))]))
        if p.startswith('//'):
            return p.replace('//', '/')
        else:
            return p

    def clean_tmp_file(self, subpath):
        try:
            self.client.delete(subpath)
        except Exception as e:
            pass

    @pytest.mark.base
    @pytest.mark.abor
    def test_abor_during_transfer(self):
        # Case 4: ABOR while a data transfer on DTP channel is in
        # progress: close data channel, respond with 426, respond
        # with 226.
        data = b'abcde12345' * 1000000
        dummy_sendfile = io.BytesIO()
        dummy_sendfile.write(data)
        dummy_sendfile.seek(0)
        temp_file_path = self.get_tmp_file_path()
        self.client.storbinary('stor ' + temp_file_path, dummy_sendfile)
        self.client.voidcmd('TYPE I')
        def do_abort_function():
            self.client.putcmd('ABOR')
            assert self.client.getline()[:3] == "426"
            assert self.client.voidresp()[:3] == '225'

        with contextlib.closing(
            self.client.transfercmd('retr ' + temp_file_path)
        ) as conn:
            bytes_recv = 0
            while bytes_recv < 65536:
                chunk = conn.recv(BUFSIZE)
                bytes_recv += len(chunk)

            t1 = threading.Thread(target=do_abort_function)
            t1.start()
            time.sleep(3)

        t1.join()
        dummy_sendfile.close()
        self.clean_tmp_file(temp_file_path)

class TestFtpListingCmds(unittest.TestCase):
    """Test LIST, NLST, argumented STAT."""

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

    def upload_empty_file(self, tmp_path):
        tmpfile = get_tmpfilename('-{}'.format(self._testMethodName))
        touch_filename(tmpfile)
        with open(tmpfile, 'rb') as f:
            self.client.storbinary('stor ' + tmp_path, f)
        os.remove(tmpfile)

    def make_tmp_file(self):
        tmp_path = self.get_tmp_path()
        self.upload_empty_file(tmp_path)
        return tmp_path

    def clean_tmp_file(self, subpath):
        try:
            self.client.delete(subpath)
        except Exception as e:
            pass

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

    def get_tmp_path(self, tmp_file=None):
        if tmp_file == None:
            tmp_file = get_tmpfilename('-{}'.format(self._testMethodName))
        return self.generate_valid_path(self.work_dir, self.share_name, tmp_file)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_ok(self):
        subpaths = self.client.nlst(self.get_share_path())
        assert self.temp_dir_path in subpaths and self.temp_file_path in subpaths

    @pytest.mark.base
    @pytest.mark.symlink
    @pytest.mark.list
    def test_nlst_symlink(self):
        symlink_name = self.uconfig.get("symlink_dir_name")
        assert symlink_name != None
        symlink_name_path = self.generate_valid_path(self.work_dir, self.share_name, symlink_name)
        self.client.nlst(symlink_name_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_no_path(self):
        self.client.cwd(self.get_share_path())
        subpaths = self.client.nlst()
        assert os.path.basename(self.temp_file_path) in subpaths and os.path.basename(self.temp_dir_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_glob_file(self):
        self.client.cwd(self.get_share_path())
        subpaths = self.client.nlst(f'{TEST_PREFIX}*')
        assert os.path.basename(self.temp_file_path) in subpaths and os.path.basename(self.temp_dir_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    @pytest.mark.should_fail
    def test_nlst_glob_enoent(self):
        self.client.cwd(self.get_share_path())
        subpaths = self.client.nlst('foo*')
        assert subpaths == []

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_glob_more_than_9999(self):
        clean_list = []
        for x in range(14998):
            temp_path = self.make_tmp_file()
            clean_list.append(temp_path)
        self.client.cwd(self.get_share_path())
        subpaths = self.client.nlst(f'{TEST_PREFIX}*')
        assert len(subpaths) >= 15000
        for temp_path in clean_list:
            self.clean_tmp_file(temp_path)

    @pytest.mark.base
    @pytest.mark.perm
    @pytest.mark.list
    def test_nlst_eperm(self):
        noperm_dir_name = self.uconfig.get("noperm_dir_name")
        assert noperm_dir_name != None
        noperm_dir_path = self.generate_valid_path(self.work_dir, self.share_name, noperm_dir_name)
        with pytest.raises(ftplib.error_perm, match="Failed to"):
            self.client.nlst(noperm_dir_path)

    @pytest.mark.base
    @pytest.mark.list
    @pytest.mark.should_fail
    def test_nlst_enoent(self):
        temp_dir_path = self.get_tmp_path()
        subpaths = self.client.nlst(temp_dir_path)
        assert subpaths == []

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_subsub(self):
        subsubname = get_tmpfilename('-{}'.format(self._testMethodName))
        subsubpath = self.generate_valid_path(self.temp_dir_path, subsubname)
        self.client.mkd(subsubpath)
        subpaths = self.client.nlst(self.temp_dir_path)
        assert subsubpath in subpaths
        self.clean_tmp_dir(subsubpath)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_file(self):
        subpaths = self.client.nlst(self.temp_file_path)
        assert self.temp_file_path in subpaths

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_leading_whitespace(self):
        test_file_path = self.generate_valid_path(self.work_dir, self.share_name, ' testfile')
        self.upload_empty_file(test_file_path)
        subpaths = self.client.nlst(test_file_path)
        assert test_file_path in subpaths
        self.clean_tmp_file(test_file_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_dash_filename(self):
        test_file_path = self.generate_valid_path(self.work_dir, self.share_name, '-testfile')
        self.upload_empty_file(test_file_path)
        subpaths = self.client.nlst(test_file_path)
        assert test_file_path in subpaths
        self.clean_tmp_file(test_file_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_trailing_slashes(self):
        test_file_path = self.generate_valid_path(self.work_dir, self.share_name, '.testfile')
        self.upload_empty_file(test_file_path)
        test_path = self.generate_valid_path(self.work_dir, self.share_name) + '///.testfile'
        subpaths = self.client.nlst(test_path)
        assert test_file_path in subpaths or any(os.path.basename(test_file_path) in s for s in subpaths)
        self.clean_tmp_file(test_file_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_parent(self):
        self.client.cwd(self.temp_dir_path)
        subpaths = self.client.nlst('..////')
        assert any(os.path.basename(self.temp_dir_path) in s for s in subpaths) and \
                any(os.path.basename(self.temp_file_path) in s for s in  subpaths)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_with_dot(self):
        test_file_path = self.generate_valid_path(self.work_dir, self.share_name, '.testfile')
        self.upload_empty_file(test_file_path)
        self.client.cwd(self.get_share_path())
        subpaths = self.client.nlst("-a")
        assert any(os.path.basename(test_file_path) in s for s in subpaths)
        self.clean_tmp_file(test_file_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_with_query(self):
        clean_list = []
        for i in range(100):
            test_file_path = self.generate_valid_path(self.work_dir, self.share_name, 'testfile{:04d}'.format(i))
            self.upload_empty_file(test_file_path)
            clean_list.append(test_file_path)
        subpaths = self.client.nlst(self.get_share_path() + '/testfile????')
        assert len(subpaths) == 100
        for tmp_path in clean_list:
            self.clean_tmp_file(tmp_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_nlst_dotdir(self):
        clean_list = []
        dir_path = self.generate_valid_path(self.work_dir, self.share_name, '.testdir')
        self.client.mkd(dir_path)
        for i in range(100):
            test_file_path = self.generate_valid_path(dir_path, 'testfile{:04d}'.format(i))
            self.upload_empty_file(test_file_path)
            clean_list.append(test_file_path)
        subpaths = self.client.nlst(dir_path + '/testfile????')
        assert len(subpaths) == 100
        for tmp_path in clean_list:
            self.clean_tmp_file(tmp_path)
        self.client.rmd(dir_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_list_ok(self):
        subpaths = []
        self.client.retrlines('list ' + self.get_share_path(), subpaths.append)
        subpaths = [x.split(" ")[-1] for x in subpaths]
        assert os.path.basename(self.temp_dir_path) in subpaths and os.path.basename(self.temp_file_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    def test_list_enoent(self):
        temp_dir_path = self.get_tmp_path()
        subpaths = []
        resp = self.client.retrlines('list ' + temp_dir_path, subpaths.append)
        assert subpaths == []

    @pytest.mark.base
    @pytest.mark.list
    def test_list_subsub(self):
        subsubname = get_tmpfilename('-{}'.format(self._testMethodName))
        subsubpath = self.generate_valid_path(self.temp_dir_path, subsubname)
        self.client.mkd(subsubpath)
        subpaths = []
        self.client.retrlines('list ' + self.temp_dir_path, subpaths.append)
        subpaths = [x.split(" ")[-1] for x in subpaths]
        assert subsubname in subpaths
        self.clean_tmp_dir(subsubpath)

    @pytest.mark.base
    @pytest.mark.list
    def test_list_with_arguments(self):
        l1 = l2 = l3 = l4 = l5 = []
        self.client.retrlines('list ' + self.get_share_path(), l1.append)
        self.client.retrlines('list -a ' + self.get_share_path(), l2.append)
        self.client.retrlines('list -l ' + self.get_share_path(), l3.append)
        self.client.retrlines('list -al ' + self.get_share_path(), l4.append)
        self.client.retrlines('list -la ' + self.get_share_path(), l5.append)
        assert l1 == l2 == l3 == l4 == l5
        subpaths = [x.split(" ")[-1] for x in l1]
        assert os.path.basename(self.temp_dir_path) in subpaths and os.path.basename(self.temp_file_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    def test_list_rel_path(self):
        self.client.cwd(self.get_share_path())
        l1 = []
        self.client.retrlines('list', l1.append)
        subpaths = [x.split(" ")[-1] for x in l1]
        assert os.path.basename(self.temp_dir_path) in subpaths and os.path.basename(self.temp_file_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    def test_list_glob_file(self):
        self.client.cwd(self.get_share_path())
        subpaths = []
        self.client.retrlines(f'list {TEST_PREFIX}*', subpaths.append)
        subpaths = [x.split(" ")[-1] for x in subpaths]
        assert os.path.basename(self.temp_file_path) in subpaths and os.path.basename(self.temp_dir_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    @pytest.mark.should_fail
    def test_list_glob_enoent(self):
        self.client.cwd(self.get_share_path())
        subpaths = []
        self.client.retrlines(f'list foo*', subpaths.append)
        assert subpaths == []

    @pytest.mark.base
    @pytest.mark.list
    def test_list_dash_filename(self):
        test_file_path = self.generate_valid_path(self.work_dir, self.share_name, '-testfile')
        self.upload_empty_file(test_file_path)
        subpaths = []
        self.client.retrlines('list ' + test_file_path, subpaths.append)
        subpaths = [x.split(" ")[-1] for x in subpaths]
        assert os.path.basename(test_file_path) in subpaths
        self.clean_tmp_file(test_file_path)

    @pytest.mark.base
    @pytest.mark.list
    def test_list_wildcard(self):
        self.client.cwd(self.get_share_path())
        subpaths = []
        self.client.retrlines('list *', subpaths.append)
        subpaths = [x.split(" ")[-1] for x in subpaths]
        assert os.path.basename(self.temp_file_path) in subpaths and os.path.basename(self.temp_dir_path) in subpaths

    @pytest.mark.base
    @pytest.mark.list
    def test_mlst_not_support(self):
        with pytest.raises(ftplib.error_perm, match="Unknown command"):
            self.client.voidcmd('mlst ' + self.get_share_path())

    @pytest.mark.base
    @pytest.mark.list
    def test_mlsd_not_support(self):
        with pytest.raises(ftplib.error_perm, match="Unknown command"):
            self.client.retrlines('mlsd '+ self.get_share_path(), ['type', 'size', 'perm', 'modify'])

    @pytest.mark.base
    @pytest.mark.stat
    def test_stat_dir_ok(self):
        resp = self.client.sendcmd('stat ' + self.get_share_path())
        subpaths = [x.split(" ")[-1] for x in resp.split("\n")[1:-1]]
        assert os.path.basename(self.temp_dir_path) in subpaths and os.path.basename(self.temp_file_path) in subpaths

    @pytest.mark.base
    @pytest.mark.stat
    def test_stat_file_ok(self):
        resp = self.client.sendcmd('stat ' + self.temp_file_path)
        assert os.path.basename(self.temp_file_path) in [x.split(" ")[-1] for x in resp.split("\n")[1:-1]]

    @pytest.mark.base
    @pytest.mark.stat
    def test_stat_enoent(self):
        temp_dir_path = self.get_tmp_path()
        resp = self.client.sendcmd('stat ' + temp_dir_path)
        assert [] == [x for x in resp.split("\n")[1:-1]]

    @pytest.mark.base
    @pytest.mark.stat
    def test_stat_wildcard(self):
        resp = self.client.sendcmd('stat *')
        assert [] != [x for x in resp.split("\n")[1:-1]]
