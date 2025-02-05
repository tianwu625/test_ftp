
import os
import tempfile

GLOBAL_TIMEOUT = 5
BUFSIZE = 1024
INTERRUPTED_TRANSF_SIZE = 32768

TEST_PREFIX = f'ftptest-tmp-{os.getpid()}-'

def get_tmpfilename(suffix=""):
    name = tempfile.mktemp(prefix=TEST_PREFIX, suffix=suffix)
    return os.path.basename(name)

def touch_filename(fpath):
    with open(fpath, 'wb') as f:
        pass
