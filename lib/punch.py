import logging
import ctypes
import ctypes.util
import os
from xmlrpc.client import Boolean

c_off_t = ctypes.c_int64

def make_fallocate():
    libc_name = ctypes.util.find_library('c')
    libc = ctypes.CDLL(libc_name)

    _fallocate = libc.fallocate
    _fallocate.restype = ctypes.c_int
    _fallocate.argtypes = [ctypes.c_int, ctypes.c_int, c_off_t, c_off_t]

    del libc
    del libc_name

    def fallocate(fd, mode, offset, len_):
        res = _fallocate(fd.fileno(), mode, offset, len_)
        if res != 0:
            raise IOError(res, 'fallocate')

    return fallocate

fallocate = make_fallocate()
del make_fallocate

FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def punch(filename, verbose=False):
    blocksize = 4096
    if verbose:
        print("processing", filename)
    with open(filename, 'rb+') as f:
        offset = 0
        while True:
            buf = f.read(blocksize)
            if buf == b"":
                break
            hole = b"\x00" * len(buf)
            if buf == hole:
                if verbose:
                    print("punching hole at offset", offset, "length", len(buf))
                fallocate(f, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
                          offset, len(buf))
            offset = offset + blocksize
        f.flush()
        os.fsync(f.fileno())
            
def punchhole(filePath, offset, length, verbose=False) -> Boolean:
    try:
        if verbose:
            print("punching hole at offset", offset, "length", length)
        with open(filePath, 'r+') as file:
            fallocate(file, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, offset, length)
        return True
    except Exception as e:
        logging.error(e)
        return False;
