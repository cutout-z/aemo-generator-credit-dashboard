"""macOS phys_footprint (current and lifetime peak) via proc_pid_rusage(RUSAGE_INFO_V4)."""
import ctypes, os
_lib = ctypes.CDLL("/usr/lib/libproc.dylib")
class _RU(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16), ("v", ctypes.c_uint64 * 35)]
def footprint(pid=None):
    ru = _RU()
    if _lib.proc_pid_rusage(pid or os.getpid(), 4, ctypes.byref(ru)) != 0:
        raise OSError("proc_pid_rusage failed")
    return {"rss": ru.v[6], "footprint": ru.v[7], "lifetime_max_footprint": ru.v[28]}
