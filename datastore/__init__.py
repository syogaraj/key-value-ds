import fcntl, os
import sys
import config

from .datastore import DataStore


def get_file_name() -> str:
    """
    Creates a unique file name for datastore by appending epoch timestamp to the file name
    :return:
    """
    import time
    curr_ts = int(time.time()*1000)
    return "LOCAL_STORAGE_{}".format(curr_ts)


def get_instance(file_name=None) -> DataStore:
    if file_name is None:
        file_name = get_file_name()
    full_file_name = f"{config.LOCAL_STORAGE_PREPEND_PATH}/{file_name}"
    file_descriptor = os.open(full_file_name, os.O_CREAT | os.O_RDWR)

    """
        Try to acquire file lock. 
        If file is already locked, `fcntl` raises BlockingIOError which can be used to handle exceptions.
    """
    try:
        print(f"Acquiring file lock on {file_name}")
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise ValueError(f"Resource '{file_name}' is already locked'")
    except Exception:
        raise
    else:
        print(f"File lock acquired on {file_name}")

    """
        File lock acquired.
        Fill it with bytes from config.
    """
    if not os.path.isfile(full_file_name) or os.fstat(file_descriptor).st_size == 0:
        with open(full_file_name, 'ab') as f:
            string = "{}"
            f.write(bytes(string.encode('ascii')))
            string_size = sys.getsizeof(string)
            f.write((config.MAX_LOCAL_STORAGE_SIZE - string_size) * b'\0')
    return DataStore(file_descriptor)
