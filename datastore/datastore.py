import threading, mmap, json, sys, time

import config


def is_legit(val, type="key"):
    if type == "key":
        return len(val) <= config.MAX_KEY_LEN
    elif type == "value":
        if isinstance(val, dict):
            return sys.getsizeof(val) <= config.MAX_VALUE_SIZE
        return False


class DataStoreVO:
    def __init__(self, value, ttl):
        self.value = value
        self.ttl = ttl
        self.created_at = int(time.time()*1000)

    def is_expired(self):
        if self.ttl is None:
            return False
        curr_ts = int(time.time()*1000)
        return (curr_ts - self.created_at) < self.ttl*1000


class DataStore:
    def __init__(self, file_descriptor, *args, **kwargs):
        self.__fd = file_descriptor
        self.__mmap = self._get_mmaped_fd()
        self.__data = {}
        self.__lock = threading.RLock()
        self._read_data()

    def _get_mmaped_fd(self) -> mmap.mmap:
        try:
            return mmap.mmap(self.__fd, config.MAX_LOCAL_STORAGE_SIZE, access=mmap.ACCESS_WRITE)
        except mmap.error:
            raise

    def _read_data(self) -> None:
        raw_data = self.__mmap[:].decode('ascii').rstrip('\0')
        self.__data = json.loads(raw_data)

    def create(self, key, value, ttl=None) -> None:
        with self.__lock:
            if key in self.__data:
                raise ValueError(f"Key '{key}' already present.")
            if is_legit(key, type="key") and is_legit(value, type="value"):
                if ttl is not None:
                    try:
                        ttl = int(ttl)
                    except:
                        raise ValueError(f"Time-to-live {ttl} must be an integer value.")
                value_tuple = DataStoreVO(value, ttl)
                self.__data[key] = value_tuple
                self.flush()

    def delete(self, key) -> None:
        with self.__lock:
            if key not in self.__data:
                raise ValueError(f"Key [{key}] not in datastore.")
            del self.__data[key]
            self.flush()

    def get(self, key) -> dict:
        with self.__lock:
            if key not in self.__data:
                raise ValueError(f"Key [{key}] not in datastore.")
            value = self.__data.get(key)  # type: DataStoreVO
            if value.is_expired():
                del self.__data[key]
                self.flush()
                raise ValueError(f"Key [{key}] Time-to-live expired.")
            return value.value

    def flush(self) -> None:
        self.__mmap.write(bytes(str(self.__data).encode()))

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value, ttl=None):
        return self.create(key, value, ttl)
