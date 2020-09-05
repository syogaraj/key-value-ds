import pytest

from key_value_ds import get_instance, get_file_name, DataStore


def test_get_instance():
    assert isinstance(get_instance('test_file'), DataStore)


def test_get_instance_on_same_file():
    with pytest.raises(BlockingIOError):
        get_instance('test_file')


def test_get_uniq_file_name():
    file_name = get_file_name()
    file_name1 = get_file_name()
    assert file_name != file_name1
