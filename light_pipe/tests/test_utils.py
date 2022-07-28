import pytest

from ..utils import *


def test_make_dir_path():
    bad_filepath = 'test_data/bad_dir'
    with pytest.raises(Exception):
        make_dir_path(bad_filepath)

    good_filepath = 'test_data/bad_dir/'
    make_dir_path(good_filepath)
    assert os.path.exists(good_filepath)