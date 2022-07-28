import json
import os
import pickle
import subprocess
from typing import Optional, Union


def check_path_exists(*filepaths) -> None:
    for filepath in filepaths:
        if not os.path.exists(filepath):
            raise FileNotFoundError


def overwrite_file(filepath: str, overwrite: Optional[bool] = True,
                   warn_file_exists: Optional[bool] = True) -> None:
    if os.path.exists(filepath):
        if not overwrite:
            raise FileExistsError
        elif warn_file_exists:
            print("File " + filepath + " will be overwritten.")


def make_dir_path(filepath: str) -> None:
    assert str(filepath)[-1] == '/', "Directory path must end with forward slash."
    if not os.path.exists(filepath):
        os.makedirs(os.path.dirname(filepath))


def exec_command(com_string: str) -> None:
    subprocess.check_call(com_string, shell=True)


def save_dict_json(dictionary: dict, dict_path: str) -> None:
    with open(dict_path, 'w') as fp:
        json.dump(dictionary, fp)


def save_to_pkl(obj: object, save_path: Union[str, os.PathLike]) -> None:
    # make_dir_path(os.path.save_path)
    # Pickle
    with open(save_path, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_from_pkl(load_path: Union[str, os.PathLike]) -> object:
    with open(load_path, 'rb') as f:
        obj = pickle.load(f)
    return obj
