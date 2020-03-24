import gzip
import os
import shutil

from pandas import DataFrame, read_csv

usage: str = """
Usage:
python3 main.py
"""


def get_dataset(name: str):
    if name != "":
        return f"{base_path}_{name}.csv"
    return f"{base_path}.csv"


def unzip(name: str):
    if os.path.isfile(name):
        return
    with gzip.open(f"{name}.gz", 'r') as file:
        with open(name, 'wb') as new_file:
            shutil.copyfileobj(file, new_file)


base_path = "data/code_review"
datasets = [
    get_dataset("comments"),
    get_dataset("patch_set_approvals"),
    get_dataset("patch_set_comments"),
    get_dataset("patch_set_files"),
    get_dataset("patch_sets"),
    f"{base_path}s.csv"
]
comments_path = datasets[0]
patch_set_path = datasets[1]
patch_set_approvals_path = datasets[2]
patch_set_comments_path = datasets[3]
patch_set_files_path = datasets[4]
patch_sets_path = datasets[5]

if __name__ == "__main__":
    # if not sys.argv[1]:
    #     raise ValueError("The first argument must a ")
    for dataset in datasets:
        unzip(dataset)
    comments: DataFrame = read_csv(comments_path)
    patch_set: DataFrame = read_csv(patch_set_path)
    patch_set_approvals: DataFrame = read_csv(patch_set_approvals_path)
    patch_set_comments: DataFrame = read_csv(patch_set_comments_path)
    patch_set_files: DataFrame = read_csv(patch_set_files_path)
    patch_sets: DataFrame = read_csv(patch_sets_path)
