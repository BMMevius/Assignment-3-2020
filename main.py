import gzip
import os
import shutil
from time import time

from pandas import DataFrame, read_csv, Series

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
patch_set_approvals_path = datasets[1]
patch_set_comments_path = datasets[2]
patch_set_files_path = datasets[3]
patch_sets_path = datasets[4]
code_reviews_path = datasets[5]

if __name__ == "__main__":
    # if not sys.argv[1]:
    #     raise ValueError("The first argument must a ")
    for dataset in datasets:
        unzip(dataset)
    comments: DataFrame = read_csv(comments_path)
    code_reviews: DataFrame = read_csv(code_reviews_path)
    patch_set_approvals: DataFrame = read_csv(patch_set_approvals_path)
    patch_set_comments: DataFrame = read_csv(patch_set_comments_path)
    patch_set_files: DataFrame = read_csv(patch_set_files_path)
    patch_sets: DataFrame = read_csv(patch_sets_path)

    review_duration = code_reviews[["id"]]
    review_duration["review_duration"] = code_reviews["lastUpdated"] - code_reviews["createdOn"]

    code_reviews["first_review"] = code_reviews["owner_username"].map(
        code_reviews.groupby("owner_username")["createdOn"].min()
    )
    code_reviews["review_tenure"] = (int(time()) - code_reviews["first_review"]) / 86400
    code_reviews["review_activity"] = code_reviews.sort_values("createdOn").groupby("owner_username").cumcount()
    columns = [
        "id",
        "change_tenure",
        "change_activity",
        "review_tenure",
        "review_activity",
        "approval_tenure",
        "approval_activity"
    ]

    code_reviews["first_change"] = code_reviews["owner_username"].map(
        patch_sets.groupby("uploader_username")["createdOn"].min()
    ).fillna(int(time()))
    code_reviews["change_tenure"] = (int(time()) - code_reviews["first_change"]) / 86400
    patch_sets["change_activity"] = patch_sets.sort_values("createdOn").groupby("uploader_username").cumcount()
    # patch_sets.sort_values("createdOn", inplace=True)
    # groups = patch_sets.groupby("uploader_username")
    # code_reviews["change_activity"] = 0
    # for index, row in code_reviews.iterrows():
    #     group = groups.get_group(row["owner_username"])
    #     code_reviews[index, "change_activity"] = group.loc[
    #         group["createdOn"] < row["lastUpdated"]
    #     ]["change_activity"].max()

    patch_set_approvals["is_approval"] = patch_set_approvals.isin({"value": [-2, 2]})["value"]
    code_reviews["first_approval"] = code_reviews["owner_username"].map(
        patch_set_approvals.loc[patch_set_approvals["value"].isin([-2, 2])].groupby("by_username")["grantedOn"].min()
    ).fillna(int(time()))
    code_reviews["approval_tenure"] = (int(time()) - code_reviews["first_approval"]) / 86400

    predictor_variables = code_reviews[["id"]]
    predictor_variables["review_tenure"] = code_reviews["change_tenure"]
    # predictor_variables["review_tenure"] = code_reviews["change_activity"]
    predictor_variables["review_tenure"] = code_reviews["review_tenure"]
    predictor_variables["review_activity"] = code_reviews["review_activity"]
    predictor_variables["review_activity"] = code_reviews["approval_tenure"]
    # predictor_variables["review_activity"] = code_reviews["approval_activity"]



    pass
