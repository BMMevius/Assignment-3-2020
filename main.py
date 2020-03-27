import gzip
import os
import shutil

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
patch_set_path = datasets[5]

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

    groups = patch_sets.groupby("author_username")

    first_created_on = groups["createdOn"].min()
    patch_sets["first_change"] = patch_sets["author_username"].map(first_created_on)
    patch_sets["tenure"] = (patch_sets["createdOn"] - patch_sets["first_change"]) / 86400
    patch_sets["change_activity"] = patch_sets.sort_values("createdOn").groupby("author_username").cumcount()
    patch_set_approvals["first_review"] = patch_set_approvals["by_username"].map(
        patch_set_approvals.groupby("by_username")["grantedOn"].min()
    )
    patch_set_approvals["review_activity"] = patch_set_approvals.sort_values("grantedOn").groupby("by_username").cumcount()
    patch_set_approvals["first_approval"] = patch_set_approvals["by_username"].map(
        patch_set_approvals.loc[patch_set_approvals["value"].isin([-2, 2])].groupby("by_username")["grantedOn"].min()
    ).fillna(0)
    patch_set_approvals["approval_activity"] = patch_set_approvals.sort_values("grantedOn").groupby("by_username")["value"].apply(
        lambda x: x.isin([-2, 2]).astype(int).fillna(1).cumsum()
    )

    columns = ["number_cr", "tenure", "change_activity", "review_tenure", "review_activity", "approval_tenure", "approval_activity"]
    predictor_variables = DataFrame(columns=columns)
    for index, row in patch_sets.iterrows():
        new_row = dict.fromkeys(columns)
        new_row[columns[0]] = row[columns[0]]
        new_row[columns[1]] = row[columns[1]]
        new_row[columns[2]] = row[columns[2]]
        review_series: Series = patch_set_approvals.loc[
            patch_set_approvals["by_username"] == row["author_username"]
        ]["first_review"]
        if review_series.empty:
            new_row[columns[3]] = 0
        else:
            new_row[columns[3]] = max(row["createdOn"] - review_series.iloc[0], 0)
        new_row[columns[4]] = patch_set_approvals.loc[
            (patch_set_approvals["by_username"] == row["author_username"]) &
            (row["createdOn"] <= patch_set_approvals["grantedOn"])
        ]["review_activity"].max()
        activity_series: Series = patch_set_approvals.loc[
            patch_set_approvals["by_username"] == row["author_username"]
        ]["first_approval"]
        if activity_series.empty:
            new_row[columns[5]] = 0
        else:
            new_row[columns[5]] = max(row["createdOn"] - activity_series.iloc[0], 0)
        new_row[columns[6]] = patch_set_approvals.loc[
            (patch_set_approvals["by_username"] == row["author_username"]) &
            (row["createdOn"] <= patch_set_approvals["grantedOn"])
        ]["approval_activity"].max()
        predictor_variables = predictor_variables.append(new_row, ignore_index=True)

    review_duration = DataFrame(columns=[])

    pass
