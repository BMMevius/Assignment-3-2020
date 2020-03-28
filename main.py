import gzip
import os
import shutil
from time import time

from pandas import DataFrame, read_csv, Series
from sklearn import linear_model
import statsmodels.api as sm
import matplotlib.pyplot as plt

usage: str = """
Usage:
python3 main.py
"""


now = int(time())


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


def show_scatter_plot(variable: str):
    plt.scatter(predictor_variables[variable], review_duration["review_duration"], color='red')
    plt.title(f"{variable} Vs review_duration", fontsize=14)
    plt.xlabel(variable, fontsize=14)
    plt.ylabel("review_duration", fontsize=14)
    plt.grid(True)
    plt.show()


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
    code_reviews["review_tenure"] = (now - code_reviews["first_review"]) / 86400
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
    ).fillna(now)
    code_reviews["change_tenure"] = (now - code_reviews["first_change"]) / 86400
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
    ).fillna(now)
    code_reviews["approval_tenure"] = (now - code_reviews["first_approval"]) / 86400

    # predictor_variables = code_reviews[["id"]]
    predictor_variables = DataFrame()
    predictor_variables["change_tenure"] = code_reviews["change_tenure"]
    # predictor_variables["review_tenure"] = code_reviews["change_activity"]
    predictor_variables["review_tenure"] = code_reviews["review_tenure"]
    predictor_variables["review_activity"] = code_reviews["review_activity"]
    predictor_variables["approval_tenure"] = code_reviews["approval_tenure"]
    # predictor_variables["review_activity"] = code_reviews["approval_activity"]
    for column_name in list(predictor_variables):
        if column_name == 'id':
            continue
        show_scatter_plot(column_name)

    predictor_variables = sm.add_constant(predictor_variables)

    model = sm.OLS(review_duration["review_duration"], predictor_variables).fit()
    predictions = model.predict(predictor_variables)

    print_model = model.summary()
    print(print_model)

    #
    # # with sklearn
    # regr = linear_model.LinearRegression()
    # regr.fit(predictor_variables, review_duration["review_duration"])
    #
    # print('Intercept: \n', regr.intercept_)
    # print('Coefficients: \n', regr.coef_)
    #
    # # prediction with sklearn
    # New_Interest_Rate = 2.75
    # New_Unemployment_Rate = 5.3
    # print('Predicted Stock Index Price: \n', regr.predict([[New_Interest_Rate, New_Unemployment_Rate]]))
    #
    # # with statsmodels
    # X = sm.add_constant(X)  # adding a constant
    #
    # model = sm.OLS(Y, X).fit()
    # predictions = model.predict(X)
    #
    # print_model = model.summary()
    # print(print_model)

    pass
