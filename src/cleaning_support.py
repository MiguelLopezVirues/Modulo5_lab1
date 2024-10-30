import pandas as pd

import numpy as np

def find_outliers(serie: pd.Series):
    q1, q3 = np.percentile(serie, [25, 75])
    iqr = q3 - q1
    outlier_mask = (serie < (q1 - iqr*1.5)) | (serie > (q3 + iqr*1.5))
    return outlier_mask

def group_calculate_outliers(df,column_list):
    # calculate q1 and q3 through describe. Assign lower and upper bounds
    group_bounds = (df.groupby(column_list)["value"].describe()
                        .assign(iqr1_5=lambda x: (x["75%"]-x["25%"])*1.5, lower_bound= lambda x:x["25%"]-x["iqr1_5"],upper_bound= lambda x:x["75%"]+x["iqr1_5"])).reset_index()

    # define columns for the merging group_bounds df
    group_columns_selected = column_list + ["lower_bound","upper_bound"]

    # merge to input df
    df = df.merge(group_bounds[group_columns_selected], on=column_list)

    # create and return boolean outliers column
    outlier_mask = (df["value"] < df["lower_bound"]) | (df["value"] > df["upper_bound"])
    return outlier_mask.astype(int)