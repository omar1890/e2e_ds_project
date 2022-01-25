# Constants
DATA_PATH = "E:/ITI -AI pro/ثقة بالنفس/End_to_End_Data_Sceince_Project/data/raw/survey_results_public.csv"
EXPORT_PATH = "E:/ITI -AI pro/ثقة بالنفس/End_to_End_Data_Sceince_Project/data/processed/1_preprocessed_df.pkl"

REPLACE_DICT = {
    'YearsCodePro': {'Less than 1 year': 0, 'More than 50 years': 51}, 
    'YearsCode':    {'Less than 1 year': 0, 'More than 50 years': 51}, 
    'Age1stCode':   {'Older than 85':86,    'Younger than 5 years':4}
}

# Load Packages
import pandas as pd
import numpy as np
import logging
import pickle
  # Sub functions
def is_splittable(pd_series, delimiter):
    """ Check if results multiple should be splitted - Returns boolean """    
    return pd_series.str.contains(delimiter)

def split_answer(pd_series, delimiter):
    """Functiion to split single answer"""
    return pd_series.str.split(delimiter)

def split_answers(data_series, delimiter=";"):
    """ 
    Split multiple answers in a single string 
    to a list of single strings each represnting a single answers 

    Parameters:
    * data_series (pd.Series): String series with answers 
    * delimiter (string): Another decimal integer 
                          Defaults to ";"

    Returns: (pd.Series): If column contains 
    """


    # -----------------------
    # Check if multiple answers exist - if none: return original 
    splittable_values = is_splittable(data_series,delimiter)
    if not splittable_values.any():
        return data_series
    
    # Else, split each value to a list 
    modified_series = split_answers(data_series, delimiter)

    # Replace NAs with empty lists 
    mask_null = modified_series.isnull()
    modified_series.loc[mask_null] = modified_series.loc[mask_null].apply(lambda x : [])

    return modified_series


raw_df = pd.read_csv(DATA_PATH)
df = raw_df.copy()
print('here')
for col, replacement in REPLACE_DICT.items():
    df[col] = df[col].replace(replacement).astype(np.float32)

print('here1')

object_cols = df.select_dtypes(include='object').columns.tolist()
for col in object_cols[0:25]:
    df[col] = split_answers(df[col])

print('here2')

for col in object_cols[25:]:
    df[col] = split_answers(df[col])

print('here2')

df.to_pickle(EXPORT_PATH)
# df.to_pickle(EXPORT_PATH)
