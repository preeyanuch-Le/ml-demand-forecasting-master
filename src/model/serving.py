import sys
import pandas as pd
import numpy as np

# set path to find local modules
sys.path.append("/home/ec2-user/SageMaker/ml-demand-forecasting")
from src.preprocessing.data_cleaning import clean_data


def predict_online_sales(data: pd.DataFrame, model, label_encoder: dict, feature_cols):
    """ function to predict sales with NN """
    
    features = clean_data(data[feature_cols])
    
    # Apply label encoder
    for col, vals in label_encoder.items():
        print(col)
        features[col] = features[col].map(label_encoder[col])

    features = features.fillna(0)
    
    X = np.asarray(features).astype("float64")
    y_pred = model.predict(X)
    
    data["pred"] = np.expm1(y_pred)
    data["pred"] = np.where(data["pred"] < 0, 0, data["pred"])
    return data

def predict_sales_offline(model_format: pd.DataFrame, mid_list, model, label_encoder: dict):
    """ function to predict sales with NN """
    
    # Apply label encoder
    for col, vals in label_encoder.items():
        model_format[col] = model_format[col].map(label_encoder[col])

    model_format = model_format.fillna(0)
    
    X = np.asarray(model_format).astype("float64")
    y_pred = model.predict(X)
    
    mid_list["pred"] = np.square(y_pred)
    mid_list["pred"] = np.where(mid_list["pred"] < 0, 0, mid_list["pred"])
    mid_list["warehouse"] = np.select(
        [
            mid_list["id_shop_name"] == "TH",
            mid_list["id_shop_name"] == "ID",
            mid_list["id_shop_name"] == "MY",
            mid_list["id_shop_name"] == "SG",
        ],
        ["TH", "ID", "MY", "MY"],
    )
    mid_list.rename(columns={'cluster':'store_cluster'},inplace=True)
    return mid_list