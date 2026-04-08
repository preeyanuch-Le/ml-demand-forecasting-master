import pandas as pd


def load_data(marketing_spend_data_path, lookbook_spend_collection_path):
    marketing_data = pd.DataFrame(pd.read_csv(marketing_spend_data_path))
    lookbook_collection = pd.DataFrame(pd.read_csv(lookbook_spend_collection_path))
    return marketing_data, lookbook_collection
