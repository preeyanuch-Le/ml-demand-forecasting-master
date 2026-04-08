import pandas as pd


def load_data(prod_clus_path, store_clusters_path):

    prod_clus = pd.DataFrame(pd.read_csv(prod_clus_path, sep=","))
    store_clusters = pd.DataFrame(pd.read_csv(store_clusters_path, sep=","))

    return prod_clus, store_clusters
