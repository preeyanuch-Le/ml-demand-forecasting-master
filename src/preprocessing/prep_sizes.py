import pandas as pd
import numpy as np
#from src.utils.function_lib import map_size, change_size


# map old sized to new sizes
def map_size(row):
    if row["old_size"] == "XS":
        return "XXS"
    if row["old_size"] == "S":
        return "XS"
    if row["old_size"] == "M":
        return "S"
    if row["old_size"] == "L":
        return "M"
    if row["old_size"] == "XL":
        return "L"
    if row["old_size"] == "XXL":
        return "XL"
    else:
        return row["old_size"]
    
    
def change_slash_size(row):
    if row["size"] == "XXS":
        return "XXS/XS"
    if row["size"] == "XS":
        return "XXS/XS"
    if row["size"] == "S":
        return "S/M"
    if row["size"] == "M":
        return "S/M"
    if row["size"] == "L":
        return "L/XL"
    if row["size"] == "XL":
        return "L/XL"
