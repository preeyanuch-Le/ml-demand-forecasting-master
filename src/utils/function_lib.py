import math
from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


def normal_round(n):
    if n - math.floor(n) < 0.5:
        return math.floor(n)
    return math.ceil(n)


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
    if row["old_size"] == "XS/S":
        return "XXS/XS"
    if row["old_size"] == "M/L":
        return "S/M"
    if row["old_size"] == "XL/XXL":
        return "L/XL"
    else:
        return row["old_size"]


def change_slash_size(row):
    if row["size"] == "XS":
        return "XS/S"
    if row["size"] == "S":
        return "XS/S"
    if row["size"] == "M":
        return "M/L"
    if row["size"] == "L":
        return "M/L"
    if row["size"] == "XL":
        return "XL/XXL"
    if row["size"] == "XXL":
        return "XL/XXL"
