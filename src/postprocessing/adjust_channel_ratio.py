"""
##logic
#### step 0 : calculate the target for both channel at style and cdc level

for offline > need to get total offline qty per style per cdc first

* eg. FF TH (XXS,XS,S,M,L,XL) = 2,3,3,3,2,1 -> total per style = 14 -> *no of store 4 = 64 ... PF TH = 60, LN TH= 50, FF =60 -> total for TH = 234

***for example master_style_id = 114539 color = white***

TH -> online target = 206, offline taget = 480

we will transfer 141 units from offline to online qty to achieve the target

#### step 1: online adjusting
for online, 206 - total qty for TH warehouse 

1.1 calculate the target at style and cdc level

1.2 calculate size distribution

1.3 we muliple size distribution with target to get the adjusted qty at size level


#### step 2: offline adjusting

for offline, 480 - total qty for TH warehouse for all store

2.1 calculate the target at style and cdc level

2.2 calculate cluster and shop distribution (eg. TH FFF 0.27, PF 0.25, LN 0.23, FF 0.25)

2.3 we mutiply the target with shop and store cluster distribution to get the total qty for each shop and store cluster (eg. TH FFF 120)

2.4 then we divide it with no of store to get total qty per store [per style] (eg. TH FFF 120/5 = 24 per store)

2.5 calculate size distribution

2.6 finally we divide it with size distribution to get the adjusted qty at size level
"""

import pandas as pd
import numpy as np
from src.io.loaders import load_external_data

def check_channel_raio(
    online_file,
    offline_file_agg,
    adjustment_rate
):
    """check if online ratio reach 30%"""
    # getting total online qty per style per cdc
    online_df = (
        online_file.groupby(["master_style_id", "color", "warehouse"])["forecast"]
        .sum()
        .reset_index()
        .rename(columns={"forecast": "online_qty"})
    )
    
    offline_file_agg['warehouse'] = np.select([offline_file_agg['id_shop_name']=='TH',
                                         offline_file_agg['id_shop_name']=='ID',
                                         offline_file_agg['id_shop_name']=='MY',
                                         offline_file_agg['id_shop_name']=='SG'],
                                          ['TH','ID','MY','MY'])
    
    offline_df = (
        offline_file_agg.groupby(["master_style_id", "color", "warehouse"])["forecast_total"]
        .sum()
        .reset_index()
        .rename(columns={"forecast_total": "offline_qty"})
    )

    # calculating total qty by style/cdc
    target_df = pd.merge(online_df, offline_df)
    target_df['online_qty'] = np.round(target_df['online_qty'],2)
    target_df['offline_qty'] = np.round(target_df['offline_qty'],2)
    target_df["total_qty"] = target_df["online_qty"] + target_df["offline_qty"]

    avg_online_dist = target_df["online_qty"].sum() / target_df["total_qty"].sum()
    print("total online qty",  "%.2f" %target_df["online_qty"].sum())
    print("total offline qty",  "%.2f" %target_df["offline_qty"].sum())
    print("total qty", "%.2f" % target_df["total_qty"].sum())
    avg_online_dist = np.round(avg_online_dist,2)
    print("Avg online dist for this batch :", avg_online_dist)
    
    if avg_online_dist>adjustment_rate:
        print("avg online ratio for this batch is already meet the target!!!!")
        print('there will be no adjustment!!')
        target_df["unit_to_adjust"] = 0
        target_df["online_target"] = target_df['online_qty']
        
    else:
        # channel distribution
        target_df["online_dist"] = target_df["online_qty"] / target_df["total_qty"]
        target_df["offline_dist"] = target_df["offline_qty"] / target_df["total_qty"]

        # calculating 30% threshold
        target_df["online_target"] = np.round(adjustment_rate * target_df["total_qty"])
        target_df["unit_to_adjust"] = target_df["online_target"] - target_df["online_qty"]
        # we will not reduce any units for now
        target_df["unit_to_adjust"] = np.where(target_df["unit_to_adjust"]<=0,0,target_df["unit_to_adjust"])

        print("total styles in this batch :", len(target_df))
        print(
            "there are",
            len(target_df[target_df["online_dist"] < adjustment_rate]),
            "styles need to be adjusted",
        )
        print("total unit to adjust", "%.2f" % target_df["unit_to_adjust"].sum())
        target_df["offline_target"] = target_df["total_qty"] - target_df["online_target"]


    return target_df


def adjust_online_qty(
    online_file, target_df, MODEL_OUTPUT_DIR, OUTPUT_FILE_NAME
):
    # adjusting online QTY
    # get size dist
    online_file["size_dist"] = online_file["forecast"] / online_file.groupby(
        ["warehouse", "master_style_id", "color","week_id"]
    )["forecast"].transform("sum")
    
    # get week distribution
    online_file["week_dist"] = online_file["forecast"] / online_file.groupby(
        ["warehouse", "master_style_id", "color","size"]
    )["forecast"].transform("sum")

    
    # get target
    final_online = online_file.merge(
        target_df[["master_style_id", "color", "warehouse", "online_target"]],
        how="left",
    )

    # mutiply size dist with target to get adjusted qty
    final_online["pred_round_adjusted"] = np.round(
        final_online["size_dist"] * final_online["online_target"] * final_online["week_dist"],2 
    )

    # Save adjusted file to the ordersheet format 
    final_online["master_style_id"] = final_online["master_style_id"].astype(str)

    final_online.rename(
        columns={
            "forecast":'forecast_before_adjust',
            "pred_round_adjusted": "forecast",
        },
        inplace=True,
    )

    return final_online

"""
NOT in used
If needed in the future, will have to change into new store profile with SPL

"""
# def adjust_offline_qty(
#     target_df, offline_file, MODEL_OUTPUT_DIR, OUTPUT_FILE_NAME_OFFLINE
# ):
#     # calculating cluster and shop distribution
#     offline_cluster_shop = (
#         offline_file.groupby(["warehouse", "id_shop_name", "store_cluster"])[
#             "pred_round"
#         ]
#         .sum()
#         .reset_index()
#     )
#     offline_cluster_shop["cluster_shop_dist"] = offline_cluster_shop[
#         "pred_round"
#     ] / offline_cluster_shop.groupby("warehouse")["pred_round"].transform("sum")
#     offline_cluster_shop

#     # merge back cluster and shop distribution
#     final_list4 = offline_file.merge(
#         offline_cluster_shop[
#             ["warehouse", "id_shop_name", "store_cluster", "cluster_shop_dist"]
#         ],
#         how="left",
#     )

#     # get the offline target by cluster and shop
#     final_list5 = final_list4.merge(
#         target_df[["master_style_id", "color", "warehouse", "offline_target"]],
#         how="left",
#     )

#     final_list5["offline_target_by_cluster_shop"] = np.round(
#         final_list5["offline_target"] * final_list5["cluster_shop_dist"]
#     )

#     # divide by no of store to get the target per store cluster & shop
#     final_list5["no_of_store"] = np.select(
#         [
#             (final_list5["id_shop_name"] == "TH")
#             & (final_list5["store_cluster"] == "FFF"),
#             (final_list5["id_shop_name"] == "TH")
#             & (final_list5["store_cluster"] == "PF"),
#             (final_list5["id_shop_name"] == "TH")
#             & (final_list5["store_cluster"] == "LN"),
#             (final_list5["id_shop_name"] == "TH")
#             & (final_list5["store_cluster"] == "FF"),
#             (final_list5["id_shop_name"] == "MY")
#             & (final_list5["store_cluster"] == "FFF"),
#             (final_list5["id_shop_name"] == "SG")
#             & (final_list5["store_cluster"] == "LN"),
#             (final_list5["id_shop_name"] == "SG")
#             & (final_list5["store_cluster"] == "FFF"),
#         ],
#         [4, 5, 8, 5, 1, 1, 2],
#     )

#     final_list5["offline_target_by_store"] = (
#         final_list5["offline_target_by_cluster_shop"] / final_list5["no_of_store"]
#     )

#     # get size dist
#     final_list5["size_dist"] = final_list5["pred_round"] / final_list5.groupby(
#         ["id_shop_name", "store_cluster", "master_style_id", "color"]
#     )["pred_round"].transform("sum")

#     # mutiply size dist with target to get adjusted qty
#     final_list5["pred_round_adjusted"] = np.round(
#         final_list5["size_dist"] * final_list5["offline_target_by_store"]
#     )

#     """ Save adjusted file to the ordersheet format """
#     store_map = {
#         "FF": "Fun & Fashion (A)",
#         "PF": "Premium & Feminine (B)",
#         "LN": "Local & Neighbourhood ( C)",
#         "FFF": "Fashion Follower & Feminine (D)",
#     }
#     final_list5["store_cluster_full"] = final_list5["store_cluster"].map(store_map)
#     final_list5["master_style_id"] = final_list5["master_style_id"].astype(str)
#     final_list5["index"] = (
#         final_list5["master_style_id"]
#         + final_list5["color"]
#         + final_list5["size"]
#         + final_list5["id_shop_name"]
#         + final_list5["store_cluster_full"]
#     )

#     final_list5.rename(
#         columns={
#             "pred_round": "pred_round_before_online_boost",
#             "pred_round_adjusted": "pred_round",
#         },
#         inplace=True,
#     )

#     final_offline = final_list5[
#         [
#             "master_style_id",
#             "color",
#             "id_shop_name",
#             "store_cluster",
#             "store_cluster_full",
#             "released_collection_name",
#             "size",
#             "index",
#             "pred_round_before_online_boost",
#             "pred_round",
#         ]
#     ]
#     final_offline2 = final_offline.drop("pred_round_before_online_boost", axis=1)

#     """ Save file  """
#     writer = pd.ExcelWriter(
#         f"{MODEL_OUTPUT_DIR}{OUTPUT_FILE_NAME_OFFLINE}", engine="xlsxwriter"
#     )

#     final_offline.to_excel(writer, sheet_name="offline", index=False)  # sum

#     for sub in set(final_offline2["released_collection_name"].to_list()):
#         final_offline2[final_offline2["released_collection_name"] == sub].to_excel(
#             writer, sheet_name=sub.replace(" ", "")[:24], index=False
#         )

#     writer.save()

#     print(f"file saved to {MODEL_OUTPUT_DIR}{OUTPUT_FILE_NAME_OFFLINE}")
#     return final_offline
