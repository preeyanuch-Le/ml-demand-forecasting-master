""" Library for short postprocessing functions """
import pandas as pd
import numpy as np


def adjust_marketing_spend_cut_my(prediction_df: pd.DataFrame) -> pd.DataFrame:
    """Decreases the units per Style in MY (online) by 25% while maintaining the size distribution"""

    total_units_by_style = (
        prediction_df.groupby(["master_style_id", "color", "warehouse"])
        .pred.sum()
        .reset_index()
        .rename(columns={"pred": "total_units"})
    )

    avg_units_my_before = total_units_by_style[
        total_units_by_style.warehouse == "MY"
    ].total_units.mean()
    print(
        f"Average forecasted units per style in MY before adjustment: {np.round(avg_units_my_before)}"
    )

    size_dist = (
        prediction_df.groupby(["master_style_id", "color", "warehouse", "size"])
        .pred.sum()
        .reset_index()
        .rename(columns={"pred": "prediction"})
    )

    size_dist = pd.merge(
        size_dist,
        total_units_by_style,
        on=["master_style_id", "color", "warehouse"],
        how="left",
    )
    size_dist["size_percent"] = size_dist.prediction / size_dist.total_units
    
    # decrease forecast by 25%
    size_dist["new_forecast"] = (
        (size_dist.size_percent * (size_dist.total_units * 0.75)).round().astype(int)
    )

    prediction_df = pd.merge(
        prediction_df,
        size_dist[["master_style_id", "color", "warehouse", "size", "new_forecast"]],
        on=["master_style_id", "color", "warehouse", "size"],
        how="left",
    )
    prediction_df.loc[
        prediction_df.warehouse == "MY", "pred_round"
    ] = prediction_df.new_forecast
    
    # Testing and logging resutling changes:
    new_total_units_per_style = (
        prediction_df.groupby(["master_style_id", "color", "warehouse"])
        .pred_round.sum()
        .reset_index()
        .rename(columns={"pred_round": "total_units"})
    )

    new_average = new_total_units_per_style[
        new_total_units_per_style.warehouse == "MY"
    ].total_units.mean()
    
    
    print(
        f"New average forecasted units per style in MY after adjustment: {np.round(new_average)}"
    )
    
    resulting_change_percent = ((np.round(new_average) / np.round(avg_units_my_before)) -1) * 100
    print(
        f"Units by style in MY were decreased by : {resulting_change_percent} %"
    )
    
    prediction_df.drop(columns=["new_forecast"], inplace=True)

    return prediction_df


def drop_indonesia(input_df):
    """ drop all rows for indonesia """
    
    is_offline = 'id_shop_name' in input_df.columns

    if is_offline:
        input_df = input_df[input_df.id_shop_name != 'ID']
    else:
        input_df = input_df[input_df.warehouse != 'ID']

    return input_df

def add_sm_slash_size(final_list, ms_with_one_size_list):
    """ Function only meant to be used adhoc. Changes forecasts one-size forecast to be S/M slash size. 
        Delets all remaining forecasts that are not S/M for the given master styles.
        
        Note: master syles in the list need to be originally marked as one-size so
        that forecats for all 6 sizes are created (will be dropped). Also check that the desired size is S/M!
        
        Args:
            final_list: dataframe with forecats
            ms_with_one_size_list: list containing master_style_ids that only have S/M forecast
        Returns:
            dataframe with adjusted predictions
    """
    
    tmp = final_list.copy()

    slash_size_rows = tmp[(tmp.master_style_id.isin(ms_with_one_size_list)) & (tmp['size'] == 'S')]
    slash_size_rows['size'] = 'S/M'
    slash_size_rows.loc[:,'pred_round'] = (
        slash_size_rows.pred_round 
        + tmp[(tmp.master_style_id.isin(ms_with_one_size_list)) & (tmp['size'] == 'M')].pred_round.values
    )
    tmp = tmp.append(slash_size_rows, ignore_index = True)
    idx_to_drop = tmp[(tmp.master_style_id.isin(ms_with_one_size_list)) & (~(tmp['size'] == 'S/M'))].index
    tmp.drop(index = idx_to_drop, axis = 0, inplace = True)
    
    return tmp