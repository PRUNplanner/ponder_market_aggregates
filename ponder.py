from typing import List
import pandas as pd
import glob
import os
import sys

ponder_base: str = "https://ponder.shurelia.de/prun/cx/price_"

int_columns: List[str] = ["AskCount", "Supply", "BidCount", "Demand", "Traded"]
pd_column_to_int = dict.fromkeys(int_columns, "int")

materials: List[str] = pd.read_csv("material_list.csv")["Ticker"].to_list()
field_order: List[str] = [
    "MaterialTicker",
    "ExchangeCode",
    "MMBuy",
    "MMSell",
    "PriceAverage",
    "AskCount",
    "Ask",
    "Supply",
    "BidCount",
    "Bid",
    "Demand",
    "Timestamp",
    "Price",
    "High",
    "AllTimeHigh",
    "Low",
    "Traded",
    "VolumeAmount",
    "NarrowPriceBandLow",
    "NarrowPriceBandHigh",
    "WidePriceBandLow",
    "WidePriceBandHigh",
    "AllTimeLow",
]


def rearrange_ponder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[field_order]


def request_ponder_price(material: str) -> pd.DataFrame | None:
    try:
        ponder_url: str = f"{ponder_base}{material}.csv"
        df = pd.read_csv(ponder_url)

        # remove first Column containing Ponder Id
        df = df.iloc[:, 1:]
        df = df.fillna(0)

        # convert certain columns to int
        df = df.astype(pd_column_to_int)

        # add missing fields from ponder
        df["MMBuy"] = 0
        df["MMSell"] = 0
        df["Timestamp"] = df["PriceTimeEpochMs"]

        # remove empty Timestamp row
        df.drop(df[df.Timestamp == 0].index, inplace=True)

        return df
    except FileNotFoundError:
        print("File not found")
        return None
    except pd.errors.EmptyDataError:
        print("File is empty")
        return None
    except Exception as e:
        print(f"Unknown exception: {ponder_url}")
        print(e)
        return None


def save_pd_to_csv(material: str, df: pd.DataFrame) -> str | None:
    try:
        df.to_csv("csv/price_" + material + ".csv", index=False, header=True)
        return "Saved: " + material
    except Exception as e:
        print(e)
        return None


def combine_csvs(startwith: str) -> None:
    joined_files = os.path.join("csv", startwith + "_*.csv")
    joined_list = glob.glob(joined_files)

    # join files
    try:
        df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)

        # remove empty Timestamp row
        df.drop(df[df.Timestamp == 0].index, inplace=True)

        df.to_csv("csv_merged/" + startwith + ".csv", index=False, header=True)
    except Exception as e:
        print(e)


def get_and_combine():
    materials_amount: int = len(materials)
    material_counter: int = 1

    for material in materials:
        df: None | pd.DataFrame = request_ponder_price(material)

        if df is not None:
            df = rearrange_ponder_columns(df=df)
            save_status = save_pd_to_csv(material=material, df=df)

            if save_status is None:
                print(f"{material_counter}/{materials_amount}: FAILED {material}")
            else:
                print(f"{material_counter}/{materials_amount}: {material}")

        material_counter += 1

    combine_csvs(startwith="price")


def combine():
    combine_csvs(startwith="price")


if __name__ == "__main__":
    if "--combine" in sys.argv:
        combine()
    else:
        get_and_combine()
