from cdp_interface import CDPInterface
import datetime
import credentials as crd
import environments as env
import competitor_data as comp
import pandas as pd
import pathlib
import re

from sharepoint_interface import get_sharepoint_interface


REPOSITORY = "/sites/RetailPricing/Shared%20Documents/General/Competitive%20Intel/Competitor%20PDF%20Upload/"
LOCAL_REPOSITORY = "sharepoint_interface/local_repository/"

def set_column_types(df):
    df["product_number"] = df["product_number"].astype("string")
    df["formula_code"] = df["formula_code"].astype("string")
    df["product_name"] = df["product_name"].astype("string")
    df["ref_col"] = df["ref_col"].astype("string")
    df["unit_weight"] = df["unit_weight"].astype("string")
    df["product_form"] = df["product_form"].astype("string")
    df["fob_or_dlv"] = df["fob_or_dlv"].astype("string")
    df["price_change"] = df["price_change"].astype("float64")
    df["single_unit_list_price"] = df["single_unit_list_price"].astype("float64")
    df["full_pallet_list_price"] = df["full_pallet_list_price"].astype("float64")
    df["pkg_bulk_discount"] = df["pkg_bulk_discount"].astype("float64")
    df["best_net_list_price"] = df["best_net_list_price"].astype("float64")
    df["species"] = df["species"].astype("string")
    df["plant_location"] = df["plant_location"].astype("string")
    df["date_inserted"] = df["date_inserted"].astype("string")
    return df


def get_price_list_in_db(location, effective_date):
    cdp = CDPInterface(env.production, crd.process_account)
    query = pathlib.Path("competitor_data/sql_queries/price_list.sql").read_text()
    query = query.replace("@location", location)
    query = query.replace("@effective_date", effective_date) ## effective_date.strftime("%Y-%m-%d")
    current_data = cdp.select(query)
    current_data["source"] = "db"
    current_data = set_column_types(current_data)
    return current_data


def check_if_data_exists_and_reconciliate(price_list, location, effective_date):
    cdp = CDPInterface(env.production, crd.process_account)
    
    current_data = get_price_list_in_db(location, effective_date)
    merged_data = pd.concat([price_list, current_data])
    
    only_new_records = merged_data.drop_duplicates(subset=["product_number", 
    "formula_code",
    "product_name",
    "unit_weight",
    "product_form",
    "fob_or_dlv",
    "price_change",
    "single_unit_list_price",
    "full_pallet_list_price",
    "pkg_bulk_discount",
    "best_net_list_price",
    "species",
    "plant_location",
    "date_inserted"], keep=False)
    
    only_new_records = only_new_records[only_new_records["source"] == "pdf"]
    
    return only_new_records


def get_competitor_data(file_path):
    price_list = comp.get_purina_price_list(file_path)
    location = comp.get_purina_location(file_path)
    effective_date = comp.get_purina_effective_date(file_path)
    
    return {
        "price_list": price_list,
        "location": location,
        "effective_date": effective_date
    }


def get_pending_files(sp_interface):
    files = sp_interface.files_in_folder(REPOSITORY)
    print(f"Archivos en la carpeta {REPOSITORY}: {files}")
    return files
    
def correct_file_name(val):
    val = str(val).lower()
    val = re.sub('^(0){2,}', "", val)
    val = re.sub('^[" "-]+', "", val)
    val = re.sub('[\r\n\r\t\u00a0]+', ' ', val)
    val = re.sub('( ){2,}', ' ', val)
    val = val.strip()
    val = val.replace(" ", "_")
    val = val.replace(".", "_")
    val = val.replace("-", "_")
    return val
    

def process_pending_files():
    cdp = CDPInterface(env.production, crd.process_account)
    sp = get_sharepoint_interface("retailpricing")
    pending_files = get_pending_files(sp)
    total_file_count = len(pending_files)
    for counter, file in enumerate(pending_files, 1):
        file_name = correct_file_name( pathlib.Path(file["file_name"]).stem )
        print(f"file name: {file_name}")
        print(f"{counter}/{total_file_count} {file}")
        
        print("downloading file...")
        file_local_path = sp.download_file(file["file_path"], LOCAL_REPOSITORY)
        print(f"file downloaded: {file_local_path}")
        
        print("processing file...")
        comp_data_dict = get_competitor_data(file_local_path)
        
        print(comp_data_dict["price_list"])
        
        price_list = check_if_data_exists_and_reconciliate(comp_data_dict["price_list"], comp_data_dict["location"], comp_data_dict["effective_date"])
        print("price list after check if data exists")
        print(price_list)
        price_list = price_list.drop("source", axis=1)
        price_list = set_column_types(price_list)
        if price_list.shape[0] > 0:
            print(price_list)
            if cdp.upload_data(price_list, "comp_price_grid", file_name):
                print(f"{file} uploaded successfully to database.")
        
                sp.delete_file(file["file_path"])
                print(f"file deleted from SharePoint folder: {file_name}")
        else:
            print("empty dataframe. Data already in database.")
            sp.delete_file(file["file_path"])
            print(f"file deleted from SharePoint folder: {file_name}")

    print("Done.")

    

if __name__ == "__main__":
    process_pending_files()