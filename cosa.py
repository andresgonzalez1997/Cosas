import datetime
import pandas as pd
import tabula
import re

# Define las columnas ordenadas según el layout de Statesville
def default_columns(df):
    return df[[
        "product_number",
        "formula_code",
        "product_name",
        "product_form",
        "unit_weight",
        "pallet_quantity",
        "stocking_status",
        "min_order_quantity",
        "lead_time_days",
        "fob_or_dlv",
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price",
        "species",
        "plant_location",
        "date_inserted",
        "source"
    ]]

# Marca la fuente como "pdf"
def source_columns(df):
    df["source"] = "pdf"
    return df

# Extrae unit_weight desde el nombre del producto si no está explícito
def find_unit_weight(df):
    for index, row in df.iterrows():
        if not re.search("LB", str(row["unit_weight"])):
            search_result = re.findall("\d*\s*LB", str(row["product_name"]))
            if search_result:
                df.at[index, "unit_weight"] = search_result[0]
    return df

# Convierte valores como "34.20-" en -34.20
def correct_negative_value(value):
    if str(value).endswith("-"):
        return float(str(value).replace("-", "")) * -1
    try:
        return float(value)
    except:
        return value

# Aplica la corrección a columnas de precios
def correct_negative_value_in_price_list(df):
    for col in df.columns[10:16]:
        df[col] = df[col].apply(correct_negative_value)
    return df

# Extrae la fecha efectiva desde zona específica
def effective_date(file_path):
    table = tabula.read_pdf(file_path, pages=1, area=[54,10,82,254])
    results = re.findall(r"\d{2}/\d{2}/\d{2}", str(table[0]))
    if results:
        date = datetime.datetime.strptime(results[0], "%m/%d/%y").date()
        return date.strftime("%Y-%m-%d")
    return None

# Extrae la ubicación de planta desde zona específica
def plant_location(file_path):
    table = tabula.read_pdf(file_path, pages=1, area=[0,500,40,700])
    location = str(table[0]).split("\n")[0].strip().replace(",", "").upper()
    return location

# Agrega columnas auxiliares
def add_effective_date(df, file_path):
    df["date_inserted"] = effective_date(file_path)
    return df

def add_plant_location(df, file_path):
    df["plant_location"] = plant_location(file_path)
    return df

# Detecta encabezados de especie (texto sin número) y los usa para categorizar cada fila
def add_species_column(df):
    species = None
    df["species"] = None
    for index, row in df.iterrows():
        if re.match(r"\d", str(row[0])) is None:
            species = str(row[0]).replace(",", "").upper()
            df = df.drop(index, axis=0)
        else:
            df.loc[index, "species"] = species
    df = df.reset_index(drop=True)
    return df

# Asigna los nombres correctos de columnas para Statesville
def set_column_names(df):
    df.columns = [
        "product_number",
        "formula_code",
        "product_name",
        "product_form",
        "unit_weight",
        "pallet_quantity",
        "stocking_status",
        "min_order_quantity",
        "lead_time_days",
        "fob_or_dlv",
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price"
    ]
    return df

# Solo valida si la tabla tiene suficiente columnas
def valid_table(df):
    return isinstance(df, pd.DataFrame) and df.shape[1] > 10

# Une todos los fragmentos válidos del PDF
def raw_price_list(table_list):
    price_list = pd.DataFrame()
    for tbl in table_list:
        if valid_table(tbl):
            price_list = pd.concat([price_list, tbl], ignore_index=True)
    return price_list

# Extrae todas las tablas desde coordenadas específicas para layout horizontal
def find_tables_in_pdf(file_path):
    try:
        # Área horizontal precisa para el layout de Statesville
        return tabula.read_pdf(file_path, pages="all", area=[160, 25, 760, 1080], lattice=True)
    except Exception as error:
        return []

# Función principal para procesar el archivo PDF horizontal
def read_file(file_path):
    tables = find_tables_in_pdf(file_path)
    price_list = raw_price_list(tables)
    price_list = set_column_names(price_list)
    price_list = add_species_column(price_list)
    price_list = add_plant_location(price_list, file_path)
    price_list = add_effective_date(price_list, file_path)
    price_list = correct_negative_value_in_price_list(price_list)
    price_list = find_unit_weight(price_list)
    price_list = source_columns(price_list)
    price_list = default_columns(price_list)
    return price_list

#------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------

import pathlib
import re
import os
import credentials as crd
import environments as env
import pandas as pd

from sharepoint_interface import get_sharepoint_interface
from cdp_interface import CDPInterface

# Estas son funciones que asumo ya existen en tu proyecto
# Adáptalas a tus nombres / rutas:
# - get_pending_files(sp_interface)
# - get_competitor_data(file_path)
# - check_if_data_exists_and_reconciliate(price_list, location, effective_date)
# - set_column_types(df)  <-- si lo usas

REPOSITORY = "/sites/RetailPricing/Shared%20Documents/General/Competitive%20Intel/Competitor%20PDF%20Upload/"
LOCAL_REPOSITORY = "sharepoint_interface/local_repository/"

def get_pending_files(sp_interface):
    files = sp_interface.files_in_folder(REPOSITORY)
    print(f"Archivos en la carpeta {REPOSITORY}: {files}")
    return files

def sanitize_table_name(s: str) -> str:
    """
    Reemplaza todo lo que no sea alfanumérico o '_' por '_', 
    evitando espacios y puntos en el nombre de la tabla temporal.
    """
    return re.sub(r'[^A-Za-z0-9_]+', '_', s)


def process_pending_files():
    """
    1) Conecta a SharePoint y obtiene PDFs.
    2) Descarga y parsea cada archivo PDF.
    3) Reconciliación vs. BD (opcional).
    4) Sube datos a la tabla final en CDP (comp_price_grid).
    5) Elimina el archivo en SharePoint tras el proceso.
    """
    cdp = CDPInterface(env.production, crd.process_account)
    sp = get_sharepoint_interface("retailpricing")
    
    # 1) Listar archivos
    pending_files = get_pending_files(sp)
    if not pending_files:
        print(f"[INFO] No hay archivos en {REPOSITORY}")
        return
    
    total_file_count = len(pending_files)
    for counter, file in enumerate(pending_files, 1):
        # Extraer el nombre sin extensión
        raw_file_name = pathlib.Path(file["file_name"]).stem
        # Convertirlo a algo seguro para tablas temporales
        file_name_clean = sanitize_table_name(raw_file_name)
        
        print(f"{counter}/{total_file_count} -> file name: {file_name_clean}")
        print(f"Descargando archivo desde SharePoint: {file['file_name']} ...")
        
        # 2) Descargar el PDF a LOCAL_REPOSITORY
        file_local_path = sp.download_file(file["file_path"], LOCAL_REPOSITORY)
        if not file_local_path:
            print("[ERROR] No se pudo descargar el archivo.")
            continue
        
        # 3) Parsear el PDF
        comp_data_dict = get_competitor_data(str(file_local_path))
        
        price_list = comp_data_dict["price_list"]
        location = comp_data_dict["location"]
        effective_date = comp_data_dict["effective_date"]
        
        print("[DEBUG] Price list tras parseo:")
        print(price_list.head(10))
        
        # (Opcional) Reconciliación vs. BD
        price_list = check_if_data_exists_and_reconciliate(price_list, location, effective_date)
        print("[DEBUG] Price list tras reconciliación:")
        print(price_list.head(10))

        # (Opcional) Quitar la columna "source" si no se requiere en BD
        if "source" in price_list.columns:
            price_list = price_list.drop("source", axis=1)
        
        # (Opcional) set_column_types
        # price_list = set_column_types(price_list)
        
        # 4) Subir a la tabla final si hay data nueva
        if price_list.shape[0] > 0:
            uploaded_ok = cdp.upload_data(price_list, "comp_price_grid", file_name_clean)
            if uploaded_ok:
                print(f"[INFO] {file['file_name']} uploaded successfully to database.")
                # 5) Borrar el archivo de SharePoint
                sp.delete_file(file["file_path"])
                print(f"[INFO] file deleted from SharePoint folder: {file_name_clean}")
            else:
                print("[ERROR] Ocurrió un problema subiendo a la BD.")
        else:
            print("[INFO] DataFrame vacío o duplicado. Data ya existe en BD.")
            sp.delete_file(file["file_path"])
            print(f"[INFO] file deleted from SharePoint folder: {file_name_clean}")

    print("Done.")


if __name__ == "__main__":
    process_pending_files()

