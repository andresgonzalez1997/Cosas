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

import competitor_data.purina_file_horizontal as pfh
import os
import re
import tabula
import pandas as pd

# SharePoint
from sharepoint_interface.sharepoint_interface import download_pdf_from_sharepoint
from sharepoint_interface.sharepoint_interface import get_sharepoint_interface

# CDP
import credentials as crd
import environments as env
from cdp_interface import CDPInterface


REPOSITORY  = "/sites/RetailPricing/Shared%20Documents/General/Competitive%20Intel/Competitor%20PDF%20new%20format%20(horizontal%20file)/"
LOCAL_REPOSITORY = "sharepoint_interface/local_repository/"


def sanitize_table_name(s: str) -> str:
    """
    Reemplaza todo lo que no sea alfanumérico o '_' por '_'.
    Evita espacios, puntos y otros caracteres que no admite Impala/Hive 
    en nombres de tabla.
    """
    return re.sub(r'[^A-Za-z0-9_]+', '_', s)


def set_column_types(df: pd.DataFrame) -> pd.DataFrame:
    # Columnas que deben ser STRING
    string_cols = [
        "product_number",
        "formula_code",
        "product_name",
        "product_form",
        "unit_weight",
        "stocking_status",
        "fob_or_dlv",
        "species",
        "plant_location",
        "date_inserted",
        "source"
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Columnas que deben ser numéricas (float)
    float_cols = [
        "pallet_quantity",
        "min_order_quantity",
        "days_lead_time",
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price"
        # Agrega aquí cualquier otra columna que sepas que es numérica
    ]
    for col in float_cols:
        if col in df.columns:
            # Con errors="coerce", si hay texto no convertible, se vuelve NaN
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def excecute_process():
    # 1) Obtener la interfaz de SharePoint
    sp = get_sharepoint_interface("retailpricing")
    if not sp:
        print("[ERROR] No se pudo obtener la interfaz de SharePoint.")
        exit()

    # 2) Listar archivos en la carpeta
    files = sp.files_in_folder(REPOSITORY)
    if not files:
        print(f"[INFO] No hay archivos en {REPOSITORY}")
        exit()

    # 3) Filtrar PDFs
    pdf_files = [f for f in files if f["file_name"].lower().endswith(".pdf")]
    if not pdf_files:
        print(f"[INFO] No se encontraron PDFs en {REPOSITORY}")
        exit()

    # 4) Seleccionar el primer PDF
    pdf_to_download = pdf_files[0]
    pdf_sharepoint_path = pdf_to_download["file_path"]
    pdf_filename = pdf_to_download["file_name"]

    # 5) Descargar el PDF a local
    if not os.path.exists(LOCAL_REPOSITORY):
        os.makedirs(LOCAL_REPOSITORY, exist_ok=True)

    local_pdf_path = sp.download_file(pdf_sharepoint_path, LOCAL_REPOSITORY)
    if not local_pdf_path:
        print("[ERROR] No se pudo descargar el PDF.")
        exit()

    # 6) Procesar el PDF (parseo horizontal)
    df = pfh.read_file(str(local_pdf_path))
    print("[CONTROL]-------------------------------------------------------------")
    print(f"Estas columnas trae el data frame despues de leer el archivo de PDF:\n {df.columns.tolist()}")
    print(df.head(5))
    print("[CONTROL]-------------------------------------------------------------")

    # (Opcional) Añadir "source" si no lo agrega tu parser
    if "source" not in df.columns:
        df["source"] = "pdf"

    # 6.1) Forzar tipos
    df = set_column_types(df)
    print("[CONTROL2]-------------------------------------------------------------")
    print(f"COLUMNAS DESPUES DEL SET COLUM TYPES:\n {df.columns.tolist()}")
    print(df.dtypes)
    print("[CONTROL2]-------------------------------------------------------------")

    # 7) Mostrar el DataFrame (inspección)
    print("[INFO] Final parsed DataFrame shape:", df.shape)
    print(df.head(20))

    # 8) Subir a la tabla "comp_price_horizontal_files" en CDP (si hay registros)
    if df.shape[0] > 0:
        cdp = CDPInterface(env.production, crd.process_account)

        # Quita la extensión ".pdf" y sanitiza caracteres
        base_name = os.path.splitext(pdf_filename)[0]
        base_name = sanitize_table_name(base_name)

        # Sube al CDP
        if cdp.upload_data(df, "comp_price_horizontal_files", base_name):
            print(f"[INFO] Datos subidos correctamente a 'comp_price_horizontal_files'.")
        else:
            print("[ERROR] No se pudieron subir datos a CDP.")
    else:
        print("[INFO] DataFrame vacío; no se suben datos.")

    # 9) Eliminar de SharePoint, exitoso o no
    try:
        if sp.delete_file(pdf_sharepoint_path):
            print(f"[INFO] Archivo '{pdf_filename}' eliminado de SharePoint.")
        else:
            print(f"[WARN] No se pudo eliminar '{pdf_filename}' de SharePoint.")
    except Exception as e:
        print(f"[ERROR] Al intentar eliminar en SharePoint: {e}")


if __name__ == "__main__":
    excecute_process()
