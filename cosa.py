import competitor_data.purina_file_horizontal as pfh
import os
import pathlib
import re
import tabula
import pandas as pd

# SharePoint
from sharepoint_interface.sharepoint_interface import get_sharepoint_interface

# CDP
import credentials as crd
import environments as env
from cdp_interface import CDPInterface

REPOSITORY  = "/sites/RetailPricing/Shared%20Documents/General/Competitive%20Intel/Competitor%20PDF%20new%20format%20(horizontal%20file)/"
LOCAL_REPOSITORY = "sharepoint_interface/local_repository/"

def correct_file_name(val: str) -> str:
    """
    'correct_file_name' del código original.
    Reemplaza espacios/puntos/caracteres raros y 
    deja un string 'limpio' para la tabla temporal.
    """
    val = str(val).lower()
    # elimina ceros adelante
    val = re.sub('^(0){2,}', "", val)
    # elimina espacios al inicio
    val = re.sub('^[" "-]+', "", val)
    # elimina caracteres \r \n \t \u00a0
    val = re.sub('[\r\n\r\t\u00a0]+', ' ', val)
    # colapsa espacios múltiples
    val = re.sub('( ){2,}', ' ', val)
    # quita espacios finales
    val = val.strip()
    # reemplaza espacios, puntos y guiones por underscore
    val = val.replace(" ", "_")
    val = val.replace(".", "_")
    val = val.replace("-", "_")
    return val


def set_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura que no existan columnas obsoletas como 'ref_col' y
    convierte tipos a float/string.
    """
    # Columnas string
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

    # Columnas float
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
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Opcional: si existiera 'ref_col', lo quitamos en caso de que
    # todavía aparezca por error:
    if "ref_col" in df.columns:
        df.drop(columns=["ref_col"], inplace=True)

    return df


def excecute_process():
    sp = get_sharepoint_interface("retailpricing")
    if not sp:
        print("[ERROR] No se pudo obtener la interfaz de SharePoint.")
        return

    files = sp.files_in_folder(REPOSITORY)
    if not files:
        print(f"[INFO] No hay archivos en {REPOSITORY}")
        return

    # Filtrar PDFs
    pdf_files = [f for f in files if f["file_name"].lower().endswith(".pdf")]
    if not pdf_files:
        print(f"[INFO] No se encontraron PDFs en {REPOSITORY}")
        return

    # Conexión a CDP
    cdp = CDPInterface(env.production, crd.process_account)

    total = len(pdf_files)
    for idx, pdf_info in enumerate(pdf_files, start=1):
        pdf_filename = pdf_info["file_name"]
        pdf_sharepoint_path = pdf_info["file_path"]
        print(f"\n[{idx}/{total}] Procesando: {pdf_filename}")

        if not os.path.exists(LOCAL_REPOSITORY):
            os.makedirs(LOCAL_REPOSITORY, exist_ok=True)

        # Descargar
        local_pdf_path = sp.download_file(pdf_sharepoint_path, LOCAL_REPOSITORY)
        if not local_pdf_path:
            print("[ERROR] No se pudo descargar el PDF.")
            continue

        # Parsear horizontal
        df = pfh.read_file(str(local_pdf_path))

        # Observa columnas
        print("[DEBUG] Columnas del DF tras parsear:\n", df.columns.tolist())
        if "ref_col" in df.columns:
            print("[WARN] Se detectó ref_col en el DF... se eliminará.")
        print(df.head(5))

        # Forzar tipos
        df = set_column_types(df)
        print("[DEBUG] Columnas tras set_column_types:\n", df.columns.tolist())

        # Revisar shape
        print("[INFO] DataFrame shape:", df.shape)
        print(df.head(10))

        if df.shape[0] > 0:
            # Nombre base sin extension
            raw_name = pathlib.Path(pdf_filename).stem
            # Aplica la logica "original" de correct_file_name
            base_name = correct_file_name(raw_name)
            print("[DEBUG] Nombre base para la tabla temporal:", base_name)

            # Subir a la tabla final
            if cdp.upload_data(df, "comp_price_horizontal_files", base_name):
                print(f"[INFO] '{pdf_filename}' subido correctamente a 'comp_price_horizontal_files'.")
            else:
                print("[ERROR] Falló la subida a CDP.")
        else:
            print("[INFO] DF vacío, no se suben datos.")

        # Eliminar de SharePoint
        try:
            if sp.delete_file(pdf_sharepoint_path):
                print(f"[INFO] Archivo '{pdf_filename}' eliminado de SharePoint.")
            else:
                print(f"[WARN] No se pudo eliminar '{pdf_filename}' de SharePoint.")
        except Exception as e:
            print(f"[ERROR] Al intentar eliminar en SharePoint: {e}")

    print("\n[INFO] Proceso completado para todos los PDFs.")


if __name__ == "__main__":
    excecute_process()


    
#------------------------------------------------------------


CREATE TABLE IF NOT EXISTS @schema.@temp_table (
    product_number STRING,
    formula_code STRING,
    product_name STRING,
    product_form STRING,
    unit_weight STRING,
    pallet_quantity DOUBLE,
    stocking_status STRING,
    min_order_quantity DOUBLE,
    days_lead_time DOUBLE,
    fob_or_dlv STRING,
    price_change DOUBLE,
    list_price DOUBLE,
    full_pallet_price DOUBLE,
    half_load_full_pallet_price DOUBLE,
    full_load_full_pallet_price DOUBLE,
    full_load_best_price DOUBLE,
    plant_location STRING,
    date_inserted STRING,
    source STRING
)
STORED AS PARQUET
LOCATION "@hdfs_root_folder/@temp_table"
