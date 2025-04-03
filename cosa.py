import datetime
import pandas as pd
import tabula
import re

# Define el orden final y columnas esperadas del DataFrame limpio
def default_columns(df):
    return df[[
        "product_number",
        "formula_code",
        "product_name",
        "ref_col",
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
        "date_inserted",
        "source"
    ]]

# Añade la fuente del archivo al DataFrame
def source_columns(df):
    df["source"] = "pdf"
    return df

# Intenta extraer el peso unitario (en libras) desde el nombre del producto si está ausente
def find_unit_weight(df):
    for index, row in df.iterrows():
        if not re.search("LB", str(row["unit_weight"])):
            search_result = re.findall("\d*\s*LB", str(row["product_name"]))
            if len(search_result) > 0:
                df.at[index, "unit_weight"] = search_result[0]
    return df

# Convierte valores con signo negativo al estilo “22.30-” en floats negativos (-22.30)
def correct_negative_value(value):
    if str(value).endswith("-"):
        return float(str(value).replace("-", "")) * -1
    else:
        try:
            return float(value)
        except:
            return value  # Si no se puede convertir, retorna el valor original

# Aplica la corrección de negativos a las columnas de precios
def correct_negative_value_in_price_list(df):
    for col in df.columns[7:12]:
        df[col] = df[col].apply(correct_negative_value)
    return df

# Extrae la fecha efectiva del archivo PDF desde coordenadas específicas
def effective_date(file_path):
    effective_date_table = tabula.read_pdf(file_path, pages=1, area=[54,10,82,254])
    results = re.findall(r"\d{2}/\d{2}/\d{2}", str(effective_date_table[0])) 
    if results: 
        date = datetime.datetime.strptime(results[0], "%m/%d/%y").date()
        return date.strftime("%Y-%m-%d")
    return None

# Extrae el nombre de la planta (ubicación) desde una región específica del PDF
def plant_location(file_path):
    location_table = tabula.read_pdf(file_path, pages=1, area=[0,500,40,700])
    location = str(location_table[0]).split("\n")[0].strip().replace(",", "").upper()
    return location

# Agrega la fecha efectiva como columna en el DataFrame
def add_effective_date(df, file_path):
    df["date_inserted"] = effective_date(file_path)
    return df

# Agrega la ubicación de la planta al DataFrame
def add_plant_location(df, file_path):
    df["plant_location"] = plant_location(file_path)
    return df

# Identifica la especie (texto sin números) y la asigna a las filas siguientes hasta cambiar
def add_species_column(df):
    species = None
    df["species"] = None
    for index, row in df.iterrows():
        if re.match(r"\d", str(row[0])) is None:
            species = str(row[0]).replace(",", "").upper()
            df = df.drop(index, axis=0)  # Elimina la fila del título
        else:
            df.loc[index, "species"] = species
    df = df.reset_index(drop=True)
    return df

# Asigna nombres de columna fijos y esperados al DataFrame
def set_column_names(df):
    df.columns = [
        "product_number",
        "formula_code",
        "product_name",
        "ref_col",
        "unit_weight",
        "product_form",
        "fob_or_dlv",
        "price_change",
        "single_unit_list_price",
        "full_pallet_list_price",
        "pkg_bulk_discount",
        "best_net_list_price"
    ]
    return df

# Verifica si una tabla es válida (debe ser un DataFrame y tener más de 5 columnas)
def valid_table(df):
    return isinstance(df, pd.DataFrame) and df.shape[1] > 5

# Une todas las tablas válidas extraídas en un solo DataFrame
def raw_price_list(table_list):
    price_list = pd.DataFrame()
    for tbl in table_list: 
        if valid_table(tbl): 
            price_list = pd.concat([price_list, tbl], ignore_index=True)
    return price_list

# Extrae tablas desde el PDF horizontal usando coordenadas amplias y modo lattice
def find_tables_in_pdf(file_path):
    try:
        # Coordenadas exactas para PDFs horizontales como Statesville
        table_list = tabula.read_pdf(file_path, pages="all", area=[160, 25, 760, 1080], lattice=True)
        return table_list
    except Exception as error:
        return False

# Función principal para procesar el archivo PDF horizontal
def read_file(file_path):
    table_list = find_tables_in_pdf(file_path)
    price_list = raw_price_list(table_list)
    price_list = set_column_names(price_list)
    price_list = add_species_column(price_list)
    price_list = add_plant_location(price_list, file_path)
    price_list = add_effective_date(price_list, file_path)
    price_list = correct_negative_value_in_price_list(price_list)
    price_list = find_unit_weight(price_list)
    price_list = source_columns(price_list)
    price_list = default_columns(price_list)
    return price_list
