CREATE TABLE IF NOT EXISTS @schema.@temp_table (
    product_number STRING,
    formula_code STRING,
    product_name STRING,
    ref_col STRING,
    unit_weight STRING,
    product_form STRING,
    fob_or_dlv STRING,
    price_change DOUBLE,
    single_unit_list_price DOUBLE,
    full_pallet_list_price DOUBLE,
    pkg_bulk_discount DOUBLE,
    best_net_list_price DOUBLE,
    species STRING,
    plant_location STRING,
    date_inserted STRING
)
STORED AS PARQUET
LOCATION "@hdfs_root_folder/@temp_table"