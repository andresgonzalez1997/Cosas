SELECT 
    product_number,
    formula_code,
    product_name,
    ref_col,
    unit_weight,
    product_form,
    fob_or_dlv,
    price_change,
    single_unit_list_price,
    full_pallet_list_price,
    pkg_bulk_discount,
    best_net_list_price,
    species,
    plant_location,
    date_inserted
    
FROM 
    @schema.comp_price_grid
    
WHERE
    plant_location = "@location"
    AND date_inserted = "@effective_date"