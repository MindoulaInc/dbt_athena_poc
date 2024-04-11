{{
  config(
    materialized = 'table',
    table_type = 'iceberg'
    )
}}

select 
  claim_number,
  line_number,
  supplier_file_log_id,
  rec_num,
  max(supplier_file_log_id) over (w)                                                as max_supplier_file_log_id,
  max(rec_num) over ( partition by claim_number, line_number, supplier_file_log_id) as max_rec_num,
  sum(allowed_amount) over (w)                                                      as total_allowed_amount,
  sum(copay_amount) over (w)                                                        as total_copay_amount,
  sum(coinsurance_amount) over (w)                                                  as total_coinsurance_amount,
  sum(deductible_amount) over (w)                                                   as total_deductible_amount,
  sum(paid_amount) over (w)                                                         as total_paid_amount,
  sum(its_access_fees) over (w)                                                     as total_its_access_fees,
  sum(its_supplemental_fees) over (w)                                               as total_its_supplemental_fees,
  sum(surcharge_amount) over (w)                                                    as total_surcharge_amount,
  sum(medicare_paid_amount) over (w)                                                as total_medicare_paid_amount,
  sum(non_covered_amount) over (w)                                                  as total_non_covered_amount,
  sum(cob_amount) over (w)                                                          as total_cob_amount
from {{ source('raw', 's_63_merged_medical') }}
window w as (partition by claim_number, line_number)
