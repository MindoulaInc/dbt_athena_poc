{{
  config(
    materialized = 'table',
    schema = 'raw',
    table_type = 'iceberg'
    )
}}

select
  supplier_file_log_id,
  rec_num,
  memb_life_id,
  old_memb_life_id,
  reltp_cd,
  relt_desc,
  gndr_cd,
  date_of_birth,
  age,
  lvl_of_cov_cd,
  memb_enrlt_from_dt,
  memb_enrlt_thru_dt,
  addr_line_1,
  addr_line_2,
  city,
  state,
  zip,
  empr_grp_nm,
  renwl_mth,
  seg_id,
  prodt_nm_desc,
  prodt_line_desc,
  pkg_cl_cd,
  empr_grp_id,
  grp_sect,
  dept_nbr,
  rsk_cd,
  hsa_indc,
  max(rec_num) over ( partition by memb_life_id, supplier_file_log_id) as max_rec_num,
  max(supplier_file_log_id) over ( partition by memb_life_id)          as max_supplier_file_log_id
from {{ source('raw', 's_63_merged_member') }}