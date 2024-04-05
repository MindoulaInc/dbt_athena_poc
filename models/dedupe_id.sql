{{ config(materialized='table', table_type='iceberg') }}

select s_9_merged_member.supplier_file_log_id, count(*) as cnt
    from {{source('raw','s_9_merged_member')}}
where member_name_full = 'Nancy Moreno'
group by 1