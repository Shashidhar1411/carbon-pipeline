select distinct job_id from {{ ref('stg_job_run') }}
