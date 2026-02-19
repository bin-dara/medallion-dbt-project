select
    customer_id,
    upper(first_name) as first_name,
    upper(last_name) as last_name,
    lower(email) as email,
    to_date(signup_date) as signup_date,
    country
from {{ source('bronze', 'customers_raw') }}
