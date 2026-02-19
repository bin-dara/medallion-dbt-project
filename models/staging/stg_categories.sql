select
    category_id,
    category_name
from {{ source('bronze', 'categories_raw') }}
