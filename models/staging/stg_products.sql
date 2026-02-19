select
    product_id,
    product_name,
    category,
    cast(price as number(10,2)) as price
from {{ source('bronze', 'products_raw') }}
