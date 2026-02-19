select
    order_id,
    customer_id,
    to_date(order_date) as order_date,
    status,
    cast(total_amount as number(10,2)) as total_amount
from {{ source('bronze', 'orders_raw') }}
