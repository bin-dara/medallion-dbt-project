select
    order_item_id,
    order_id,
    product_id,
    cast(quantity as number) as quantity,
    cast(unit_price as number(10,2)) as unit_price
from {{ source('bronze', 'order_items_raw') }}
