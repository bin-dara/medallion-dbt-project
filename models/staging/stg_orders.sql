with ranked_orders as (

    select
        order_id,
        customer_id,
        to_date(order_date) as order_date,
        status,
        cast(total_amount as number(10,2)) as total_amount,
        row_number() over (
            partition by order_id
            order by order_date desc
        ) as rn

    from {{ source('bronze', 'orders_raw') }}

)

select
    order_id,
    customer_id,
    order_date,
    status,
    total_amount
from ranked_orders
where rn = 1

