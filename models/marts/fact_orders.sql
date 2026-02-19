-- select
--     o.order_id,
--     o.order_date,
--     o.status,
--     o.total_amount,
--     c.customer_id,
--     c.first_name,
--     c.last_name,
--     oi.product_id,
--     p.product_name,
--     oi.quantity,
--     oi.unit_price,
--     (oi.quantity * oi.unit_price) as line_total
-- from {{ ref('stg_orders') }} o
-- left join {{ ref('stg_customers') }} c
--     on o.customer_id = c.customer_id
-- left join {{ ref('stg_order_items') }} oi
--     on o.order_id = oi.order_id
-- left join {{ ref('stg_products') }} p
--     on oi.product_id = p.product_id


{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    o.order_id,
    o.order_date,
    o.status,
    o.total_amount,
    c.customer_id,
    c.first_name,
    c.last_name,
    oi.product_id,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) as line_total
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
left join {{ ref('stg_order_items') }} oi
    on o.order_id = oi.order_id
left join {{ ref('stg_products') }} p
    on oi.product_id = p.product_id

{% if is_incremental() %}

where o.order_date > (select max(order_date) from {{ this }})

{% endif %}
