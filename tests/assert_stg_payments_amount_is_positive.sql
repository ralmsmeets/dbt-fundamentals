with payments as (
    select * from {{ ref('stg_payments') }}
)

select 
    order_id,
    sum(dollaramount) as total_amount
from payments
group by order_id
having total_amount < 0