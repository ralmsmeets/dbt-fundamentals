with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_payments as (
    select 
        order_id,
        sum(case when status = 'success' then dollaramount end) as dollaramount
    
    from payments
    group by 1
),

final as (
  
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payments.dollaramount, 0) as dollaramount
    from 
    
    orders
    left join order_payments using (order_id)

)

select * from final