with payments as(
    select * from {{ ref('stg_payments')}}
),

{% set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] %}

pivoted as (
    select 
        order_id,
        {%- for method in payment_methods %}
        sum(case when payment_method = '{{ method }}' then dollaramount else 0 end) as {{ method }}_amount {%- if loop.last -%} {%- else -%} , {% endif %}
        {%- endfor %}
    from payments
    where status = 'success'
    group by 1
)

select * from pivoted