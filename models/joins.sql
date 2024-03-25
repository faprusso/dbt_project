with
    prod as(
        select
            ct.category_name,
            sp.company_name,
            pd.product_name,
            pd.unit_price,
            pd.product_id
        from {{source('sources', 'products')}} pd
        left join {{source('sources', 'suppliers')}} sp on (pd.supplier_id = sp.supplier_id)
        left join {{source('sources', 'categories')}} ct on (pd.category_id = ct.category_id)
), orddetai as (
        select
            pd.*,
            od.order_id,
            od.quantity,
            od.discount
        from {{ref('orderdetails')}} od
        left join prod pd on (pd.product_id = od.product_id)
), ordrs as (
        select
            ord.order_id,
            ord.order_date,
            cs.company_name as customer,
            em.name as employee,
            em.age,
            em.length_of_service
        from {{source('sources', 'orders')}} ord
        left join {{ref('customers')}} cs on (cs.customer_id = ord.customer_id)
        left join {{ref('employees')}} em on (em.employee_id = ord.employee_id)
        left join {{source('sources', 'shippers')}} sh on (sh.shipper_id = ord.ship_via)
), finaljoin as (
        select
            od.*,
            ord.order_date,
            ord.customer,
            ord.employee,
            ord.age,
            ord.length_of_service
        from orddetai od
        inner join ordrs ord on (ord.order_id = od.order_id)
)

select * from finaljoin
