/* create multiple CTEs */
with
  /* write a CTE query to calculate the total revenue */
    total_revenue as (
            select 
                sum(payments_dataset.payment_value) as total_revenue
                , date(order_purchase_timestamp) as purchase_date
            from brooklyndata.olist_orders_dataset as order_dataset
            left join brooklyndata.olist_order_payments_dataset  as payments_dataset on order_dataset.order_id = payments_dataset.order_id
            group by date(order_dataset.order_purchase_timestamp)
    )

  /* write CTE queries to rank products by revenue generated per day and select only products 
     with rank one,two and three to get the top 3 performing products for each day */
   
    , products_ranked_one as (
            select * 
            from (
                    select 
                        sum(items_dataset.price + items_dataset.freight_value) as rank1_revenue 
                        , product_dataset.product_category_name
                        , date(orders_dataset.order_purchase_timestamp) as dates
                            /* partition the products by date purchased and arrange from the largest to the smallest based on revenue earned */
                        , rank() over (partition  by date(order_purchase_timestamp) order by sum(price + freight_value) desc) product_rank
                    from brooklyndata.olist_orders_dataset as orders_dataset
                    inner join brooklyndata.olist_order_items_dataset as items_dataset on orders_dataset.order_id = items_dataset.order_id
                    inner join brooklyndata.olist_products_dataset as product_dataset on items_dataset.product_id = product_dataset.product_id
                    where product_dataset.product_category_name is not null /*remove product id with no product name */
                    group by date(orders_dataset.order_purchase_timestamp), product_dataset.product_category_name
                   
            ) b 
            /* select only products that rank as 1 on each day */
            where b.product_rank = 1 
    )


    , products_ranked_two as (
            select * 
            from (
                    select 
                        sum( items_dataset.price +  items_dataset.freight_value) as rank2_revenue 
                        , product_dataset.product_category_name
                        , date(orders_dataset.order_purchase_timestamp) as dates
                            /* partition the products by date purchased and  arrange from the largest to the smallest based on revenue earned */
                        , rank() over (partition  by date(order_purchase_timestamp) order by sum(price + freight_value) desc) product_rank
                    from brooklyndata.olist_orders_dataset as orders_dataset
                    inner join brooklyndata.olist_order_items_dataset as items_dataset on orders_dataset.order_id = items_dataset.order_id
                    inner join brooklyndata.olist_products_dataset as product_dataset on items_dataset.product_id = product_dataset.product_id
                    where product_dataset.product_category_name is not null /*remove product id with no product name*/
                    group by date(orders_dataset.order_purchase_timestamp), product_dataset.product_category_name
                   
            ) b 
            /* select only products that rank as 2 on each day */
            where b.product_rank = 2
    )



    , products_ranked_three as ( 
            select * 
            from (
                    select 
                        sum( items_dataset.price +  items_dataset.freight_value) as rank3_revenue 
                        , product_dataset.product_category_name
                        , date(orders_dataset.order_purchase_timestamp) as dates
                            /* partition the products by date purchased and arrange from the largest to the smallest based on revenue earned */
                        , rank() over (partition  by date(order_purchase_timestamp) order by sum(price + freight_value) desc) product_rank
                    from brooklyndata.olist_orders_dataset as orders_dataset
                    inner join brooklyndata.olist_order_items_dataset as items_dataset on orders_dataset.order_id = items_dataset.order_id
                    inner join brooklyndata.olist_products_dataset as product_dataset on items_dataset.product_id = product_dataset.product_id
                    where product_dataset.product_category_name is not null /*remove product id with no product name*/
                    group by date(orders_dataset.order_purchase_timestamp), product_dataset.product_category_name
                   
            ) b 
            /* select only products that rank as 3 on each day */
          where b.product_rank = 3 
    )

 /* write more CTE queries for calculate total customers making orders and total orders */
    , customers_making_orders as ( 
        select 
            count (distinct(customer_id)) as customer_count
            , date(order_purchase_timestamp) as dates
        from brooklyndata.olist_orders_dataset
        group by date(order_purchase_timestamp)
    )

    , total_orders as ( 
        select 
            count (distinct(order_id)) as orders_count
            , date(order_purchase_timestamp) as dates
        from brooklyndata.olist_orders_dataset
        group by date(order_purchase_timestamp)
    )


/* write a single query that combines results from the multiple CTEs */
select 
    total_revenue.purchase_date:: date  as order_purchase_date 
    , total_orders.orders_count
    , customers_making_orders.customer_count as customers_making_orders_count
    /* rounding the total revenue to two decimal place*/
    , round(total_revenue.total_revenue::numeric,2) as revenue_usd
    
    /* getting the average revenue per order by dividing the total revenue 
        by the total orders and rounding to two decimal place */
    , round((total_revenue.total_revenue / total_orders.orders_count)::numeric,2) as average_revenue_per_order_usd
    
    /* concantenate products in rank 1, 2 and 3 to form a list using || symbol */
    , products_ranked_one.product_category_name || ',' || products_ranked_two.product_category_name
    ||','||products_ranked_three.product_category_name as top_3_product_categories_by_revenue
    
    /* calculating the revenue precentage contributed by the top 3 products for each day 
        by dividing the top product revenue by total revenue earned 
        and multiplying by 100. Round the result to two decimal place and store as a string using */
    , to_char((products_ranked_one.rank1_revenue/ total_revenue.total_revenue)*100::float, 'FM999999990.00')
    || ','|| to_char((products_ranked_two.rank2_revenue/ total_revenue.total_revenue)*100::float, 'FM999999990.00')
    || ','||to_char((products_ranked_three.rank3_revenue/ total_revenue.total_revenue)*100::float, 'FM999999990.00') 
    as top_3_product_categories_revenue_percentage
  
from total_revenue
left join products_ranked_one on total_revenue.purchase_date = products_ranked_one.dates
left join products_ranked_two on total_revenue.purchase_date= products_ranked_two.dates
left join products_ranked_three on total_revenue.purchase_date= products_ranked_three.dates
left join customers_making_orders on total_revenue.purchase_date  = customers_making_orders.dates
left join total_orders on total_revenue.purchase_date = total_orders.dates

order by total_revenue.purchase_date desc /* order the result by date */


