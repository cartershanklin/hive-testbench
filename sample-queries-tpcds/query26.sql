select  i_item_id, 
        avg(cs_quantity) agg1,
        avg(cs_list_price) agg2,
        avg(cs_coupon_amt) agg3,
        avg(cs_sales_price) agg4 
 from catalog_sales, customer_demographics, date_dim, item, promotion
 where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk and
       catalog_sales.cs_item_sk = item.i_item_sk and
       catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk and
       catalog_sales.cs_promo_sk = promotion.p_promo_sk and
       cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'Primary' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998
 group by i_item_id
 order by i_item_id
 limit 100;
