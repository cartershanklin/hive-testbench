select cs1.product_name ,cs1.store_name ,cs1.store_zip ,cs1.b_street_number ,cs1.b_streen_name ,cs1.b_city
     ,cs1.b_zip ,cs1.c_street_number ,cs1.c_street_name ,cs1.c_city ,cs1.c_zip ,cs1.syear ,cs1.cnt
     ,cs1.s1 ,cs1.s2 ,cs1.s3
     ,cs2.s1 ,cs2.s2 ,cs2.s3 ,cs2.syear ,cs2.cnt
from
(select i_product_name as product_name ,i_item_sk as item_sk ,s_store_name as store_name
     ,s_zip as store_zip ,ad1.ca_street_number as b_street_number ,ad1.ca_street_name as b_streen_name
     ,ad1.ca_city as b_city ,ad1.ca_zip as b_zip ,ad2.ca_street_number as c_street_number
     ,ad2.ca_street_name as c_street_name ,ad2.ca_city as c_city ,ad2.ca_zip as c_zip
     ,d1.d_year as syear ,d2.d_year as fsyear ,d3.d_year as s2year ,count(*) as cnt
     ,sum(ss_wholesale_cost) as s1 ,sum(ss_list_price) as s2 ,sum(ss_coupon_amt) as s3
  FROM   store_sales
        JOIN store_returns ON store_sales.ss_item_sk = store_returns.sr_item_sk and store_sales.ss_ticket_number = store_returns.sr_ticket_number
        JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
        JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
        JOIN date_dim d2 ON customer.c_first_sales_date_sk = d2.d_date_sk 
        JOIN date_dim d3 ON customer.c_first_shipto_date_sk = d3.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN customer_demographics cd1 ON store_sales.ss_cdemo_sk= cd1.cd_demo_sk
        JOIN customer_demographics cd2 ON customer.c_current_cdemo_sk = cd2.cd_demo_sk
        JOIN promotion ON store_sales.ss_promo_sk = promotion.p_promo_sk
        JOIN household_demographics hd1 ON store_sales.ss_hdemo_sk = hd1.hd_demo_sk
        JOIN household_demographics hd2 ON customer.c_current_hdemo_sk = hd2.hd_demo_sk
        JOIN customer_address ad1 ON store_sales.ss_addr_sk = ad1.ca_address_sk
        JOIN customer_address ad2 ON customer.c_current_addr_sk = ad2.ca_address_sk
        JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
        JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
        JOIN item ON store_sales.ss_item_sk = item.i_item_sk
        JOIN
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales JOIN catalog_returns
  ON catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
    and catalog_sales.cs_order_number = catalog_returns.cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)) cs_ui
ON store_sales.ss_item_sk = cs_ui.cs_item_sk
  WHERE  
         cd1.cd_marital_status <> cd2.cd_marital_status and
         i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
         i_current_price between 35 and 35 + 10 and
         i_current_price between 35 + 1 and 35 + 15
group by i_product_name ,i_item_sk ,s_store_name ,s_zip ,ad1.ca_street_number
       ,ad1.ca_street_name ,ad1.ca_city ,ad1.ca_zip ,ad2.ca_street_number
       ,ad2.ca_street_name ,ad2.ca_city ,ad2.ca_zip ,d1.d_year ,d2.d_year ,d3.d_year
) cs1
JOIN
(select i_product_name as product_name ,i_item_sk as item_sk ,s_store_name as store_name
     ,s_zip as store_zip ,ad1.ca_street_number as b_street_number ,ad1.ca_street_name as b_streen_name
     ,ad1.ca_city as b_city ,ad1.ca_zip as b_zip ,ad2.ca_street_number as c_street_number
     ,ad2.ca_street_name as c_street_name ,ad2.ca_city as c_city ,ad2.ca_zip as c_zip
     ,d1.d_year as syear ,d2.d_year as fsyear ,d3.d_year as s2year ,count(*) as cnt
     ,sum(ss_wholesale_cost) as s1 ,sum(ss_list_price) as s2 ,sum(ss_coupon_amt) as s3
  FROM   store_sales
        JOIN store_returns ON store_sales.ss_item_sk = store_returns.sr_item_sk and store_sales.ss_ticket_number = store_returns.sr_ticket_number
        JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
        JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
        JOIN date_dim d2 ON customer.c_first_sales_date_sk = d2.d_date_sk 
        JOIN date_dim d3 ON customer.c_first_shipto_date_sk = d3.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN customer_demographics cd1 ON store_sales.ss_cdemo_sk= cd1.cd_demo_sk
        JOIN customer_demographics cd2 ON customer.c_current_cdemo_sk = cd2.cd_demo_sk
        JOIN promotion ON store_sales.ss_promo_sk = promotion.p_promo_sk
        JOIN household_demographics hd1 ON store_sales.ss_hdemo_sk = hd1.hd_demo_sk
        JOIN household_demographics hd2 ON customer.c_current_hdemo_sk = hd2.hd_demo_sk
        JOIN customer_address ad1 ON store_sales.ss_addr_sk = ad1.ca_address_sk
        JOIN customer_address ad2 ON customer.c_current_addr_sk = ad2.ca_address_sk
        JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
        JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
        JOIN item ON store_sales.ss_item_sk = item.i_item_sk
        JOIN
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales JOIN catalog_returns
  ON catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
    and catalog_sales.cs_order_number = catalog_returns.cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)) cs_ui
ON store_sales.ss_item_sk = cs_ui.cs_item_sk
  WHERE  
         cd1.cd_marital_status <> cd2.cd_marital_status and
         i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
         i_current_price between 35 and 35 + 10 and
         i_current_price between 35 + 1 and 35 + 15
group by i_product_name ,i_item_sk ,s_store_name ,s_zip ,ad1.ca_street_number
       ,ad1.ca_street_name ,ad1.ca_city ,ad1.ca_zip ,ad2.ca_street_number
       ,ad2.ca_street_name ,ad2.ca_city ,ad2.ca_zip ,d1.d_year ,d2.d_year ,d3.d_year
) cs2
ON cs1.item_sk=cs2.item_sk
where 
     cs1.syear = 2000 and
     cs2.syear = 2000 + 1 and
     cs2.cnt <= cs1.cnt and
     cs1.store_name = cs2.store_name and
     cs1.store_zip = cs2.store_zip
order by cs1.product_name ,cs1.store_name ,cs2.cnt;
