select  i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        avg( cast(cs_quantity as decimal(12,2))) agg1,
        avg( cast(cs_list_price as decimal(12,2))) agg2,
        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
        avg( cast(cs_sales_price as decimal(12,2))) agg4,
        avg( cast(cs_net_profit as decimal(12,2))) agg5,
        avg( cast(c_birth_year as decimal(12,2))) agg6,
        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
 from catalog_sales, date_dim, customer_demographics cd1, item, customer, customer_address, 
      customer_demographics cd2
 where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk and
       catalog_sales.cs_item_sk = item.i_item_sk and
       catalog_sales.cs_bill_cdemo_sk = cd1.cd_demo_sk and
       catalog_sales.cs_bill_customer_sk = customer.c_customer_sk and
       cd1.cd_gender = 'M' and 
       cd1.cd_education_status = 'College' and
       customer.c_current_cdemo_sk = cd2.cd_demo_sk and
       customer.c_current_addr_sk = customer_address.ca_address_sk and
       c_birth_month in (9,5,12,4,1,10) and
       d_year = 2001 and
       ca_state in ('ND','WI','AL'
                   ,'NC','OK','MS','TN')
 group by i_item_id, ca_country, ca_state, ca_county with rollup
 order by ca_country,
        ca_state, 
        ca_county,
	i_item_id
 limit 100;
