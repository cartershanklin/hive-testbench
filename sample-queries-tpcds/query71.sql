select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
 	sum(ext_price) ext_price
 from item JOIN (select ws_ext_sales_price as ext_price, 
                        ws_sold_date_sk as sold_date_sk,
                        ws_item_sk as sold_item_sk,
                        ws_sold_time_sk as time_sk  
                 from web_sales,date_dim
                 where date_dim.d_date_sk = web_sales.ws_sold_date_sk
                   and d_moy=12
                   and d_year=2001
                 union all
                 select cs_ext_sales_price as ext_price,
                        cs_sold_date_sk as sold_date_sk,
                        cs_item_sk as sold_item_sk,
                        cs_sold_time_sk as time_sk
                 from catalog_sales,date_dim
                 where date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
                   and d_moy=12
                   and d_year=2001
                 union all
                 select ss_ext_sales_price as ext_price,
                        ss_sold_date_sk as sold_date_sk,
                        ss_item_sk as sold_item_sk,
                        ss_sold_time_sk as time_sk
                 from store_sales,date_dim
                 where date_dim.d_date_sk = store_sales.ss_sold_date_sk
                   and d_moy=12
                   and d_year=2001
                 ) tmp ON tmp.sold_item_sk = item.i_item_sk
 JOIN time_dim ON tmp.time_sk = time_dim.t_time_sk
 where
       i_manager_id=1
   and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
 group by i_brand, i_brand_id,t_hour,t_minute
 order by ext_price desc, i_brand_id
 ;


