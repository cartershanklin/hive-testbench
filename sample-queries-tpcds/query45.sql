select  ca_zip, ca_county, sum(ws_sales_price)
 from
    web_sales
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
    JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk 
    JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    JOIN item ON web_sales.ws_item_sk = item.i_item_sk 
 where
        ( item.i_item_id in (select i_item_id
                             from item i2
                             where i2.i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
            )
        and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_county
 order by ca_zip, ca_county
 limit 100;
