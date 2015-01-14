
select  substr(r_reason_desc,1,20) as r
       ,avg(ws_quantity) wq
       ,avg(wr_refunded_cash) ref
       ,avg(wr_fee) fee
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where web_sales.ws_web_page_sk = web_page.wp_web_page_sk
   and web_sales.ws_item_sk = web_returns.wr_item_sk
   and web_sales.ws_order_number = web_returns.wr_order_number
   and web_sales.ws_sold_date_sk = date_dim.d_date_sk and d_year = 1998
   and cd1.cd_demo_sk = web_returns.wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = web_returns.wr_returning_cdemo_sk
   and customer_address.ca_address_sk = web_returns.wr_refunded_addr_sk
   and reason.r_reason_sk = web_returns.wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '4 yr Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'D'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Primary' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'U'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('KY', 'GA', 'NM')
     and ws_net_profit between 100 and 200  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('MT', 'OR', 'IN')
     and ws_net_profit between 150 and 300  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('WI', 'MO', 'WV')
     and ws_net_profit between 50 and 250  
    )
   )
group by r_reason_desc
order by r, wq, ref, fee
limit 100;


