select  ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     ,customer
     ,customer_address
     ,date_dim
 where catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
 	and customer.c_current_addr_sk = customer_address.ca_address_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or customer_address.ca_state in ('CA','WA','GA')
 	      or catalog_sales.cs_sales_price > 500)
 	and catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 	and date_dim.d_qoy = 2 and date_dim.d_year = 2000
 group by ca_zip
 order by ca_zip
 limit 100;


