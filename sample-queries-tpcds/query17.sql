select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as_store_returns_quantitycount
       ,avg(sr_return_quantity) as_store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2000Q1'
   and d1.d_date_sk = store_sales.ss_sold_date_sk
   and item.i_item_sk = store_sales.ss_item_sk
   and store.s_store_sk = store_sales.ss_store_sk
   and store_sales.ss_customer_sk = store_returns.sr_customer_sk
   and store_sales.ss_item_sk = store_returns.sr_item_sk
   and store_sales.ss_ticket_number = store_returns.sr_ticket_number
   and store_returns.sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
   and store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk
   and store_returns.sr_item_sk = catalog_sales.cs_item_sk
   and catalog_sales.cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
limit 100;
