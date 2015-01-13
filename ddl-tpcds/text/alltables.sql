create database if not exists ${DB};
use ${DB};

drop table if exists call_center;

create external table call_center(
      cc_call_center_sk         int               
,     cc_call_center_id         string              
,     cc_rec_start_date        string                         
,     cc_rec_end_date          string                         
,     cc_closed_date_sk         int                       
,     cc_open_date_sk           int                       
,     cc_name                   string                   
,     cc_class                  string                   
,     cc_employees              int                       
,     cc_sq_ft                  int                       
,     cc_hours                  string                      
,     cc_manager                string                   
,     cc_mkt_id                 int                       
,     cc_mkt_class              string                      
,     cc_mkt_desc               string                  
,     cc_market_manager         string                   
,     cc_division               int                       
,     cc_division_name          string                   
,     cc_company                int                       
,     cc_company_name           string                      
,     cc_street_number          string                      
,     cc_street_name            string                   
,     cc_street_type            string                      
,     cc_suite_number           string                      
,     cc_city                   string                   
,     cc_county                 string                   
,     cc_state                  string                       
,     cc_zip                    string                      
,     cc_country                string                   
,     cc_gmt_offset             float                  
,     cc_tax_percentage         float
)
row format delimited fields terminated by '|' 
location '${LOCATION}/call_center';
create database if not exists ${DB};
use ${DB};

drop table if exists catalog_page;

create external table catalog_page(
      cp_catalog_page_sk        int               
,     cp_catalog_page_id        string              
,     cp_start_date_sk          int                       
,     cp_end_date_sk            int                       
,     cp_department             string                   
,     cp_catalog_number         int                       
,     cp_catalog_page_number    int                       
,     cp_description            string                  
,     cp_type                   string
)
row format delimited fields terminated by '|' 
location '${LOCATION}/catalog_page';
create database if not exists ${DB};
use ${DB};

drop table if exists catalog_returns;

create external table catalog_returns
(
    cr_returned_date_sk       int,
    cr_returned_time_sk       int,
    cr_item_sk                int,
    cr_refunded_customer_sk   int,
    cr_refunded_cdemo_sk      int,
    cr_refunded_hdemo_sk      int,
    cr_refunded_addr_sk       int,
    cr_returning_customer_sk  int,
    cr_returning_cdemo_sk     int,
    cr_returning_hdemo_sk     int,
    cr_returning_addr_sk      int,
    cr_call_center_sk         int,
    cr_catalog_page_sk        int,
    cr_ship_mode_sk           int,
    cr_warehouse_sk           int,
    cr_reason_sk              int,
    cr_order_number           int,
    cr_return_quantity        int,
    cr_return_amount          float,
    cr_return_tax             float,
    cr_return_amt_inc_tax     float,
    cr_fee                    float,
    cr_return_ship_cost       float,
    cr_refunded_cash          float,
    cr_reversed_charge        float,
    cr_store_credit           float,
    cr_net_loss               float
)
row format delimited fields terminated by '|' 
location '${LOCATION}/catalog_returns';
create database if not exists ${DB};
use ${DB};

drop table if exists catalog_sales;

create external table catalog_sales
(
    cs_sold_date_sk           int,
    cs_sold_time_sk           int,
    cs_ship_date_sk           int,
    cs_bill_customer_sk       int,
    cs_bill_cdemo_sk          int,
    cs_bill_hdemo_sk          int,
    cs_bill_addr_sk           int,
    cs_ship_customer_sk       int,
    cs_ship_cdemo_sk          int,
    cs_ship_hdemo_sk          int,
    cs_ship_addr_sk           int,
    cs_call_center_sk         int,
    cs_catalog_page_sk        int,
    cs_ship_mode_sk           int,
    cs_warehouse_sk           int,
    cs_item_sk                int,
    cs_promo_sk               int,
    cs_order_number           int,
    cs_quantity               int,
    cs_wholesale_cost         float,
    cs_list_price             float,
    cs_sales_price            float,
    cs_ext_discount_amt       float,
    cs_ext_sales_price        float,
    cs_ext_wholesale_cost     float,
    cs_ext_list_price         float,
    cs_ext_tax                float,
    cs_coupon_amt             float,
    cs_ext_ship_cost          float,
    cs_net_paid               float,
    cs_net_paid_inc_tax       float,
    cs_net_paid_inc_ship      float,
    cs_net_paid_inc_ship_tax  float,
    cs_net_profit             float
)
row format delimited fields terminated by '|' 
location '${LOCATION}/catalog_sales';
create database if not exists ${DB};
use ${DB};

drop table if exists customer_address;

create external table customer_address
(
    ca_address_sk             int,
    ca_address_id             string,
    ca_street_number          string,
    ca_street_name            string,
    ca_street_type            string,
    ca_suite_number           string,
    ca_city                   string,
    ca_county                 string,
    ca_state                  string,
    ca_zip                    string,
    ca_country                string,
    ca_gmt_offset             float,
    ca_location_type          string
)
row format delimited fields terminated by '|' 
location '${LOCATION}/customer_address';
create database if not exists ${DB};
use ${DB};

drop table if exists customer_demographics;

create external table customer_demographics
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
)
row format delimited fields terminated by '|' 
location '${LOCATION}/customer_demographics';
create database if not exists ${DB};
use ${DB};

drop table if exists customer;

create external table customer
(
    c_customer_sk             int,
    c_customer_id             string,
    c_current_cdemo_sk        int,
    c_current_hdemo_sk        int,
    c_current_addr_sk         int,
    c_first_shipto_date_sk    int,
    c_first_sales_date_sk     int,
    c_salutation              string,
    c_first_name              string,
    c_last_name               string,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
)
row format delimited fields terminated by '|' 
location '${LOCATION}/customer';
create database if not exists ${DB};
use ${DB};

drop table if exists date_dim;

create external table date_dim
(
    d_date_sk                 int,
    d_date_id                 string,
    d_date                    string,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
)
row format delimited fields terminated by '|' 
location '${LOCATION}/date_dim';
create database if not exists ${DB};
use ${DB};

drop table if exists household_demographics;

create external table household_demographics
(
    hd_demo_sk                int,
    hd_income_band_sk         int,
    hd_buy_potential          string,
    hd_dep_count              int,
    hd_vehicle_count          int
)
row format delimited fields terminated by '|' 
location '${LOCATION}/household_demographics';
create database if not exists ${DB};
use ${DB};

drop table if exists income_band;

create external table income_band(
      ib_income_band_sk         int               
,     ib_lower_bound            int                       
,     ib_upper_bound            int
)
row format delimited fields terminated by '|' 
location '${LOCATION}/income_band';
create database if not exists ${DB};
use ${DB};

drop table if exists inventory;

create external table inventory
(
    inv_date_sk                 int,
    inv_item_sk                 int,
    inv_warehouse_sk            int,
    inv_quantity_on_hand        int
)
row format delimited fields terminated by '|' 
location '${LOCATION}/inventory';
create database if not exists ${DB};
use ${DB};

drop table if exists item;

create external table item
(
    i_item_sk                 int,
    i_item_id                 string,
    i_rec_start_date          string,
    i_rec_end_date            string,
    i_item_desc               string,
    i_current_price           float,
    i_wholesale_cost          float,
    i_brand_id                int,
    i_brand                   string,
    i_class_id                int,
    i_class                   string,
    i_category_id             int,
    i_category                string,
    i_manufact_id             int,
    i_manufact                string,
    i_size                    string,
    i_formulation             string,
    i_color                   string,
    i_units                   string,
    i_container               string,
    i_manager_id              int,
    i_product_name            string
)
row format delimited fields terminated by '|' 
location '${LOCATION}/item';
create database if not exists ${DB};
use ${DB};

drop table if exists promotion;

create external table promotion
(
    p_promo_sk                int,
    p_promo_id                string,
    p_start_date_sk           int,
    p_end_date_sk             int,
    p_item_sk                 int,
    p_cost                    float,
    p_response_target         int,
    p_promo_name              string,
    p_channel_dmail           string,
    p_channel_email           string,
    p_channel_catalog         string,
    p_channel_tv              string,
    p_channel_radio           string,
    p_channel_press           string,
    p_channel_event           string,
    p_channel_demo            string,
    p_channel_details         string,
    p_purpose                 string,
    p_discount_active         string 
)
row format delimited fields terminated by '|' 
location '${LOCATION}/promotion';
create database if not exists ${DB};
use ${DB};

drop table if exists reason;

create external table reason(
      r_reason_sk               int               
,     r_reason_id               string              
,     r_reason_desc             string                
)
row format delimited fields terminated by '|' 
location '${LOCATION}/reason';
create database if not exists ${DB};
use ${DB};

drop table if exists ship_mode;

create external table ship_mode(
      sm_ship_mode_sk           int               
,     sm_ship_mode_id           string              
,     sm_type                   string                      
,     sm_code                   string                      
,     sm_carrier                string                      
,     sm_contract               string                      
)
row format delimited fields terminated by '|' 
location '${LOCATION}/ship_model';
create database if not exists ${DB};
use ${DB};

drop table if exists store_returns;

create external table store_returns
(
    sr_returned_date_sk       int,
    sr_return_time_sk         int,
    sr_item_sk                int,
    sr_customer_sk            int,
    sr_cdemo_sk               int,
    sr_hdemo_sk               int,
    sr_addr_sk                int,
    sr_store_sk               int,
    sr_reason_sk              int,
    sr_ticket_number          int,
    sr_return_quantity        int,
    sr_return_amt             float,
    sr_return_tax             float,
    sr_return_amt_inc_tax     float,
    sr_fee                    float,
    sr_return_ship_cost       float,
    sr_refunded_cash          float,
    sr_reversed_charge        float,
    sr_store_credit           float,
    sr_net_loss               float             
)
row format delimited fields terminated by '|' 
location '${LOCATION}/store_returns';
create database if not exists ${DB};
use ${DB};

drop table if exists store_sales;

create external table store_sales
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float                  
)
row format delimited fields terminated by '|' 
location '${LOCATION}/store_sales';
create database if not exists ${DB};
use ${DB};

drop table if exists store;

create external table store
(
    s_store_sk                int,
    s_store_id                string,
    s_rec_start_date          string,
    s_rec_end_date            string,
    s_closed_date_sk          int,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              float,
    s_tax_precentage          float                  
)
row format delimited fields terminated by '|' 
location '${LOCATION}/store';
create database if not exists ${DB};
use ${DB};

drop table if exists time_dim;

create external table time_dim
(
    t_time_sk                 int,
    t_time_id                 string,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   string,
    t_shift                   string,
    t_sub_shift               string,
    t_meal_time               string
)
row format delimited fields terminated by '|' 
location '${LOCATION}/time_dim';
create database if not exists ${DB};
use ${DB};

drop table if exists warehouse;

create external table warehouse(
      w_warehouse_sk            int               
,     w_warehouse_id            string              
,     w_warehouse_name          string                   
,     w_warehouse_sq_ft         int                       
,     w_street_number           string                      
,     w_street_name             string                   
,     w_street_type             string                      
,     w_suite_number            string                      
,     w_city                    string                   
,     w_county                  string                   
,     w_state                   string                       
,     w_zip                     string                      
,     w_country                 string                   
,     w_gmt_offset              float                  
)
row format delimited fields terminated by '|' 
location '${LOCATION}/warehouse';
create database if not exists ${DB};
use ${DB};

drop table if exists web_page;

create external table web_page(
      wp_web_page_sk            int               
,     wp_web_page_id            string              
,     wp_rec_start_date        string                         
,     wp_rec_end_date          string                         
,     wp_creation_date_sk       int                       
,     wp_access_date_sk         int                       
,     wp_autogen_flag           string                       
,     wp_customer_sk            int                       
,     wp_url                    string                  
,     wp_type                   string                      
,     wp_char_count             int                       
,     wp_link_count             int                       
,     wp_image_count            int                       
,     wp_max_ad_count           int
)
row format delimited fields terminated by '|' 
location '${LOCATION}/web_page';
create database if not exists ${DB};
use ${DB};

drop table if exists web_returns;

create external table web_returns
(
    wr_returned_date_sk       int,
    wr_returned_time_sk       int,
    wr_item_sk                int,
    wr_refunded_customer_sk   int,
    wr_refunded_cdemo_sk      int,
    wr_refunded_hdemo_sk      int,
    wr_refunded_addr_sk       int,
    wr_returning_customer_sk  int,
    wr_returning_cdemo_sk     int,
    wr_returning_hdemo_sk     int,
    wr_returning_addr_sk      int,
    wr_web_page_sk            int,
    wr_reason_sk              int,
    wr_order_number           int,
    wr_return_quantity        int,
    wr_return_amt             float,
    wr_return_tax             float,
    wr_return_amt_inc_tax     float,
    wr_fee                    float,
    wr_return_ship_cost       float,
    wr_refunded_cash          float,
    wr_reversed_charge        float,
    wr_account_credit         float,
    wr_net_loss               float
)
row format delimited fields terminated by '|' 
location '${LOCATION}/web_returns';
create database if not exists ${DB};
use ${DB};

drop table if exists web_sales;

create external table web_sales
(
    ws_sold_date_sk           int,
    ws_sold_time_sk           int,
    ws_ship_date_sk           int,
    ws_item_sk                int,
    ws_bill_customer_sk       int,
    ws_bill_cdemo_sk          int,
    ws_bill_hdemo_sk          int,
    ws_bill_addr_sk           int,
    ws_ship_customer_sk       int,
    ws_ship_cdemo_sk          int,
    ws_ship_hdemo_sk          int,
    ws_ship_addr_sk           int,
    ws_web_page_sk            int,
    ws_web_site_sk            int,
    ws_ship_mode_sk           int,
    ws_warehouse_sk           int,
    ws_promo_sk               int,
    ws_order_number           int,
    ws_quantity               int,
    ws_wholesale_cost         float,
    ws_list_price             float,
    ws_sales_price            float,
    ws_ext_discount_amt       float,
    ws_ext_sales_price        float,
    ws_ext_wholesale_cost     float,
    ws_ext_list_price         float,
    ws_ext_tax                float,
    ws_coupon_amt             float,
    ws_ext_ship_cost          float,
    ws_net_paid               float,
    ws_net_paid_inc_tax       float,
    ws_net_paid_inc_ship      float,
    ws_net_paid_inc_ship_tax  float,
    ws_net_profit             float
)
row format delimited fields terminated by '|' 
location '${LOCATION}/web_sales';
create database if not exists ${DB};
use ${DB};

drop table if exists web_site;

create external table web_site
(
    web_site_sk           int,
    web_site_id           string,
    web_rec_start_date    string,
    web_rec_end_date      string,
    web_name              string,
    web_open_date_sk      int,
    web_close_date_sk     int,
    web_class             string,
    web_manager           string,
    web_mkt_id            int,
    web_mkt_class         string,
    web_mkt_desc          string,
    web_market_manager    string,
    web_company_id        int,
    web_company_name      string,
    web_street_number     string,
    web_street_name       string,
    web_street_type       string,
    web_suite_number      string,
    web_city              string,
    web_county            string,
    web_state             string,
    web_zip               string,
    web_country           string,
    web_gmt_offset        float,
    web_tax_percentage    float
)
row format delimited fields terminated by '|' 
location '${LOCATION}/web_site';
