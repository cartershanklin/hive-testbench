
select  
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from
        call_center,
        catalog_returns,
        date_dim,
        customer,
        customer_address,
        customer_demographics,
        household_demographics
where
        catalog_returns.cr_call_center_sk       = call_center.cc_call_center_sk
and     catalog_returns.cr_returned_date_sk     = date_dim.d_date_sk
and     catalog_returns.cr_returning_customer_sk= customer.c_customer_sk
and     customer_demographics.cd_demo_sk              = customer.c_current_cdemo_sk
and     household_demographics.hd_demo_sk              = customer.c_current_hdemo_sk
and     customer_address.ca_address_sk           = customer.c_current_addr_sk
and     d_year                  = 1999 
and     d_moy                   = 11
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like '0-500%'
and     ca_gmt_offset           = -7
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by Returns_Loss desc;


