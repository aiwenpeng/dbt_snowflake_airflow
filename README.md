# ETL Pipeline with dbt, Snowflake, Airflow

## Intro
This project uses industry standard tools (dbt, Snowflake, Airflow) to setup and to build an ETL pipeline.

## Setup dbt, Snowflake
```
-- create accounts
use role accountadmin;

create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

show grants on warehouse dbt_wh;

grant role dbt_role to user awpeng;
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;

use role dbt_role;

create schema if not exists dbt_db.dbt_schema;

-- clean up
use role accountadmin;

drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```
## Configure dbt_profile.yml
This tells dbt how and where to connect to your data warehouse.

#### 1. Install dbt
`python -m pip install dbt-core dbt-snowflake`
#### 2. Create dbt project
`dbt init my_dbt_project`
#### 3.Configure dbt Profile
Open ~/.dbt/profiles.yml and add your Snowflake configuration:
```
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_snowflake_account>
      user: dbt_user
      password: <your_password>
      role: dbt_role
      database: MY_DATABASE
      warehouse: MY_WAREHOUSE
      schema: PUBLIC
```
#### 4.Configure dbt_profile.yaml
```
models:
 snowflake_workshop:
   staging:
     materialized: view
     snowflake_warehouse: dbt_wh
   marts:
     materialized: table
     snowflake_warehouse: dbt_wh
```
## Create Source and Staging Files
Create models/staging/tpch_sources.yml
```
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey
```
Create staging models models/staging/stg_tpch_orders.sql
```
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}
```
Create models/staging/tpch/stg_tpch_line_items.sql
```
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```
## Macros (Don't Repeat Yourself or D.R.Y.)
Create macros/pricing.sql
```
sql {% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
(-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```
## Transform Models (Fact Tables, Data Marts)
Create Intermediate table models/marts/int_order_items.sql
```
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date
```
Create marts/int_order_items_summary.sql to aggregate info
```
select 
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
```
create fact model models/marts/fct_orders.sql
```
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
order by order_date
```
## Generic and Singular Tests
Create models/marts/generic_tests.yml
```
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```
Build Singular Tests tests/fct_orders_discount.sql
```
select
    *
from
    {{ref('fct_orders')}}
where
    item_discount_amount > 0
```

```
Create tests/fct_orders_date_valid.sql
```
select
    *
from
    {{ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```








Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
