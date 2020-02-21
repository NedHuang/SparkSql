### 1

##### Task:
Find how many items were shipped on a date.
##### SQL:
select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';

### 2
##### Task:
Find which clerks were responsible for processing items that were shipped on a particular date.
##### SQL:
select o_clerk, o_orderkey from lineitem, orders
where
  l_orderkey = o_orderkey and
  l_shipdate = 'YYYY-MM-DD'
order by o_orderkey asc limit 20;
### 3.
##### Task: 
Find  what are the names of parts and suppliers of items shipped on a particular date
##### SQL:
select l_orderkey, p_name, s_name from lineitem, part, supplier
where
  l_partkey = p_partkey and
  l_suppkey = s_suppkey and
  l_shipdate = 'YYYY-MM-DD'
order by l_orderkey asc limit 20;

### 4.
##### Task:
Find how many items were shipped to each country on a particular date.
##### SQL:
select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
where
  l_orderkey = o_orderkey and
  o_custkey = c_custkey and
  c_nationkey = n_nationkey and
  l_shipdate = 'YYYY-MM-DD'
group by n_nationkey, n_name
order by n_nationkey asc;

### 5.
##### Task:
Compare the shipments to Canada vs. the United States by month

##### SQL: 
Similar to what have done in 4, but not on a particular date. 

### 6.
##### Task:
Report the amount of business that was billed, shipped, and returned:
##### SQL:
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from lineitem
where
  l_shipdate = 'YYYY-MM-DD'
group by l_returnflag, l_linestatus;

### 7.
##### Task:
Retrieves the 10 unshipped orders with the highest value.
##### SQL:
select
  c_name,
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from customer, orders, lineitem
where
  c_custkey = o_custkey and
  l_orderkey = o_orderkey and
  o_orderdate < "YYYY-MM-DD" and
  l_shipdate > "YYYY-MM-DD"
group by
  c_name,
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc
limit 10;
