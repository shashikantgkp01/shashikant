# shashikant's hive assignment 

##### Storing  raw data into hdfs location
 hdfs dfs -mkdir /assign
 hdfs dfs -chmod 777 /assign
 hdfs dfs -copyFromLocal /home/cloudera/new/sales_order_data.csv /assign
 hdfs dfs -ls /assign 
 
 ##### Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table
 
 ## create database
create database hive_assignment;
use hive_assignment;

## create table 
create table sales_order_data_csv
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1")
; 

###### Load data from hdfs path into "sales_order_csv" 

load data inpath '/assign/' into table sales_order_data_csv;

###### Create an internal hive table which will store data in ORC format "sales_order_orc"

create table sales_order_data_orc
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;

###### Load data from "sales_order_csv" into "sales_order_orc"
from sales_order_data_csv insert overwrite table sales_order_orc select * ;


###### question1 - Calculatye total sales per year
select YEAR_ID , sum(SALES) as total_sales from sales_order_orc group by YEAR_ID ;

$$$  result-
2003    3516979.547241211
2004    4724162.593383789
2005    1791486.7086791992


###### question2 - Find a product for which maximum orders were placed
select PRODUCTLINE  from (select  PRODUCTLINE,sum(QUANTITYORDERED) as total_order from sales_data_orc group by PRODUCTLINE order by total_order DESC) a1 limit 1;

$$$  result-
Classic Cars   



###### question3 - Calculate the total sales for each quarter
select QTR_ID , sum(SALES) as total_sales from sales_data_orc group by QTR_ID;

$$$ result-

1       2350817.726501465
2       2048120.3029174805
3       1758910.808959961
4       3874780.010925293


###### question4 - In which quarter sales was minimum
select QTR_ID from (select QTR_ID,sum(SALES) as total_sales from sales_data_orc group by QTR_ID order by total_sales) a1 limit 1;

$$$ result - 
4


##### question5-  In which country sales was maximum and in which country sales was minimum
## maxium sales in country
select COUNTRY as max_sale_country from (select sum(SALES) as total_sale,COUNTRY from sales_data_orc group by COUNTRY order by total_sales DESC) a1 limit 1;

$$$ result-
USA

## minimun sales in country 
select COUNTRY as min_sale_country from (select sum(SALES) as total_sale,COUNTRY from sales_data_orc group by COUNTRY order by total_sales) a1 limit 1;

$$$ result-
Ireland



###### quention6 - Calculate quartelry sales for each city


