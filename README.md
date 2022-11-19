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
  3


##### question5-  In which country sales was maximum and in which country sales was minimum
##### maxium sales in country
select COUNTRY as max_sale_country from (select sum(SALES) as total_sale,COUNTRY from sales_data_orc group by COUNTRY order by total_sales DESC) a1 limit 1;

$$$ result-
USA

#### minimun sales in country 
select COUNTRY as min_sale_country from (select sum(SALES) as total_sale,COUNTRY from sales_data_orc group by COUNTRY order by total_sales) a1 limit 1;

$$$ result-
Ireland



###### quention6 - Calculate quartelry sales for each city
select CITY,sum(SALES),QTR_ID from sales_data_orc group by QTR_ID,CITY order by CITY,QTR_ID;

$$$ result -
Aaarhus	100595.5498046875	4
Allentown	6166.7998046875	2
Allentown	71930.61041259766	3
Allentown	44040.729736328125	4
Barcelona	4219.2001953125	2
Barcelona	74192.66003417969	4
Bergamo	56181.320068359375	1
Bergamo	81774.40008544922	4
Bergen	16363.099975585938	3
Bergen	95277.17993164062	4
Boras	31606.72021484375	1
Boras	53941.68981933594	3
Boras	48710.92053222656	4
Boston	74994.240234375	2
Boston	15344.640014648438	3
Boston	63730.7802734375	4
Brickhaven	31474.7802734375	1
Brickhaven	7277.35009765625	2
Brickhaven	114974.53967285156	3
Brickhaven	11528.52978515625	4
Bridgewater	75778.99060058594	2
Bridgewater	26115.800537109375	4
Brisbane	16118.479858398438	1
Brisbane	34100.030029296875	3
Bruxelles	18800.089721679688	1
Bruxelles	8411.949829101562	2
Bruxelles	47760.479736328125	3
Burbank	37850.07958984375	1
Burbank	8234.559936523438	4
Burlingame	13529.570190429688	1
Burlingame	42031.83020019531	3
Burlingame	65221.67004394531	4
Cambridge	21782.699951171875	1
Cambridge	14380.920043945312	2
Cambridge	48828.71942138672	3
Cambridge	54251.659912109375	4
Charleroi	16628.16015625	1
Charleroi	1711.260009765625	2
Charleroi	1637.199951171875	3
Charleroi	13463.480224609375	4
Chatswood	43971.429931640625	2
Chatswood	69694.40002441406	3
Chatswood	37905.14990234375	4
Cowes	26906.68017578125	1
Cowes	51334.15966796875	4
Dublin	38784.470458984375	1
Dublin	18971.959838867188	3
Espoo	51373.49072265625	1
Espoo	31018.230102539062	2
Espoo	31569.430053710938	3
Frankfurt	48698.82922363281	1
Frankfurt	36472.76025390625	4
Gensve	50432.549560546875	1
Gensve	67281.00903320312	3
Glen Waverly	14378.089965820312	2
Glen Waverly	12334.819580078125	3
Glen Waverly	37878.54992675781	4
Glendale	3987.199951171875	1
Glendale	20350.949768066406	2
Glendale	7600.1201171875	3
Glendale	34485.49987792969	4
Graz	8775.159912109375	1
Graz	43488.740234375	4
Helsinki	26422.819458007812	1
Helsinki	42744.0595703125	3
Helsinki	42083.499755859375	4
Kobenhavn	58871.110107421875	1
Kobenhavn	62091.880615234375	2
Kobenhavn	24078.610107421875	4
Koln	100306.58020019531	4
Las Vegas	33847.61975097656	2
Las Vegas	34453.84973144531	3
Las Vegas	14449.609741210938	4
Lille	20178.1298828125	1
Lille	48874.28088378906	4
Liverpool	91211.0595703125	2
Liverpool	26797.210083007812	4
London	8477.219970703125	1
London	32376.29052734375	2
London	83970.029296875	4
Los Angeles	23889.320068359375	1
Los Angeles	24159.14013671875	4
Lule	9748.999755859375	1
Lule	66005.8798828125	4
Lyon	101339.13977050781	1
Lyon	41535.11022949219	4
Madrid	357668.4899291992	1
Madrid	339588.0513305664	2
Madrid	69714.09008789062	3
Madrid	315580.80963134766	4
Makati City	55245.02014160156	1
Makati City	38770.71032714844	4
Manchester	51017.919860839844	1
Manchester	106789.88977050781	4
Marseille	2317.43994140625	1
Marseille	52481.840087890625	2
Marseille	20136.859985351562	4
Melbourne	49637.57067871094	1
Melbourne	60135.84033203125	2
Melbourne	91221.99914550781	4
Minato-ku	38191.38977050781	1
Minato-ku	26482.700256347656	2
Minato-ku	55888.65026855469	4
Montreal	58257.50012207031	2
Montreal	15947.290405273438	4
Munich	34993.92004394531	3
NYC	32647.809814453125	1
NYC	165100.33947753906	2
NYC	63027.92004394531	3
NYC	300011.6999511719	4
Nantes	59617.39978027344	1
Nantes	60344.990173339844	2
Nantes	61310.880126953125	3
Nantes	23031.589599609375	4
Nashua	12133.25	1
Nashua	119552.04949951172	4
New Bedford	48578.95935058594	1
New Bedford	45738.38952636719	3
New Bedford	113557.509765625	4
New Haven	36973.309814453125	2
New Haven	42498.760498046875	4
Newark	8722.1201171875	1
Newark	74506.06909179688	2
North Sydney	65012.41955566406	1
North Sydney	47191.76013183594	3
North Sydney	41791.949462890625	4
Osaka	50490.64013671875	1
Osaka	17114.43017578125	2
Oslo	34145.47021484375	3
Oslo	45078.759765625	4
Oulu	49055.40026855469	1
Oulu	17813.40008544922	2
Oulu	37501.580322265625	3
Paris	71494.17944335938	1
Paris	80215.4203491211	2
Paris	27798.480102539062	3
Paris	89436.60034179688	4
Pasadena	44273.359436035156	1
Pasadena	55776.119873046875	3
Pasadena	4512.47998046875	4
Philadelphia	27398.820434570312	1
Philadelphia	7287.240234375	2
Philadelphia	116503.07043457031	4
Reggio Emilia	41509.94006347656	2
Reggio Emilia	56421.650390625	3
Reggio Emilia	44669.740478515625	4
Reims	52029.07043457031	1
Reims	18971.959716796875	2
Reims	15146.31982421875	3
Reims	48895.59014892578	4
Salzburg	98104.24005126953	2
Salzburg	6693.2802734375	3
Salzburg	45001.10986328125	4
San Diego	87489.23010253906	1
San Francisco	72899.19995117188	1
San Francisco	151459.4805908203	4
San Jose	160010.27026367188	2
San Rafael	267315.2586669922	1
San Rafael	7261.75	2
San Rafael	216297.40063476562	3
San Rafael	163983.64880371094	4
Sevilla	54723.621154785156	4
Singapore	28395.18994140625	1
Singapore	92033.77014160156	2
Singapore	90250.07995605469	3
Singapore	77809.37023925781	4
South Brisbane	21730.029907226562	1
South Brisbane	10640.290161132812	3
South Brisbane	27098.800048828125	4
Stavern	54701.999755859375	1
Stavern	61897.19006347656	4
Strasbourg	80438.47985839844	2
Torino	94117.25988769531	3
Toulouse	15139.1201171875	1
Toulouse	17251.08056640625	3
Toulouse	38098.240234375	4
Tsawassen	31302.500244140625	2
Tsawassen	43332.349609375	3
Vancouver	75238.91955566406	4
Versailles	5759.419921875	1
Versailles	59074.90026855469	4
White Plains	85555.98962402344	4



##### question7- Find a month for each year in which maximum number of quantities were sold
select YEAR_ID,MONTH_ID,QUANT from 
(select *,rank() over (partition by YEAR_ID order by QUANT desc)ctr from 
(select YEAR_ID,MONTH_ID,sum(QUANTITYORDERED) QUANT from sales_data_orc  group by YEAR_ID,MONTH_ID order by YEAR_ID,MONTH_ID)a)b where ctr=1;

$$$ result- 
 2003    11      10179
 2004    11      10678
 2005    5       4357









