## Effect of advertisement on sales. Average daily increase per store.


#### The experiment show the increase of **Hot Drinks** during advertisement period.


**The plot below shows** the number of items on average sold in the random store per day. <br/>
**There are two periods (green line):** without advertisement (first period) and with advertisement (second period). <br/>
**There are two store groups:** control stores and experiment stores.

<img width="400" alt="hot_drinks" src="https://user-images.githubusercontent.com/65950685/160533683-70c5e207-e242-4af0-9ac5-ba2ae6f406e9.png">

**The testing period:** from 2021-07-01 until 2021-08-31 . Ad start date: 2021-07-27 <br/>

In the first period the average daily number of sales in the experimental store group is 7.12% higher and this uplift is compensated for in the second period. <br/>

**The average daily increased** in the number of sold Hot Drinks is 0.12 <br/>
**The confidence interval:** <br/>
**Period 1:** 6.71, 7.52 <br/>
**Period 2:** 6.83, 7.49 <br/>

### Import Libraries
```
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import numpy as np
import scipy.stats as st
```
### Load advertisement data and filter 'Hot Drinks'

```
%sql
create or replace temporary view temp_ad AS (
    SELECT id, campaign, campaign_name, ad_name, local_date, customer_store_id, category, file_location
    FROM ****.tbl_silver_ad_reporting
    WHERE (local_date >= '2021-07-01' AND local_date < '2022-09-01') 
      AND file_location like '%/****************/%' 
      AND ad_name like '%offe%'   
);
```
### Select unique stores participated in the advertisement of 'Hot Drinks' and dates when ad is running:
```
ads = spark.read.table("temp_ad")

def unique_col_values_to_list(i):    
    value = (ads
             .select(col(i))
             .dropDuplicates()
             .toPandas())
    return value[i].to_list()

stores_id_with_ads = unique_col_values_to_list('customer_store_id')
date_with_ads = unique_col_values_to_list('local_date')
date_with_ads = [date_obj.strftime('%Y-%m-%d') for date_obj in date_with_ads]
```

### COMMAND 
```
print(stores_id_with_ads[:5])
print(date_with_ads[:5])
```
### Load and Filter for 'HOT DRINKS' category during testing period. The 'indicator_for_cancelled_items' filter transactions that have been cancelled.
```
%sql
create or replace temporary view temp_sale AS (
    SELECT id, site_id, local_date, sales_qty_su_number, article_name, fine_category, 
    transaction_type, indicator_for_cancelled_items
    FROM ***_**.tbl_silver_pos
    WHERE (local_date like '%202107%' OR
          local_date like '%202108%') 
      AND fine_category like 'HOT DRINKS' 
      AND transaction_type = '1001'  
      AND indicator_for_cancelled_items is NULL  
);
```
### Transform temporary view to spark dataframe. Drop columns used for filtering.
```
sales = spark.read.table("temp_sale")
sales = sales.drop("fine_category", "transaction_type", "indicator_for_cancelled_items", "article_name") 
```

### Transform string column "local_date" to 'date' type
```
date = sales.select(col("local_date"),to_date(col("local_date"),"yyyyMMdd").alias("date"))
date = date.dropDuplicates()
sales = (sales
         .join(date, sales["local_date"] == date["local_date"])
         .select(sales["*"], date['date']))
sales = sales.drop("local_date") 
```

### Select sales data using the list with stores from the control store group
```
sales_no_ads = sales.where(~F.col("site_id").isin(stores_id_with_ads))
sales_no_ads = (sales_no_ads.groupBy('site_id', 'date').sum()).drop("sum(site_id)", "site_id") 
sales_no_ads = sales_no_ads.groupBy('date').mean()
sales_no_ads = sales_no_ads.withColumnRenamed("avg(sum(sales_qty_su_number))","mean_sales_no_ads")
```

### Select sales data using the list with stores from the experiment store group
```
sales_ads = sales.where(F.col("site_id").isin(stores_id_with_ads))
sales_ads = (sales_ads.groupBy('site_id', 'date').sum()).drop("sum(site_id)", "site_id") 
sales_ads = sales_ads.groupBy('date').mean()
sales_ads = sales_ads.withColumnRenamed("avg(sum(sales_qty_su_number))","mean_sales_ads")
```

### Merge control and experimental
```
data = (sales_no_ads
         .join(sales_ads, sales_no_ads["date"] == sales_ads["date"])
         .select(sales_no_ads["*"], sales_ads['mean_sales_ads']))

# Add period line
from pyspark.sql.functions import when
df2 = data.withColumn("period", when(data.date < "2021-07-27","160")
                                 .when(data.date > "2021-07-27","175")
                                 .when(data.date == "2021-07-27","175")
                                 )
df2 = df2.withColumnRenamed("mean_sales_no_ads","control_store")
df2 = df2.withColumnRenamed("mean_sales_ads","experiment_store")
display(df2) 
```
### The daily sales per store

```
sales = spark.read.table("temp_sale")
sales = sales.filter(sales.indicator_for_cancelled_items.isNull())
sales = sales.drop("fine_category", "transaction_type", "indicator_for_cancelled_items") 

test = sales.groupBy('site_id', 'local_date').sum()
test.agg({'sum(sales_qty_su_number)': 'mean'}).show()

```
### Fuel check for both groups

```
%sql
create or replace temporary view temp_fuel AS (
    SELECT id, has_fuel
    FROM 711_au.tbl_gold_pos
    );
```
### Line
```
fuel = spark.read.table("temp_fuel")
fuel = fuel.dropDuplicates()

sales_no_ads_for_fuel_only = sales.where(~F.col("site_id").isin(stores_id_with_ads))
sales_ads_for_fuel_only = sales.where(F.col("site_id").isin(stores_id_with_ads))

no = (sales_no_ads_for_fuel_only
         .join(fuel, sales_no_ads_for_fuel_only["id"] == fuel["id"])
         .select(sales_no_ads_for_fuel_only["site_id"], fuel['has_fuel']))

yes = (sales_ads_for_fuel_only
         .join(fuel, sales_ads_for_fuel_only["id"] == fuel["id"])
         .select(sales_ads_for_fuel_only["site_id"], fuel['has_fuel']))

no = no.dropDuplicates()
yes = yes.dropDuplicates()
display(no.groupBy('has_fuel').count())
display(yes.groupBy('has_fuel').count())

```
### Fuel Table

Choice | no ads store |  ads stores |
--- | --- | --- | 
fuel | 131 | 444 | 
no fuel | 53 | 83 |
percentage | 0.29 | 0.16 |



### No advertisement period
```
no_ads = df2.where(~F.col("date").isin(date_with_ads))
no_ads = no_ads.toPandas()
no_ads = no_ads.sort_values(by='date') [:46]

def percentage_change(col1,col2):
    return ((col2 - col1) / col1) * 100

no_ads['change'] = percentage_change(no_ads['control_store'],no_ads['experiment_store'])   
no_ads2 = no_ads['change']
print(st.norm.interval(alpha=0.95, loc=np.mean(no_ads2), scale=st.sem(no_ads2)))
no_ads2.mean()
```
### Yes advertisement period
### Percentage

```
yes_ads = df2.where(F.col("date").isin(date_with_ads))
yes_ads = yes_ads.toPandas()

def percentage_change(col1,col2):
    return ((col2 - col1) / col1) * 100

yes_ads['change'] = percentage_change(yes_ads['control_store'],yes_ads['experiment_store'])   
yes_ads.sort_values(by='date') 

yes_ads2 = yes_ads['change']
print(st.norm.interval(alpha=0.95, loc=np.mean(yes_ads2), scale=st.sem(yes_ads2)))
yes_ads2.mean()
```
### Yes advertisement period
### Amount

```

yes_ads_amount = df2.where(F.col("date").isin(date_with_ads))
yes_ads_amount = yes_ads_amount.toPandas()

yes_ads_amount['updated_control_store'] = yes_ads_amount['control_store'] * 1.07120298657144583
yes_ads_amount['change_number'] = yes_ads_amount['experiment_store'] - yes_ads_amount['updated_control_store']
yes_ads_amount.sort_values(by='date') 

yes_ads_amount2 = yes_ads_amount['change_number']
print(st.norm.interval(alpha=0.95, loc=np.mean(yes_ads_amount2), scale=st.sem(yes_ads_amount2)))
yes_ads_amount2.mean()

```
### Line
```
print(st.shapiro(yes_ads_amount[['change_number']]))
print(st.shapiro(yes_ads_amount[['control_store']]))
print(st.shapiro(yes_ads_amount[['experiment_store']]))

print('')

yes_ads_amount = yes_ads_amount[['control_store', 'experiment_store']]
# chi-square test
chiRes = st.chi2_contingency(yes_ads_amount)
# Details
print(f'chi-square statistic: {chiRes[0]}')
print(f'p-value: {chiRes[1]}')
print(f'degree of freedom: {chiRes[2]}')
#print('expected contingency table') 
#print(chiRes[3])

yes_ads_amount2.hist()
```
