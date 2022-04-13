## Effect of advertisement on sales. Average daily increase per store.


**The experiment shows** the changes of **Coffee** sales **during** the advertisement period in comparison to non advertisement period. 
<br/> **It based on the comparison** between the **control** and **experimental** store groups during weeks with and without advertisement.



### Import Libraries
```
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import scipy.stats as st
import numpy as np

```
### Load advertisement data and filter 'Coffee' products
Selection of data from **'****.tbl_silver_ad_reporting'** table <br/>
Filtering by: dates, file_location and advertisement name.

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
https://docs.databricks.com/spark/2.x/spark-sql/language-manual/select.html

Selection of the unique stores participated in the advertisement of 'Coffee' and the dates when the ad is running. The date is needed for later use.
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

# show
print(stores_id_with_ads[:5])
print(date_with_ads[:5])
```

<img width="776" alt="Screen Shot 2022-04-11 at 11 10 55 am" src="https://user-images.githubusercontent.com/65950685/162649483-d87354b6-134e-4a9b-a396-bd71a9b70321.png">


### Load and Filter for 'COFFEE' category during testing period from sales data
The 'indicator_for_cancelled_items' filters the transactions that have been cancelled.
```
%sql
create or replace temporary view temp_sale AS (
    SELECT id, site_id, local_date, sales_qty_su_number, article_name, fine_category, 
    transaction_type, indicator_for_cancelled_items
    FROM ***_**.tbl_silver_pos
    WHERE (local_date like '%202107%' OR
          local_date like '%202108%') 
      AND fine_category like 'COFFEE' 
      AND transaction_type = '1001'  
      AND indicator_for_cancelled_items is NULL  
);
```

### Transformation of the temporary view to the spark dataframe. 
* Removal of the columns used for filtering. <br/>
* Transform string column "local_date" to 'date' type**

```
# spark
sales = spark.read.table("temp_sale")
sales = sales.drop("fine_category", "transaction_type", "indicator_for_cancelled_items", "article_name") 

# date
date = sales.select(col("local_date"),to_date(col("local_date"),"yyyyMMdd").alias("date"))
date = date.dropDuplicates()
sales = (sales
         .join(date, sales["local_date"] == date["local_date"])
         .select(sales["*"], date['date']))
sales = sales.drop("local_date") 
```


**Selection of the data from control and experiment store groups**
```
# Experiment group
sales_no_ads = sales.where(~F.col("site_id").isin(stores_id_with_ads))
sales_no_ads = (sales_no_ads.groupBy('site_id', 'date').sum()).drop("sum(site_id)", "site_id") 
sales_no_ads = sales_no_ads.groupBy('date').mean()
sales_no_ads = sales_no_ads.withColumnRenamed("avg(sum(sales_qty_su_number))","mean_sales_no_ads")

# Store group
sales_ads = sales.where(F.col("site_id").isin(stores_id_with_ads))
sales_ads = (sales_ads.groupBy('site_id', 'date').sum()).drop("sum(site_id)", "site_id") 
sales_ads = sales_ads.groupBy('date').mean()
sales_ads = sales_ads.withColumnRenamed("avg(sum(sales_qty_su_number))","mean_sales_ads")
```

**Merge control and experimental stores**
* Add a line for ads and non ads period.
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
### The result (df2): result and plot:

<img width="600" alt="Screen Shot 2022-04-11 at 11 16 17 am" src="https://user-images.githubusercontent.com/65950685/162652506-93a59be4-cf8a-4839-8057-b4c341de20a5.png">

**The plot below shows** the number of items sold in the random store per day on average. <br/>
**There are two periods (green line):** without advertisement (first period) and with advertisement (second period). <br/>
**The testing period:** from 2021-07-01 until 2021-08-31 . Ad start date: 2021-07-27 <br/>

<img width="500" alt="Screen Shot 2022-04-11 at 11 17 40 am" src="https://user-images.githubusercontent.com/65950685/162652502-26c55166-2144-4121-8fec-878f21bec519.png">


### Analysis (Calculations)
#### The mean difference of Coffee sales during **non advertisement** dates (in percentages): 7.12%
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

<img width="762" alt="Screen Shot 2022-04-11 at 12 41 01 pm 1" src="https://user-images.githubusercontent.com/65950685/162661066-32875e5a-3cae-4a1c-8119-218755661367.png">

#### The mean difference of Coffee sales during **advertisement** dates (in percentages): 7.16%

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

<img width="795" alt="Screen Shot 2022-04-11 at 12 41 01 pm 2" src="https://user-images.githubusercontent.com/65950685/162661104-94584f6c-adc6-4f92-92f1-16100fd777bc.png">


#### The mean difference of Coffee sales during **advertisement** dates (in numbers) is 0.12

**The Control store is updated:** it multiplied by the percentage difference between the control and experimental store group.  <br/>
* In the first period (no ads) the average daily number of sales in the experimental stores group is 7.12% higher.
* It is higher at 7.16% in the second period (ads). 
* This uplift is flattened for in the second period below. <br/>

By removing the difference, we can see the actual difference in numbers:

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
<img width="774" alt="Screen Shot 2022-04-11 at 12 41 31 pm 1" src="https://user-images.githubusercontent.com/65950685/162661130-b4347b65-c3e4-4bea-ac71-2a304dad45bc.png">
0.12

**P-value analysis**  <br/>
* If the Sig. value of the Shapiro-Wilk test is greater than 0.05, the data is normal. 
* If it is below 0.05, the data significantly deviate from a normal distribution. 
* The test is statistically significant.
```
print(st.shapiro(yes_ads_amount[['change_number']]))
```
<img width="583" alt="Screen Shot 2022-04-11 at 12 41 31 pm 3" src="https://user-images.githubusercontent.com/65950685/162675789-283b7d72-cebc-454c-952b-1420f5903c2c.png">


**The average daily increased** in the number of sold Coffee is 0.12 <br/>
**The confidence interval:** <br/>
**Period 1:** 6.71, 7.52 <br/>
**Period 2:** 6.83, 7.49 <br/>

Despite the significant p-value, the average daily increase does not show a satisfactory result.
