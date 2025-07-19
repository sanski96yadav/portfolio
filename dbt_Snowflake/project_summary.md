```Description```

Designed and implemented a ```scalable data warehouse DWH architecture``` in ```Snowflake using dbt``` as part of a portfolio project. 
Covered the full data pipeline lifecycle — from data ingestion and transformation to building analytics-ready data models with focus on aligning with realistic business scenarios and best practices in data engineering

```DWH Archirecture```
 staging > prep > mart (Star schema - facts and dim tables) > report

```Dataset```

Raw data was downloaded from Kaggle and consisted of the following columns:

| Column names     | Description     | 
| ------------- |-------------|
| InvoiceNo | Invoice number uniquely assigned to each transaction |
| StockCode | Product (item) code uniquely assigned to each distinct product |
| Description | Product (item) name |
| Quantity | The quantities of each product (item) per transaction |
| InvoiceDate | Invoice Date and time when each transaction was generated |
| UnitPrice |  Product price per unit in Euros |
| CustomerID |  Customer number uniquely assigned to each customer |
| Country | Name of the country where each customer resides |





```Approach followed```:

- ```Data Extraction```: Downloaded the data from Kaggle 
- ```Requirements Gathering```: After a quick look at the data, built business requirements that can be addressed with it
- ```Technologies```: Set up the required tools and platforms like dbt and Snowflake
- ```Data Loading```: Loaded data into Snowflake DWH
- ```Project Management```: Created a Notion folder to manage the project
- ```Data Understanding & Cleaning```: Explored data in Snowflake UI - noted the observations from data in Excel file and used them further for data cleaning
- ```Data Architecture```: Designed DWH architecture 
- ```Data Modeling```: Created analytics-ready data models by transforming data in 4 layers ```staging> prep> mart> report``` in Snowflake and leverage Star Schema data modeling technique
- ```Data Accuracy```: Verified the results of the models. Ran the tests, macros and snapshots (SCD typ2)
- ```Documentation```: Generated documentation

```dbt features used```:

- tests
- macros
- surrogate keys
- snapshots
- documentation

```dbt commands ran```:

- dbt run --select 'model_name'
- dbt snapshot
- dbt test
- dbt docs generate
- dbt docs serve
