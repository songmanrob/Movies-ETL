# Movies-ETL

## Challenge
We were tasked with automating the ETL process using raw data from Wikipedia and MovieLens.

Originally, we took the raw data, cleaned and transformed it, merged the Wikipedia and Kaggle data, then loaded the data into PostgreSQL tables.

Then, we needed to automate the process.

In order to automate, we had to assume certain things:
1) That the data would be supplied in the same file types every time (JSON for Wikipedia and csv files for MovieLens)
2) That there would be only two PostgreSQL tables to remove data from at the start of the automation
3) That there would be exactly the same columns in the data each time
4) That the column names would remain exactly the same each time
5) That no other anomalies are introduced into the data
