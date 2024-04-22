# Weather Dashboard Application
Big Data Tools and Techniques
By Jacob Walter

## Contents
- `main.py` - Main function which connects to API and Mongo DB to extract data and generate plots
- `mongo_connection.py` - Class which handles connection and writes to Mongo DB
- `us-state-capitals.csv` - CSV file with State | City | Latitude | Longitude information.  This can be modified or replaced with a different csv with the same format.
- `config.yaml` (not included) - Config file containing Mongo DB connection info
- `dashboard.twb` - Tableau Dashboard built to show Weather History using Mongo DB Connection

## How to Run
1. Setup `config.yaml` file with Redis Connection information (username, password, cluster, database, collection)
2. Update the `us-state-capitals.csv` (or replace with a different csv) as needed to provide whichever locations you want to collect data on
3. Determine the range of dates you wish to collect data on and plot (`start_date` and `end_date`)
4. Run script as follows, providing the list of companies and datasource.
```
python main.py -s <start_date> -e <end_date>
```
This will connect to the weather api, pull the data for your range of dates, and write it to your Mongo DB.
5. Open the Tableau Dashboard, set your DataSource to your Mongo DB, and connect!  You should see the dashboard get populated
