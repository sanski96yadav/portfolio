Created an automated ETL pipelines to run daily to fetch current weather data from Open Meteo API and load it into PostgreSQL using Apache Airflow. The Airflow was ran with Docker. pgAdmin was used to manage PostgreSQL. 

After running the Docker containers, the Airflow webserver UI was accessed on my local machine via localhost:8080. Similarly, pgAdmin on localhost:5050, both of which were exposed from their respective containers through Docker port mapping

 Approach:
 - Started with building a simple pipeline to fetch weather data only one city i.e. Munich
 - After its successful execution, though of scaling the pipeline for other cities, to replicate real business scenario
 - Created a pipeline for 4 multiple cities, Munich, Mumbai, Boston and London. The latitudes and logitudes for these 4 countries were automatically pass into URL end point by creating an Airflow task for it
 - The scaled up pipeline uses the same DAG (for Munich) but by expanding extraction task for multiple cities
 - The screenshots of Airflow UI and table in PostgreSQL is attached to attest the successful implementation of the pipeline 

DAG for multiple cities (Munich, Mumbai, Boston and London) weather data:
- Get the latitude and longitude from a table in PostgreSQL
- Extract data for these countries
- Transform the data
- Load data into a table in PostgreSQL


Things could be done in real business scenario:
- Would use third party latitiude and longitude for all cities in world and not limited to 4 cities (Munich, Mumbai, Boston and London)

Open Meteo API URL:
- https://api.open-meteo.com/v1/forecast?latitude={Latitude}&longitude={longitide}&current_weather=true
