Created an automated ETL pipelines to run daily to fetch data from wearher API and load it into PostgreSQL using Apache Airflow. The Airflow was ran with Docker. pgAdmin was used to manage PostgreSQL. 

After running the Docker containers, the Airflow webserver UI was accessed on my local machine via localhost:8080. Similarly, pgAdmin on localhost:5050, both of which were exposed from their respective containers through Docker port mapping

 Approach:
 - Started with building a simple pipeline to fetcg weather data for mUnich city
 - After its successful execution, though of scaling the pipeline for other cities, to replicate real business scenario
 - Created a pipeline for 4 cities, Munich, Mumbai, Boston and London using the same DAG but 
