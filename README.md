# ELT-Visualization_Flow

---

<h3><u>1. Project Structure:</u></h3>

```
ELT-Visualization-Flow
├── data_extraction
│   ├── every_earthquake.py
│   ├── location_extraction.py
│   ├── severe_climate_alert.py
│   └── weather_data_extraction.py
├── data_loading
│   ├── load_to_bigquery.py
│   └── severe_weather_loading.py
├── data_preprocessing
│   ├── data_segregation.py
│   └── severe_weather_preprocessing.py
├── gcp_pub_sub
│   ├── gcp_publisher.py
│   └── gcp_subscriber.py
│
├── ELT_DBT_Project(DBT Project Directory)
├── extract_all_data.py
├── README.md
└── requirements.txt
```

<h3><u>2. Project Flow:</u></h3>

**Introduction:**
In this project, We have developed a robust and automated system to gather, process, and analyze disaster data from various APIs, ensuring a seamless flow of information for informed decision-making. Below is a detailed step-by-step guide outlining the entire project flow:

**Step 1: Cron Job Setup**
Initiate the project by setting up a cron job to execute a Python script daily. This script serves as the backbone for data extraction from multiple APIs.

**Step 2: API Integration**
Utilize Python to interact with different APIs, including the Every Earthquake API, OpenMeteo Weather API, Flood and Cyclone Alerts API, and Google GeoCoding API. This ensures a comprehensive dataset covering a spectrum of environmental factors.

**Step 3: Data Cleaning and Standardization**
Implement data cleaning procedures in the Python script to ensure consistency and accuracy in the extracted information. Standardize the data according to pre-defined requirements for better downstream processing.

**Step 4: Google BigQuery and Pub/Sub Integration**
Leverage Google Cloud services to efficiently manage and store the extracted data. Use Pub/Sub to handle the data flow and decrease the load on Google BigQuery, ensuring optimal performance.

**Step 5: DBT Transformation**
Incorporate a DBT (Data Build Tool) project with SQL transformation codes. These codes are designed to transform raw data into a structured and meaningful format tailored to specific analytical needs.

**Step 6: Connecting Tableau to BigQuery**
Establish a connection between Tableau and Google BigQuery, enabling a seamless transfer of transformed data for analysis. This integration provides a user-friendly interface for visual exploration and reporting.

**Step 7: Data Analysis in Tableau**
Utilize Tableau's powerful features to conduct in-depth data analysis on the transformed dataset. Leverage various visualization options to extract meaningful insights from the disaster data.

**Step 8: TabPy Integration for Enhanced Calculations**
Integrate TabPy server to enhance the analytical capabilities within Tableau. This allows for more complex calculations and statistical analyses, further enriching the depth of insights derived from the data.

**Conclusion:**
This comprehensive project flow ensures an end-to-end automated pipeline for environmental data analysis. From data extraction and cleaning to transformation and visualization, each step is meticulously designed to provide accurate and actionable insights for stakeholders. The seamless integration of various technologies and platforms creates a robust ecosystem for informed decision-making in the realm of environmental monitoring and analysis.

<h3><u>3. Setup Procedure:</u></h3>

1. Navigate to the directory where you want to set up this project.
<br><br>
2. Open cmd/bash and run the below command:<br>
On Mac/Win: ``git clone https://github.com/sprakshith/ELT-Visualization-Flow.git``
<br><br>
3. Now create a virtual enviroment. <br>
On Mac: ``python3 -m venv ./venv``<br>
Example: ``python3 -m venv ./venv``
<br><br>
On Win: ``python -m venv  "[Path to Project Directory]\[NAME_OF_VIRTUAL_ENV]"``<br>
Example: ``python -m venv "D:\ELT-Visualization-Flow\venv"``
<br><br>
4. To activate the venv run the below command. <br>
On Mac: ``source venv/bin/activate`` <br>
On Win: ``venv\Scripts\activate.bat``
<br><br>
5. To install all the requirements run the below command. Execute this command whenever there is a change in requirements.txt file.<br>
On Mac/Win: ``pip install -r requirements.txt``
6. Setup a CRON Job to run the ```extract_all_data.py``` and ```dbt run``` command once a day.
7. Run the ```gcp_subscriber.py``` python file to be active all the time so that data can be appened to BigQuery in time.
8. Run the TabPy server.
