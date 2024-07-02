# Citation-Network-Analysis-with-Apache-Spark
This project demonstrates the use of Apache Spark for analyzing large-scale bibliographic data. The analysis focuses on identifying top-cited papers and authors, understanding collaboration patterns, analyzing publication trends, and more. The project is implemented in Python using PySpark.

# Repository Structure
simple_app.py: This script initializes the Spark session and loads the bibliographic dataset. It sets up the environment for subsequent data analysis tasks.
process_data.py: This script contains the main analysis tasks:

# Setup and Configuration
Apache Spark: Ensure Apache Spark is installed and properly configured on your system.
Python Environment: Install the required Python packages using pip install -r requirements.txt.
Dataset: Place the dblp_v14.parquet file in the specified directory (C:/Users/HANIF/Downloads/dblp_v14.json/).

# Running the Analysis
Load Data: Run simple_app.py to initialize the Spark session and load the dataset.
Perform Analysis: Execute process_data.py to perform the analysis tasks. The results will be visualized using Matplotlib.

# Results and Discussion
The analysis revealed insights into the most influential papers and authors, collaboration patterns, and trends in academic publishing. The visualizations effectively communicate these findings, showcasing the power of Apache Spark in handling and analyzing large-scale bibliographic data.

# Conclusion and Future Work
This project demonstrates the capabilities of Apache Spark in bibliographic data analysis. Future work can focus on integrating more granular data, such as citation contexts, and incorporating predictive modeling to enhance the understanding of the academic landscape.
