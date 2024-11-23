FROM apache/airflow:2.10.3
USER root
RUN sudo pip install apache-airflow-providers-microsoft-mssql
USER airflow
