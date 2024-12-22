# ETL Cereales Pipeline

A robust ETL (Extract, Transform, Load) pipeline built with Apache Airflow to process cereals genetics data. This pipeline extracts data from a MSSQL database, performs transformations, and saves the results to CSV files.

## 🏗️ Architecture

The pipeline consists of three main tasks:

1. **Extract**: Pulls data from MSSQL database table `Cereales_Genetics_Advanced`
2. **Transform**: Filters records where `Rendement_Par_Hectare > 75`
3. **Load**: Saves the transformed data to CSV files

## 🔧 Prerequisites

- Docker and Docker Compose
- Git
- At least 4GB of RAM
- At least 2 CPU cores
- At least 10GB of disk space

## 🛠️ Tech Stack

- Apache Airflow 2.10.3
- Python 3.12
- Microsoft SQL Server 2019
- Redis 7.2
- PostgreSQL 13

## 📁 Project Structure

```
.
├── dags/
│   └── airflow_dag.py
├── data/
│   └── clean_data.csv
├── logs/
├── plugins/
├── config/
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## 🚀 Getting Started

1. Clone the repository:

```bash
git clone https://github.com/Mouaadag/ETL-pipeline
cd ETL-pipeline
```

2. Create required directories:

```bash
mkdir -p ./dags ./logs ./plugins ./config ./data
```

3. Set proper permissions:

```bash
chmod -R 777 ./data
```

4. Start the containers:

```bash
docker-compose up -d
```

5. Access Airflow web interface:

- URL: `http://localhost:8080`
- Username: `mouaad`
- Password: `mouaadpwd`

## ⚙️ Configuration

### MSSQL Connection Setup

1. Go to Airflow web interface → Admin → Connections
2. Add new connection:
   - Conn Id: `mssql_connection_id`
   - Conn Type: Microsoft SQL Server
   - Host: `mssql-server`
   - Schema: Your database name
   - Login: `sa`
   - Password: `MyComplexPassword123!`
   - Port: `1433`

## 📅 Scheduling

The pipeline is scheduled to run every three months on the 22nd day:

- Starting from: November 22, 2024
- Next runs: February 22, May 22, August 22, November 22
- Time: 00:00 UTC

## 📊 Data Flow

```
MSSQL Database
      ↓
Extract Task (Raw Data)
      ↓
Transform Task (Filter Rendement > 75)
      ↓
Save Task (CSV Output)
```

## 🔍 Monitoring

- Airflow UI: `http://localhost:8080`
- Flower (Celery monitoring): `http://localhost:5555`

## 🛟 Troubleshooting

Common issues and solutions:

1. **Permission Denied Error**:

   - Ensure the `data` directory has proper permissions
   - Run `chmod -R 777 ./data`

2. **MSSQL Connection Issues**:
   - Verify MSSQL server is running: `docker ps`
   - Check connection credentials
   - Ensure database exists and is accessible

## 📜 License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## 👥 Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## ✍️ Authors

- Mouaad AGOURRAM - _Data Engineer_

## 🙏 Acknowledgments

- Apache Airflow team
- Microsoft SQL Server team
- All contributors
