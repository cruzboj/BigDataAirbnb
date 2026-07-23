# Project - AirBnb BigData
<div align="center">
<img src="https://news.airbnb.com/wp-content/uploads/sites/4/2017/01/airbnb_vertical_lockup_web.png">
</div>

## Team Members
<table align="center">
  <tr>
    <td align="center">
      <a href="https://github.com/cruzboj">
        <img src="https://github.com/cruzboj.png?size=120" width="120px;" alt="cruzboj"/><br />
        <sub><b>cruzboj</b></sub>
      </a>
    </td>
    <td width="40"></td>
    <td align="center">
      <a href="https://github.com/ImTheCurse">
        <img src="https://github.com/ImTheCurse.png?size=120" width="120px;" alt="ImTheCurse"/><br />
        <sub><b>ImTheCurse</b></sub>
      </a>
    </td>
  </tr>
</table>

## 
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
        <a href="#prerequisites">Prerequisites</a>
    </li>
    <li>
        <a href="#installation">Installation</a>
        <ul>
            <li>
                <a href="#uv-setup">UV setup</a>
            </li>
            <li>
                <a href="#docker-setup">Docker setup</a>
            </li>
            <li>
                <a href="#credentials-setup">Credentials setup</a>
            </li>
        </ul>
    </li>
    <li>
        <a href="#running-the-project">Running The Project</a>
        <ul>
            <li>
                <a href="#run-project">Run Project</a>
            </li>
            <li>
                <a href="#kafka-simulate-producer">Kafka simulate Producer</a>
            </li>
        </ul>
    </li>
  </ol>
</details>

<!-- Prerequisites -->
## Prerequisites
Before running the project, make sure the following software is installed:

- Docker
- Docker Compose
- Python 3.13+
- **uv (required)**
> **Important:** This project uses **uv** for Python package and environment management. The project will not run correctly without it.

our project working with .gz format but can also work with csv
> **Important:** all data must be contained in file "BigDataAirbnb\project-root\data"
* "BigDataAirbnb\project-root\data\raw" must contain the raw data

* listing csv | csv.gz must contain name formating listing_<city_name>_<country_name>.csv.gz
* review csv | csv.gz must contain name formating reviews_<city_name>_<country_name>.csv.gz

<!-- INSTALLATION -->
## installation

<!-- UV SETUP -->
### Install uv
#### Windows (PowerShell)

```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
#### macOS / Linux
```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
```
Verify the installation:
```bash
    uv --version
```
For additional installation methods and troubleshooting, refer to the official documentation:

https://docs.astral.sh/uv/getting-started/installation/    
##

<!-- DOCKER SETUP -->
### Install Docker

Download and install Docker Desktop for your operating system:

- **Windows:** https://docs.docker.com/desktop/setup/install/windows-install/
- **macOS:** https://docs.docker.com/desktop/setup/install/mac-install/
- **Linux:** https://docs.docker.com/desktop/setup/install/linux/

> **Note:** Docker Compose is included with Docker Desktop.

### Verify the installation

```bash
docker --version
```

```bash
docker compose version
```

If the installation was successful, both commands should print the installed versions of Docker and Docker Compose.

For additional installation methods and troubleshooting, refer to the official documentation:

https://docs.docker.com/get-started/

### Credentials setup
Before running the stack, create your local `.env` file from `.env.example`.

#### Linux / macOS
```sh
chmod +x set_default_credentials.sh
./set_default_credentials.sh
```

#### Windows
Windows users should copy `.env.example` to `.env` manually and update values as needed.

> **Important:** `.env` contains credentials and should stay local. 

<!-- RUNNING THE PROJECT -->
## Running The Project

### run project
After completing all the prerequisites and installation steps, you can start the entire environment using Docker Compose.

The following command will build (if needed) and start all required services, including Kafka, Spark, Airflow, MinIO, Iceberg, and the supporting infrastructure.

```sh
docker compose up -d
```
airflow - may take some time to get up youll need to wait about 5min or so...

Once all services are up and running, start the Kafka producer to simulate real-time Airbnb review events:

<!-- KAFKA PRODUCER -->
### Kafka Simulate Producer

```sh
uv run -m streaming.stream_reviews_airflow_producer
```

The producer continuously generates review events and publishes them to Kafka. These events are then consumed by the Spark Streaming application, processed through the Bronze, Silver, and Gold layers, and orchestrated by Airflow.
