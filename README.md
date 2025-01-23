# Countries by GDP ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline to extract and process data on countries' GDP from a public data source. The goal is to make the data accessible in multiple formats for analysis and reporting.

## Project Overview

An international firm, aiming to expand its operations worldwide, requires an automated pipeline to extract and process GDP data of countries. This script fetches the latest GDP data, processes it, and stores it in JSON and SQLite database formats for further use.

### Features
- **Automated Extraction**: Fetches the latest GDP data from a specified web source.
- **Transformation**: Processes and rounds GDP data to 2 decimal places.
- **Loading**: 
  - Saves data in JSON format (`Countries_by_GDP.json`).
  - Creates a database (`World_Economies.db`) with a table `Countries_by_GDP` containing:
    - `Country`
    - `GDP_USD_billion`
- **Query Execution**: Runs a query on the database to filter and display countries with a GDP exceeding 100 billion USD.
- **Logging**: Logs all steps of the ETL process in `etl_project_log.txt`.

## Getting Started

### Prerequisites
To run this project, you need:
- Python 3.x
- Required libraries:
  - `requests`
  - `sqlite3`
  - `json`
  - Any additional dependencies can be installed via `requirements.txt`.

### Installation
1. Clone this repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
