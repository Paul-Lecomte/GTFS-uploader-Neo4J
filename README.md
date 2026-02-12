<!-- PROJECT TITLE & BADGES -->

<h1 align="center">GTFS-uploader-Neo4J</h1>

<p align="center">
  <strong>Very fast GTFS data uploader for Neo4J</strong><br>
  <a href="#">
    <img alt="GitHub stars" src="https://img.shields.io/github/stars/your-username/GTFS-uploader-Neo4J?style=social">
  </a>
  <img alt="Tech Stack" src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white">
  <img alt="Tech Stack" src="https://img.shields.io/badge/Neo4j-CC0033?logo=neo4j&logoColor=white">
  <img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-green.svg">
</p>

---

## What is GTFS-uploader-Neo4J?

**GTFS-uploader-Neo4J** is a high-performance utility to import GTFS datasets into Neo4j graph databases. It was built to handle large feeds quickly and efficiently — capable of importing millions of nodes in a short time when tuned and run on suitable hardware.


---

## Features

- Fast, streamed ingestion of GTFS CSV files into Neo4j
- Efficient batching and transaction management for large datasets
- Support for common GTFS files (stops, routes, trips, stop_times, shapes, transfers, calendar, calendar_dates)
- Configurable mapping and simple CLI usage
- Minimal dependencies and easy to customize

---

## Service calendars

If your GTFS includes `calendar.txt` and/or `calendar_dates.txt`, the importer creates `Service` and `CalendarDate` nodes and links `Trip` to `Service` via `[:HAS_SERVICE]`. This makes it easy to query trips active on a specific date.

Example Cypher (active trips for a given date):

```cypher
WITH date($date) AS d, toString(date($date)) AS ds
MATCH (s:Service)
OPTIONAL MATCH (s)-[m:MODIFIED_ON]->(:CalendarDate {date: ds})
WITH s, d, ds, m,
CASE d.dayOfWeek
  WHEN 1 THEN s.monday
  WHEN 2 THEN s.tuesday
  WHEN 3 THEN s.wednesday
  WHEN 4 THEN s.thursday
  WHEN 5 THEN s.friday
  WHEN 6 THEN s.saturday
  WHEN 7 THEN s.sunday
END AS weekly
WITH s, m, weekly,
CASE
  WHEN m.type = 1 THEN true
  WHEN m.type = 2 THEN false
  ELSE weekly
END AS active
WHERE active = true
  AND (s.start_date IS NULL OR s.start_date <= ds)
  AND (s.end_date IS NULL OR s.end_date >= ds)
MATCH (t:Trip)-[:HAS_SERVICE]->(s)
RETURN t;
```

---

## Tech Stack

- Framework / Language: Python
- Database: Neo4j (Bolt protocol)
- Dependencies: see `requirements.txt`

---

## Project Structure

```bash
GTFS-uploader-Neo4J/
├── uploader.py              # Main importer script
├── requirements.txt         # Python dependencies
├── README.md                # Project overview & instructions
└── LICENSE                  # Project license (MIT)
```

---

## Getting Started

### Clone the repo

```bash
git clone https://github.com/your-username/GTFS-uploader-Neo4J.git
cd GTFS-uploader-Neo4J
```

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

Adjust paths and settings as needed.

### Run the importer

Basic usage:

```bash
python uploader.py C:\gtfs.zip username password bolt://127.0.0.1:7687
```

Replace arguments with the GTFS file or folder you want to import. See `uploader.py --help` for available options.

---

## Usage

1. Place your GTFS `.zip` or folder containing GTFS CSV files locally.
2. Configure your Neo4j credentials in environment variables or a `.env` file.
3. Run the script and monitor progress. Logs will show batch sizes, transaction times, and errors if any.

---

## Customization

- Edit `uploader.py` to change mapping, batching strategy or to add support for additional GTFS fields.
- Add pre-processing steps to normalize or filter GTFS CSVs before import.

---

## Roadmap

- [x] Core importer for standard GTFS files
- [x] Batching and transaction control
- [ ] Add support for incremental updates
- [ ] Add unit/integration tests
- [ ] Provide Docker image and Helm chart for easy deployment

---

## Acknowledgements

Built for speed and scale. Thanks to the Neo4j community for tooling inspiration and to transit data contributors worldwide.

---

## License

This project is licensed under the MIT License — see the `LICENSE` file for details.

© 2026 Paul Lecomte
