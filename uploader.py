"""
Modernized GTFS -> Neo4j uploader

This version uses the official `neo4j` Python driver (recommended for up-to-date Neo4j Desktop / Server)
and modern Cypher (CREATE CONSTRAINT IF NOT EXISTS ...). It also fixes CSV parsing, type handling,
and several bugs from the original script (double iteration over CSV, relationship names with spaces,
property name typos, unsafe string concatenation when building Cypher queries, ...).

Usage (example):
    python uploader.py path/to/gtfs.zip username password bolt://localhost:7687

Requirements: add `neo4j` to your environment (pip install neo4j)
"""

# Resilient dynamic import: if neo4j driver isn't installed, provide a clear error at runtime.
import importlib
try:
    _neo4j_mod = importlib.import_module('neo4j')
    GraphDatabase = getattr(_neo4j_mod, 'GraphDatabase')
    _NEO4J_AVAILABLE = True
except Exception:
    GraphDatabase = None  # type: ignore
    _NEO4J_AVAILABLE = False

from time import time

import argparse
import zipfile
import tempfile
import os
import shutil
import csv
from typing import Dict, Any, Optional, Iterable, List, Tuple


class GNUploader(object):
    gtfs_file_extension = ".txt"

    # modern Cypher for constraints (IF NOT EXISTS to be idempotent)
    trip_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Trip) REQUIRE (t.trip_id) IS UNIQUE"
    route_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Route) REQUIRE (r.route_id) IS UNIQUE"
    agency_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Agency) REQUIRE (a.agency_id) IS UNIQUE"
    stop_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stop) REQUIRE (s.stop_id) IS UNIQUE"
    stop_times_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (st:Stop_times) REQUIRE ((st.trip_id, st.stop_sequence)) IS NODE KEY"
    service_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE (s.service_id) IS UNIQUE"
    calendar_date_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (d:CalendarDate) REQUIRE (d.date) IS UNIQUE"
    trip_service_index_query = "CREATE INDEX IF NOT EXISTS FOR (t:Trip) ON (t.service_id)"

    # Fallback if composite node key isn't available: at least index the pairing
    stop_times_pair_index_query = "CREATE INDEX IF NOT EXISTS FOR (st:Stop_times) ON (st.trip_id, st.stop_sequence)"

    # We no longer use the global PRECEDES query by default (too expensive at scale).
    # Kept as a fallback (small datasets only).
    connect_stop_sequences_query = (
        "MATCH (t:Trip)\n"
        "MATCH (t)<-[:PART_OF_TRIP]-(s1:Stop_times)\n"
        "MATCH (t)<-[:PART_OF_TRIP]-(s2:Stop_times)\n"
        "WHERE s2.stop_sequence = s1.stop_sequence + 1\n"
        "MERGE (s1)-[:PRECEDES]->(s2)"
    )

    def __init__(self,
                 gtfs_zip_path: str,
                 username: str,
                 password: str,
                 neo4j_service_uri: str,
                 csv_delim: str = ",",
                 batch_size: int = 10000,
                 stop_times_batch_size: int = 5000,
                 fast_create: bool = False,
                 build_precedes: bool = True):
        if not _NEO4J_AVAILABLE:
            raise SystemExit("The 'neo4j' Python driver is required. Install it with: pip install neo4j")

        self.username = username
        self.password = password
        self.neo4j_service_uri = neo4j_service_uri
        self.csv_delim = csv_delim
        self.time_format = "%H:%M:%S"

        # perf knobs
        self.batch_size = max(1, int(batch_size))
        self.stop_times_batch_size = max(1, int(stop_times_batch_size))

        # If True, prefer CREATE over MERGE (expecting an empty DB + constraints for uniqueness).
        # This is much faster for multi-million rows, but re-running on the same DB will fail on duplicates.
        self.fast_create = bool(fast_create)

        # If True, build PRECEDES relationships (streaming from file, not DB-wide scans).
        self.build_precedes = bool(build_precedes)

        # metrics (MERGE makes exact 'created' counting expensive; these are rows processed)
        self.node_ctr = 0
        self.relationship_ctr = 0
        self.rows_skipped = 0

        # load input files
        self.gtfs_zip_path = gtfs_zip_path
        self.gtfs_tmp_path = tempfile.mkdtemp()
        self.__extract_zip()
        self.stops = os.path.join(self.gtfs_tmp_path, "stops" + self.gtfs_file_extension)
        self.routes = os.path.join(self.gtfs_tmp_path, "routes" + self.gtfs_file_extension)
        self.stop_times = os.path.join(self.gtfs_tmp_path, "stop_times" + self.gtfs_file_extension)
        self.trips = os.path.join(self.gtfs_tmp_path, "trips" + self.gtfs_file_extension)
        self.agencies = os.path.join(self.gtfs_tmp_path, "agency" + self.gtfs_file_extension)
        self.calendar = os.path.join(self.gtfs_tmp_path, "calendar" + self.gtfs_file_extension)
        self.calendar_dates = os.path.join(self.gtfs_tmp_path, "calendar_dates" + self.gtfs_file_extension)
        self.has_calendar = os.path.isfile(self.calendar)
        self.has_calendar_dates = os.path.isfile(self.calendar_dates)
        self.__validate_gtfs_files_in_dir()
        # connect to neo4j service
        self.driver = self.__connect_to_neo4j()

    def __del__(self):
        try:
            shutil.rmtree(self.gtfs_tmp_path)
        except Exception:
            pass
        try:
            if hasattr(self, 'driver') and self.driver is not None:
                self.driver.close()
        except Exception:
            pass

    def __connect_to_neo4j(self):
        try:
            driver = GraphDatabase.driver(self.neo4j_service_uri, auth=(self.username, self.password))
            # quick test connection
            with driver.session() as session:
                session.run("RETURN 1")
            print(f"Successfully connected to {self.neo4j_service_uri} as {self.username}")
            return driver
        except Exception as e:
            raise SystemExit(f"Could not connect to the neo4j database on {self.neo4j_service_uri} - error: {e}")

    def __extract_zip(self) -> str:
        try:
            with zipfile.ZipFile(self.gtfs_zip_path, 'r') as zip_archive:
                zip_archive.extractall(self.gtfs_tmp_path)
                print(f"Files extracted into {self.gtfs_tmp_path} successfully!")
            return self.gtfs_tmp_path
        except Exception as e:
            raise SystemExit(f"ERROR: Could not extract archive {self.gtfs_zip_path}. Error: {e}")

    def __validate_gtfs_files_in_dir(self):
        if not os.path.isfile(self.stop_times):
            raise SystemExit("Could not find stop_times.txt file. Aborting")
        if not os.path.isfile(self.stops):
            raise SystemExit("Could not find stops.txt file. Aborting")
        if not os.path.isfile(self.trips):
            raise SystemExit("Could not find trips.txt file. Aborting")
        if not os.path.isfile(self.routes):
            raise SystemExit("Could not find routes.txt file. Aborting")
        if not os.path.isfile(self.agencies):
            raise SystemExit("Could not find agency.txt file. Aborting")

        if not self.has_calendar and not self.has_calendar_dates:
            print("Warning: calendar.txt and calendar_dates.txt are missing; no service availability will be imported.")
        elif not self.has_calendar:
            print("Warning: calendar.txt is missing; only calendar_dates.txt exceptions will be imported.")
        elif not self.has_calendar_dates:
            print("Warning: calendar_dates.txt is missing; only weekly calendar will be imported.")

    def execute(self) -> None:
        start_time = time()

        # create metadata
        self.__create_constraints_and_indexes()
        print("Indexes and constraints created.")
        # main loading sequence
        self.__import_agencies()
        print(f"Agencies imported.")
        self.__import_routes()
        print(f"Routes imported.")
        self.__import_trips()
        print(f"Trips imported.")
        self.__import_stops()
        print(f"Stops imported.")

        if self.has_calendar:
            self.__import_calendar()
            print("Calendar imported.")
        if self.has_calendar_dates:
            self.__import_calendar_dates()
            print("Calendar dates imported.")
        if self.has_calendar or self.has_calendar_dates:
            self.__link_trips_to_services()
            print("Trips linked to services.")

        self.__import_stop_times()
        print(f"Stop_times imported.")

        if self.build_precedes:
            self.__connect_stop_times_sequences()
            print("Connected stop_times sequences.")

        end_time = time()
        runtime_seconds = end_time - start_time
        runtime_text = f"{runtime_seconds} seconds" if runtime_seconds > 60 else f"{runtime_seconds} seconds"
        print(
            f"Import complete, took {runtime_text} for {self.node_ctr} nodes and {self.relationship_ctr} edges imported.")

    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        # Keep a single place to run queries; for performance-critical paths we use execute_write in batch methods.
        with self.driver.session() as session:
            return session.run(query, params)

    def _normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CSV rows: remove BOM from keys and convert empty strings to None."""
        cleaned: Dict[str, Any] = {}
        for k, v in row.items():
            if k:
                k = k.replace('\ufeff', '')
            if isinstance(v, str):
                v = v.strip()
                if v == "":
                    v = None
            cleaned[k] = v
        return cleaned

    def _parse_yyyymmdd(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        if len(value) != 8 or not value.isdigit():
            return value
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"

    def _iter_batches(self, iterable: Iterable[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
        batch: List[Dict[str, Any]] = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def __create_constraints_and_indexes(self) -> None:
        print("Creating constraints and indexes..")
        try:
            self.run_query(self.trip_constraint_query)
            self.run_query(self.route_constraint_query)
            self.run_query(self.agency_constraint_query)
            self.run_query(self.stop_constraint_query)
            self.run_query(self.service_constraint_query)
            self.run_query(self.calendar_date_constraint_query)
            self.run_query(self.trip_service_index_query)
            # optional: create a node key for composite uniqueness for stop_times (trip_id + stop_sequence)
            try:
                self.run_query(self.stop_times_constraint_query)
            except Exception:
                # fallback: at least an index to make lookups cheap
                try:
                    self.run_query(self.stop_times_pair_index_query)
                except Exception:
                    pass
        except Exception as e:
            print(f"Constraint/index creation error (continuing): {e}")

    def __import_agencies(self) -> None:
        # Fast mode: CREATE (expects empty DB); otherwise MERGE.
        if self.fast_create:
            query = (
                "UNWIND $rows AS row\n"
                "CREATE (a:Agency {agency_id: row.agency_id})\n"
                "SET a += row.props\n"
            )
        else:
            query = (
                "UNWIND $rows AS row\n"
                "MERGE (a:Agency {agency_id: row.agency_id})\n"
                "SET a += row.props\n"
            )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.agencies, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    agency_id = row.get('agency_id') or row.get('agency_key') or row.get('agencyId')
                    if not agency_id:
                        self.rows_skipped += 1
                        continue

                    props = {
                        'agency_id': agency_id,
                        'name': row.get('agency_name'),
                        'url': row.get('agency_url'),
                        'timezone': row.get('agency_timezone'),
                        'agency_lang': row.get('agency_lang'),
                        'agency_phone': row.get('agency_phone'),
                    }
                    props = {k: v for k, v in props.items() if v is not None}

                    yield {'agency_id': agency_id, 'props': props}

        with self.driver.session() as session:
            for batch in self._iter_batches(_rows(), self.batch_size):
                session.execute_write(lambda tx, rows: tx.run(query, rows=rows), batch)
                self.node_ctr += len(batch)

    def __import_routes(self) -> None:
        if self.fast_create:
            query_routes = (
                "UNWIND $rows AS row\n"
                "CREATE (r:Route {route_id: row.route_id})\n"
                "SET r += row.props\n"
            )
        else:
            query_routes = (
                "UNWIND $rows AS row\n"
                "MERGE (r:Route {route_id: row.route_id})\n"
                "SET r += row.props\n"
            )

        # In fast mode with an empty DB, relationships can be CREATE; otherwise use MERGE.
        rel_keyword = "CREATE" if self.fast_create else "MERGE"
        query_operates = (
            "UNWIND $rows AS row\n"
            "MATCH (a:Agency {agency_id: row.agency_id})\n"
            "MATCH (r:Route {route_id: row.route_id})\n"
            f"{rel_keyword} (a)-[:OPERATES]->(r)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.routes, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    route_id = row.get('route_id')
                    if not route_id:
                        self.rows_skipped += 1
                        continue

                    route_type = row.get('route_type')
                    route_type_val = int(route_type) if (route_type is not None and str(route_type).isdigit()) else None

                    props = {
                        'route_id': route_id,
                        'short_name': row.get('route_short_name'),
                        'long_name': row.get('route_long_name'),
                        'type': route_type_val,
                        'agency_id': row.get('agency_id'),
                    }
                    props = {k: v for k, v in props.items() if v is not None}

                    yield {
                        'route_id': route_id,
                        'agency_id': row.get('agency_id'),
                        'props': props,
                    }

        with self.driver.session() as session:
            operates_batch: List[Dict[str, Any]] = []
            for batch in self._iter_batches(_rows(), self.batch_size):
                session.execute_write(lambda tx, rows: tx.run(query_routes, rows=rows), batch)
                self.node_ctr += len(batch)

                # collect operate relationships for this batch
                for r in batch:
                    if r.get('agency_id') is not None:
                        operates_batch.append({'agency_id': r['agency_id'], 'route_id': r['route_id']})

                if operates_batch:
                    session.execute_write(lambda tx, rows: tx.run(query_operates, rows=rows), operates_batch)
                    self.relationship_ctr += len(operates_batch)
                    operates_batch = []

    def __import_stops(self) -> None:
        """Batch import stops, including optional parent_station relation when present."""

        if self.fast_create:
            query_stops = (
                "UNWIND $rows AS row\n"
                "CREATE (s:Stop {stop_id: row.stop_id})\n"
                "SET s += row.props\n"
            )
        else:
            query_stops = (
                "UNWIND $rows AS row\n"
                "MERGE (s:Stop {stop_id: row.stop_id})\n"
                "SET s += row.props\n"
            )

        parent_rel_keyword = "CREATE" if self.fast_create else "MERGE"
        query_parent = (
            "UNWIND $rows AS row\n"
            "MATCH (child:Stop {stop_id: row.stop_id})\n"
            "MATCH (parent:Stop {stop_id: row.parent_station})\n"
            f"{parent_rel_keyword} (child)-[:PART_OF]->(parent)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.stops, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    stop_id = row.get('stop_id')
                    if not stop_id:
                        self.rows_skipped += 1
                        continue

                    # lat/lon can be empty
                    lat = row.get('stop_lat')
                    lon = row.get('stop_lon')
                    try:
                        lat_val = float(lat) if lat is not None else None
                    except Exception:
                        lat_val = None
                    try:
                        lon_val = float(lon) if lon is not None else None
                    except Exception:
                        lon_val = None

                    location_type = row.get('location_type')
                    location_type_val = int(location_type) if (location_type is not None and str(location_type).isdigit()) else None

                    props = {
                        'stop_id': stop_id,
                        'name': row.get('stop_name'),
                        'desc': row.get('stop_desc'),
                        'lat': lat_val,
                        'lon': lon_val,
                        'zone_id': row.get('zone_id'),
                        'url': row.get('stop_url'),
                        'location_type': location_type_val,
                        'parent_station': row.get('parent_station'),
                        'platform_code': row.get('platform_code'),
                        'wheelchair_boarding': row.get('wheelchair_boarding'),
                    }
                    props = {k: v for k, v in props.items() if v is not None}

                    yield {
                        'stop_id': stop_id,
                        'parent_station': row.get('parent_station'),
                        'props': props,
                    }

        with self.driver.session() as session:
            parent_rows: List[Dict[str, Any]] = []
            for batch in self._iter_batches(_rows(), self.batch_size):
                session.execute_write(lambda tx, rows: tx.run(query_stops, rows=rows), batch)
                self.node_ctr += len(batch)

                for r in batch:
                    if r.get('parent_station') is not None:
                        parent_rows.append({'stop_id': r['stop_id'], 'parent_station': r['parent_station']})

                if parent_rows:
                    session.execute_write(lambda tx, rows: tx.run(query_parent, rows=rows), parent_rows)
                    self.relationship_ctr += len(parent_rows)
                    parent_rows = []

    def __import_trips(self) -> None:
        """Batch import trips (designed for 1M+ rows).

        In fast mode, we CREATE nodes (DB must be empty + unique constraint on Trip.trip_id).
        For relationships, we keep the 2-pass approach: nodes then relationships.
        """

        if self.fast_create:
            query_trips = (
                "UNWIND $rows AS row\n"
                "CREATE (t:Trip {trip_id: row.trip_id})\n"
                "SET t.route_id = row.route_id\n"
                "SET t.service_id = row.service_id\n"
                "SET t.trip_headsign = row.trip_headsign\n"
                "SET t.wheelchair_accessible = row.wheelchair_accessible\n"
                "SET t.block_id = row.block_id\n"
                "SET t.direction_id = row.direction_id\n"
                "SET t.exceptional = row.exceptional\n"
            )
            rel_keyword = "CREATE"
        else:
            query_trips = (
                "UNWIND $rows AS row\n"
                "MERGE (t:Trip {trip_id: row.trip_id})\n"
                "SET t.route_id = row.route_id\n"
                "SET t.service_id = row.service_id\n"
                "SET t.trip_headsign = row.trip_headsign\n"
                "SET t.wheelchair_accessible = row.wheelchair_accessible\n"
                "SET t.block_id = row.block_id\n"
                "SET t.direction_id = row.direction_id\n"
                "SET t.exceptional = row.exceptional\n"
            )
            rel_keyword = "MERGE"

        query_uses = (
            "UNWIND $rows AS row\n"
            "MATCH (r:Route {route_id: row.route_id})\n"
            "MATCH (t:Trip {trip_id: row.trip_id})\n"
            f"{rel_keyword} (r)-[:USES]->(t)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.trips, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    trip_id = row.get('trip_id')
                    if not trip_id:
                        self.rows_skipped += 1
                        continue

                    direction_id = row.get('direction_id')
                    direction_val = int(direction_id) if (direction_id is not None and str(direction_id).isdigit()) else None

                    yield {
                        'trip_id': trip_id,
                        'route_id': row.get('route_id'),
                        'service_id': row.get('service_id'),
                        'trip_headsign': row.get('trip_headsign'),
                        'wheelchair_accessible': row.get('wheelchair_accessible'),
                        'block_id': row.get('block_id'),
                        'direction_id': direction_val,
                        'exceptional': row.get('exceptional'),
                    }

        with self.driver.session() as session:
            # pass 1: write Trip nodes
            batch_num = 0
            rows_written = 0
            for batch in self._iter_batches(_rows(), self.batch_size):
                batch_num += 1
                rows_written += len(batch)
                session.execute_write(lambda tx, rows: tx.run(query_trips, rows=rows), batch)
                self.node_ctr += len(batch)

                if batch_num % 50 == 0:
                    print(f"Trips: {rows_written:,} rows processed, skipped={self.rows_skipped:,}")

            # pass 2: create relationships Route->Trip
            # We need to iterate again over the file; this is deliberately cheaper than doing MATCH Route during writes.
            def _uses_rows() -> Iterable[Dict[str, Any]]:
                with open(self.trips, 'r', newline='', encoding='utf8') as fh:
                    reader = csv.DictReader(fh, delimiter=self.csv_delim)
                    for raw in reader:
                        row = self._normalize_row(raw)
                        trip_id = row.get('trip_id')
                        route_id = row.get('route_id')
                        if not trip_id or not route_id:
                            continue
                        yield {'trip_id': trip_id, 'route_id': route_id}

            rel_batch_num = 0
            rel_written = 0
            for rel_batch in self._iter_batches(_uses_rows(), self.batch_size):
                rel_batch_num += 1
                rel_written += len(rel_batch)
                session.execute_write(lambda tx, rows: tx.run(query_uses, rows=rows), rel_batch)
                self.relationship_ctr += len(rel_batch)

                if rel_batch_num % 50 == 0:
                    print(f"Trip relations: {rel_written:,} rows processed")

    def __import_stop_times(self) -> None:
        """Batch import stop_times and create relationships to Trip and Stop."""

        if self.fast_create:
            query_nodes = (
                "UNWIND $rows AS row\n"
                "CREATE (st:Stop_times {trip_id: row.trip_id, stop_sequence: row.stop_sequence})\n"
                "SET st += row.props\n"
            )
        else:
            query_nodes = (
                "UNWIND $rows AS row\n"
                "MERGE (st:Stop_times {trip_id: row.trip_id, stop_sequence: row.stop_sequence})\n"
                "SET st += row.props\n"
            )

        rel_keyword = "CREATE" if self.fast_create else "MERGE"
        query_rels = (
            "UNWIND $rows AS row\n"
            "MATCH (t:Trip {trip_id: row.trip_id})\n"
            "MATCH (s:Stop {stop_id: row.stop_id})\n"
            "MATCH (st:Stop_times {trip_id: row.trip_id, stop_sequence: row.stop_sequence})\n"
            f"{rel_keyword} (st)-[:PART_OF_TRIP]->(t)\n"
            f"{rel_keyword} (st)-[:AT_STOP]->(s)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.stop_times, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    trip_id = row.get('trip_id')
                    stop_id = row.get('stop_id')
                    stop_sequence = row.get('stop_sequence')
                    if not trip_id or not stop_id or stop_sequence is None:
                        self.rows_skipped += 1
                        continue

                    try:
                        stop_sequence_val = int(stop_sequence)
                    except Exception:
                        self.rows_skipped += 1
                        continue

                    def _to_int(v: Optional[str]) -> Optional[int]:
                        if v is None:
                            return None
                        return int(v) if str(v).isdigit() else None

                    def _to_float(v: Optional[str]) -> Optional[float]:
                        if v is None:
                            return None
                        try:
                            return float(v)
                        except Exception:
                            return None

                    props = {
                        'trip_id': trip_id,
                        'stop_id': stop_id,
                        'stop_sequence': stop_sequence_val,
                        'arrival_time': row.get('arrival_time'),
                        'departure_time': row.get('departure_time'),
                        'stop_headsign': row.get('stop_headsign'),
                        'pickup_type': _to_int(row.get('pickup_type')),
                        'drop_off_type': _to_int(row.get('drop_off_type')),
                        'shape_dist_traveled': _to_float(row.get('shape_dist_traveled')),
                        'timepoint': _to_int(row.get('timepoint')),
                    }
                    props = {k: v for k, v in props.items() if v is not None}

                    yield {
                        'trip_id': trip_id,
                        'stop_id': stop_id,
                        'stop_sequence': stop_sequence_val,
                        'props': props,
                    }

        with self.driver.session() as session:
            # pass 1: nodes
            batch_num = 0
            rows_written = 0
            for batch in self._iter_batches(_rows(), self.stop_times_batch_size):
                batch_num += 1
                rows_written += len(batch)
                session.execute_write(lambda tx, rows: tx.run(query_nodes, rows=rows), batch)
                self.node_ctr += len(batch)

                if batch_num % 50 == 0:
                    print(f"Stop_times: {rows_written:,} rows processed, skipped={self.rows_skipped:,}")

            # pass 2: relationships
            def _rel_rows() -> Iterable[Dict[str, Any]]:
                with open(self.stop_times, 'r', newline='', encoding='utf8') as fh:
                    reader = csv.DictReader(fh, delimiter=self.csv_delim)
                    for raw in reader:
                        row = self._normalize_row(raw)
                        trip_id = row.get('trip_id')
                        stop_id = row.get('stop_id')
                        stop_sequence = row.get('stop_sequence')
                        if not trip_id or not stop_id or stop_sequence is None:
                            continue
                        try:
                            stop_sequence_val = int(stop_sequence)
                        except Exception:
                            continue
                        yield {
                            'trip_id': trip_id,
                            'stop_id': stop_id,
                            'stop_sequence': stop_sequence_val,
                        }

            rel_batch_num = 0
            rel_written = 0
            for rel_batch in self._iter_batches(_rel_rows(), self.stop_times_batch_size):
                rel_batch_num += 1
                rel_written += len(rel_batch)
                session.execute_write(lambda tx, rows: tx.run(query_rels, rows=rows), rel_batch)
                self.relationship_ctr += (len(rel_batch) * 2)

                if rel_batch_num % 50 == 0:
                    print(f"Stop_times relations: {rel_written:,} rows processed")

    def __connect_stop_times_sequences(self) -> None:
        """Create PRECEDES edges by streaming stop_times in file order (assumes sorted by trip_id, stop_sequence)."""

        query = (
            "UNWIND $rows AS row\n"
            "MATCH (s1:Stop_times {trip_id: row.trip_id, stop_sequence: row.stop_sequence})\n"
            "MATCH (s2:Stop_times {trip_id: row.trip_id, stop_sequence: row.next_stop_sequence})\n"
            "MERGE (s1)-[:PRECEDES]->(s2)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            last_by_trip: Dict[str, int] = {}
            with open(self.stop_times, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    trip_id = row.get('trip_id')
                    stop_sequence = row.get('stop_sequence')
                    if not trip_id or stop_sequence is None:
                        continue
                    try:
                        stop_sequence_val = int(stop_sequence)
                    except Exception:
                        continue

                    if trip_id in last_by_trip:
                        yield {
                            'trip_id': trip_id,
                            'stop_sequence': last_by_trip[trip_id],
                            'next_stop_sequence': stop_sequence_val,
                        }
                    last_by_trip[trip_id] = stop_sequence_val

        with self.driver.session() as session:
            rel_batch_num = 0
            rel_written = 0
            for rel_batch in self._iter_batches(_rows(), self.stop_times_batch_size):
                rel_batch_num += 1
                rel_written += len(rel_batch)
                session.execute_write(lambda tx, rows: tx.run(query, rows=rows), rel_batch)
                self.relationship_ctr += len(rel_batch)

                if rel_batch_num % 50 == 0:
                    print(f"PRECEDES: {rel_written:,} rows processed")

    def __import_calendar(self) -> None:
        if self.fast_create:
            query = (
                "UNWIND $rows AS row\n"
                "CREATE (s:Service {service_id: row.service_id})\n"
                "SET s += row.props\n"
            )
        else:
            query = (
                "UNWIND $rows AS row\n"
                "MERGE (s:Service {service_id: row.service_id})\n"
                "SET s += row.props\n"
            )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.calendar, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    service_id = row.get('service_id')
                    if not service_id:
                        self.rows_skipped += 1
                        continue

                    def _to_bool(v: Optional[str]) -> Optional[bool]:
                        if v is None:
                            return None
                        if str(v).isdigit():
                            return bool(int(v))
                        return None

                    props = {
                        'service_id': service_id,
                        'monday': _to_bool(row.get('monday')),
                        'tuesday': _to_bool(row.get('tuesday')),
                        'wednesday': _to_bool(row.get('wednesday')),
                        'thursday': _to_bool(row.get('thursday')),
                        'friday': _to_bool(row.get('friday')),
                        'saturday': _to_bool(row.get('saturday')),
                        'sunday': _to_bool(row.get('sunday')),
                        'start_date': self._parse_yyyymmdd(row.get('start_date')),
                        'end_date': self._parse_yyyymmdd(row.get('end_date')),
                    }
                    props = {k: v for k, v in props.items() if v is not None}

                    yield {'service_id': service_id, 'props': props}

        with self.driver.session() as session:
            for batch in self._iter_batches(_rows(), self.batch_size):
                session.execute_write(lambda tx, rows: tx.run(query, rows=rows), batch)
                self.node_ctr += len(batch)

    def __import_calendar_dates(self) -> None:
        query = (
            "UNWIND $rows AS row\n"
            "MERGE (s:Service {service_id: row.service_id})\n"
            "MERGE (d:CalendarDate {date: row.date})\n"
            "MERGE (s)-[:MODIFIED_ON {type: row.exception_type}]->(d)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.calendar_dates, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    service_id = row.get('service_id')
                    date = self._parse_yyyymmdd(row.get('date'))
                    exception_type = row.get('exception_type')
                    if not service_id or not date or exception_type is None:
                        self.rows_skipped += 1
                        continue
                    try:
                        exception_val = int(exception_type)
                    except Exception:
                        self.rows_skipped += 1
                        continue

                    yield {
                        'service_id': service_id,
                        'date': date,
                        'exception_type': exception_val,
                    }

        with self.driver.session() as session:
            for batch in self._iter_batches(_rows(), self.batch_size):
                session.execute_write(lambda tx, rows: tx.run(query, rows=rows), batch)
                self.relationship_ctr += len(batch)

    def __link_trips_to_services(self) -> None:
        rel_keyword = "CREATE" if self.fast_create else "MERGE"
        query = (
            "UNWIND $rows AS row\n"
            "MATCH (t:Trip {trip_id: row.trip_id})\n"
            "MATCH (s:Service {service_id: row.service_id})\n"
            f"{rel_keyword} (t)-[:HAS_SERVICE]->(s)\n"
        )

        def _rows() -> Iterable[Dict[str, Any]]:
            with open(self.trips, 'r', newline='', encoding='utf8') as fh:
                reader = csv.DictReader(fh, delimiter=self.csv_delim)
                for raw in reader:
                    row = self._normalize_row(raw)
                    trip_id = row.get('trip_id')
                    service_id = row.get('service_id')
                    if not trip_id or not service_id:
                        continue
                    yield {'trip_id': trip_id, 'service_id': service_id}

        with self.driver.session() as session:
            rel_batch_num = 0
            rel_written = 0
            for rel_batch in self._iter_batches(_rows(), self.batch_size):
                rel_batch_num += 1
                rel_written += len(rel_batch)
                session.execute_write(lambda tx, rows: tx.run(query, rows=rows), rel_batch)
                self.relationship_ctr += len(rel_batch)

                if rel_batch_num % 50 == 0:
                    print(f"Trip services: {rel_written:,} rows processed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GTFS NEO4J Uploader")
    parser.add_argument("gtfs_zip_path", type=str, help="Path to the GTFS .ZIP file")
    parser.add_argument("username", type=str, help="Username for the Neo4j database")
    parser.add_argument("password", type=str, help="Password for the Neo4j database")
    parser.add_argument("neo4j_service_uri", type=str, help="URI of the Neo4j database (bolt://host:7687)")
    parser.add_argument("--csv_delim", type=str, default=",", help="CSV delimiter for the GTFS files (default is ',')")
    parser.add_argument("--batch_size", type=int, default=10000, help="Batch size for most imports (default 10000)")
    parser.add_argument("--stop_times_batch_size", type=int, default=5000, help="Batch size for stop_times import (default 5000)")
    parser.add_argument("--fast_create", action="store_true", help="Use CREATE instead of MERGE (expects empty DB; fastest)")
    parser.add_argument("--no_precedes", action="store_true", help="Skip building PRECEDES relationships (saves a lot of time)")

    args = parser.parse_args()

    uploader = GNUploader(
        args.gtfs_zip_path,
        args.username,
        args.password,
        args.neo4j_service_uri,
        args.csv_delim,
        batch_size=args.batch_size,
        stop_times_batch_size=args.stop_times_batch_size,
        fast_create=args.fast_create,
        build_precedes=(not args.no_precedes),
    )

    uploader.execute()
