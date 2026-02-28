import json
import sqlite3
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))

from analyze_cycling import analyze_workout as analyze_cycling
from analyze_strength import analyze_workout as analyze_strength
from analyze_swimming import analyze_workout as analyze_swimming


CONFIG_FILE_NAME = "file_config.json"
SYNC_DB_FILE_NAME = "workout_sync.db"


def load_config() -> dict:
	config_file = Path(CONFIG_FILE_NAME)

	if not config_file.exists():
		config_file = Path(__file__).resolve().parent / CONFIG_FILE_NAME

	if not config_file.exists():
		raise FileNotFoundError(
			"Could not find file_config.json. Expected in current folder or huawei_sync."
		)

	with config_file.open("r", encoding="utf-8") as config_handle:
		return json.load(config_handle)


def detect_workout_type(workout_dir: Path) -> Optional[str]:
	swim_segments = workout_dir / "HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE.csv"
	if swim_segments.exists():
		return "swimming"

	gpx_matches = list(workout_dir.glob("workout_*.gpx"))
	if gpx_matches:
		return "cycling"

	has_summary = (workout_dir / "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv").exists()
	has_data = (workout_dir / "HUAWEI_WORKOUT_DATA_SAMPLE.csv").exists()
	if has_summary and has_data:
		return "strength"

	return None


def resolve_sync_db_path(sync_db_location: str) -> Path:
	path = Path(sync_db_location)
	if path.suffix.lower() == ".db":
		path.parent.mkdir(parents=True, exist_ok=True)
		return path

	path.mkdir(parents=True, exist_ok=True)
	return path / SYNC_DB_FILE_NAME


def init_sync_db(connection: sqlite3.Connection) -> None:
	connection.execute(
		"""
		CREATE TABLE IF NOT EXISTS workouts (
			workout_id INTEGER PRIMARY KEY,
			workout_number INTEGER,
			workout_type TEXT NOT NULL,
			workout_date TEXT,
			duration_seconds REAL,
			total_distance_m REAL,
			total_calories INTEGER,
			has_gps INTEGER NOT NULL DEFAULT 0,
			source_workout_dir TEXT NOT NULL,
			fit_file_path TEXT,
			fit_generated_at TEXT,
			last_analyzed_at TEXT NOT NULL,
			strava_synced INTEGER NOT NULL DEFAULT 0,
			strava_activity_id INTEGER,
			strava_activity_url TEXT,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
		"""
	)
	connection.commit()


def load_summary_row(workout_dir: Path, workout_id: int) -> dict:
	summary_path = workout_dir / "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv"
	if not summary_path.exists():
		return {}

	df_summary = pd.read_csv(summary_path)
	if df_summary.empty:
		return {}

	if "WORKOUT_ID" in df_summary.columns:
		df_summary = df_summary[df_summary["WORKOUT_ID"] == workout_id]
		if df_summary.empty:
			return {}

	return df_summary.iloc[0].to_dict()


def as_float(value) -> Optional[float]:
	if value is None or pd.isna(value):
		return None
	return float(value)


def as_int(value) -> Optional[int]:
	if value is None or pd.isna(value):
		return None
	return int(value)


def derive_workout_date_iso(summary_row: dict) -> Optional[str]:
	start_ts = as_float(summary_row.get("START_TIMESTAMP"))
	if start_ts is None:
		return None
	return datetime.utcfromtimestamp(start_ts).isoformat() + "Z"


def derive_duration_seconds(summary_row: dict) -> Optional[float]:
	for key in ("DURATION", "TOTAL_TIME"):
		value = as_float(summary_row.get(key))
		if value is not None:
			return value

	start_ts = as_float(summary_row.get("START_TIMESTAMP"))
	end_ts = as_float(summary_row.get("END_TIMESTAMP"))
	if start_ts is not None and end_ts is not None:
		return max(0.0, end_ts - start_ts)

	return None


def upsert_workout_row(
	connection: sqlite3.Connection,
	workout_id: int,
	workout_dir: Path,
	workout_type: str,
	fit_path: Path,
) -> None:
	summary = load_summary_row(workout_dir, workout_id)
	workout_number = as_int(summary.get("WORKOUT_NUMBER"))
	workout_date = derive_workout_date_iso(summary)
	duration_seconds = derive_duration_seconds(summary)
	total_distance_m = as_float(summary.get("DISTANCE"))
	total_calories = as_int(summary.get("CALORIES"))
	has_gps = 1 if any(workout_dir.glob("workout_*.gpx")) else 0
	now_iso = datetime.utcnow().isoformat() + "Z"

	connection.execute(
		"""
		INSERT INTO workouts (
			workout_id,
			workout_number,
			workout_type,
			workout_date,
			duration_seconds,
			total_distance_m,
			total_calories,
			has_gps,
			source_workout_dir,
			fit_file_path,
			fit_generated_at,
			last_analyzed_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workout_id) DO UPDATE SET
			workout_number = excluded.workout_number,
			workout_type = excluded.workout_type,
			workout_date = excluded.workout_date,
			duration_seconds = excluded.duration_seconds,
			total_distance_m = excluded.total_distance_m,
			total_calories = excluded.total_calories,
			has_gps = excluded.has_gps,
			source_workout_dir = excluded.source_workout_dir,
			fit_file_path = excluded.fit_file_path,
			fit_generated_at = excluded.fit_generated_at,
			last_analyzed_at = excluded.last_analyzed_at,
			updated_at = CURRENT_TIMESTAMP
		""",
		(
			workout_id,
			workout_number,
			workout_type,
			workout_date,
			duration_seconds,
			total_distance_m,
			total_calories,
			has_gps,
			str(workout_dir),
			str(fit_path),
			now_iso,
			now_iso,
		),
	)
	connection.commit()


def get_sync_status(
	connection: sqlite3.Connection, workout_id: int
) -> Tuple[bool, Optional[str]]:
	row = connection.execute(
		"SELECT strava_synced, strava_activity_url FROM workouts WHERE workout_id = ?",
		(workout_id,),
	).fetchone()
	if not row:
		return False, None
	return bool(row[0]), row[1]


def should_skip_workout(connection: sqlite3.Connection, workout_id: int) -> bool:
	row = connection.execute(
		"SELECT fit_file_path FROM workouts WHERE workout_id = ?",
		(workout_id,),
	).fetchone()
	if not row:
		return False

	fit_file_path = row[0]
	if not fit_file_path:
		return False

	return Path(fit_file_path).exists()


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Analyze Huawei workouts and generate FIT files with sync metadata."
	)
	parser.add_argument(
		"--force",
		action="store_true",
		help="Force re-analyzing workouts even if FIT file is already tracked and exists.",
	)
	args = parser.parse_args()

	config = load_config()
	workout_location = config.get("workout_location", "")
	if not workout_location:
		raise ValueError("file_config.json is missing 'workout_location'.")
	fit_location = config.get("fit_location", "")
	if not fit_location:
		raise ValueError("file_config.json is missing 'fit_location'.")
	sync_db_location = config.get("sync_db_location", "")
	if not sync_db_location:
		raise ValueError("file_config.json is missing 'sync_db_location'.")

	workout_root = Path(workout_location)
	if not workout_root.exists():
		raise FileNotFoundError(f"Workout folder does not exist: {workout_root}")
	fit_root = Path(fit_location)
	fit_root.mkdir(parents=True, exist_ok=True)
	sync_db_path = resolve_sync_db_path(sync_db_location)
	connection = sqlite3.connect(sync_db_path)
	init_sync_db(connection)
	print(f"Sync database: {sync_db_path}")

	workout_dirs = [path for path in workout_root.iterdir() if path.is_dir()]

	def workout_sort_key(path: Path) -> Tuple[int, Union[int, str]]:
		name = path.name.strip()
		if name.isdigit():
			return (0, int(name))
		return (1, name)

	workout_dirs.sort(key=workout_sort_key)
	workout_dirs.reverse()

	if not workout_dirs:
		print("No workout folders found.")
		connection.close()
		return

	for workout_dir in workout_dirs:
		print("\n")
		workout_type = detect_workout_type(workout_dir)
		if not workout_dir.name.isdigit():
			print(f"Skipping {workout_dir}: folder name is not numeric workout ID.")
			continue
		workout_id = int(workout_dir.name)
		if not args.force and should_skip_workout(connection, workout_id):
			print(
				f"Skipping {workout_dir}: already in DB and FIT file exists. "
				"Use --force to reprocess."
			)
			synced, url = get_sync_status(connection, workout_id)
			print(f"  Sync status: {'synced' if synced else 'not synced'}")
			if url:
				print(f"  Strava URL: {url}")
			continue
		fit_path: Optional[Path] = None

		if workout_type == "swimming":
			print(f"Analyzing swimming workout in {workout_dir}...")
			fit_path = analyze_swimming(workout_dir, fit_root, pool_length=25)
			upsert_workout_row(connection, workout_id, workout_dir, workout_type, fit_path)
			synced, url = get_sync_status(connection, workout_id)
			print(f"  Sync status: {'synced' if synced else 'not synced'}")
			if url:
				print(f"  Strava URL: {url}")
			continue

		if workout_type == "cycling":
			print(f"Analyzing cycling workout in {workout_dir}...")
			fit_path = analyze_cycling(workout_dir, fit_root)
			upsert_workout_row(connection, workout_id, workout_dir, workout_type, fit_path)
			synced, url = get_sync_status(connection, workout_id)
			print(f"  Sync status: {'synced' if synced else 'not synced'}")
			if url:
				print(f"  Strava URL: {url}")
			continue

		if workout_type == "strength":
			print(f"Analyzing strength workout in {workout_dir}...")
			fit_path = analyze_strength(workout_dir, fit_root)
			upsert_workout_row(connection, workout_id, workout_dir, workout_type, fit_path)
			synced, url = get_sync_status(connection, workout_id)
			print(f"  Sync status: {'synced' if synced else 'not synced'}")
			if url:
				print(f"  Strava URL: {url}")
			continue

		print(f"Skipping {workout_dir}: no recognizable workout files.")

	connection.close()


if __name__ == "__main__":
	main()
