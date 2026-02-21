from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text


SUMMARY_TABLE = "HUAWEI_WORKOUT_SUMMARY_SAMPLE"
CONFIG_FILE_NAME = "file_config.json"


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


def resolve_db_path(cli_db_path: str | None) -> Path:
	if cli_db_path:
		path = Path(cli_db_path)
		if path.exists():
			return path
		raise FileNotFoundError(f"Database path does not exist: {path}")

	env_db_path = os.getenv("GADGETBRIDGE_DB_PATH")
	if env_db_path:
		path = Path(env_db_path)
		if path.exists():
			return path
		raise FileNotFoundError(
			f"GADGETBRIDGE_DB_PATH is set but file does not exist: {path}"
		)

	config = load_config()
	unzip_location = config.get("unzip_location", "")
	if not unzip_location:
		raise ValueError("file_config.json is missing 'unzip_location'.")

	db_dir = Path(unzip_location) / "database"
	if not db_dir.exists():
		raise FileNotFoundError(f"Database directory does not exist: {db_dir}")

	primary_db = db_dir / "Gadgetbridge"
	if primary_db.exists():
		return primary_db

	fallback_matches = sorted(db_dir.glob(".Gadgetbridge.*"))
	if fallback_matches:
		return fallback_matches[-1]

	raise FileNotFoundError(
		f"Could not find Gadgetbridge database in {db_dir}. "
		"Expected Gadgetbridge or .Gadgetbridge.*"
	)


def resolve_output_dir(cli_output_dir: str | None) -> Path:
	if cli_output_dir:
		return Path(cli_output_dir)

	config = load_config()
	workout_location = config.get("workout_location", "")
	if workout_location:
		return Path(workout_location)

	return Path(__file__).resolve().parents[1] / "raw" / "workouts"


def resolve_gpx_dir() -> Path | None:
	config = load_config()
	gpx_location = config.get("gpx_location", "")
	if not gpx_location:
		return None

	gpx_dir = Path(gpx_location)
	if gpx_dir.exists():
		return gpx_dir

	print(f"GPX directory does not exist: {gpx_dir}")
	return None


def get_workout_tables(engine) -> list[str]:
	db_inspector = inspect(engine)
	tables = []
	for table_name in db_inspector.get_table_names():
		columns = {column["name"] for column in db_inspector.get_columns(table_name)}
		if "WORKOUT_ID" in columns:
			tables.append(table_name)
	return sorted(tables)


def get_workout_ids(engine) -> list[int]:
	query = text(
		f"SELECT DISTINCT WORKOUT_ID FROM {SUMMARY_TABLE} "
		"WHERE WORKOUT_ID IS NOT NULL ORDER BY WORKOUT_ID"
	)
	df_workout_ids = pd.read_sql_query(query, engine)
	return [int(workout_id) for workout_id in df_workout_ids["WORKOUT_ID"].tolist()]


def workout_already_exported(workout_dir: Path) -> bool:
	if not workout_dir.exists():
		return False
	return any(workout_dir.glob("*.csv"))


def export_workout(engine, workout_id: int, workout_tables: list[str], output_dir: Path) -> int:
	workout_dir = output_dir / str(workout_id)
	workout_dir.mkdir(parents=True, exist_ok=True)

	exported_files = 0
	for table_name in workout_tables:
		query = text(f"SELECT * FROM {table_name} WHERE WORKOUT_ID = :workout_id")
		df_table = pd.read_sql_query(query, engine, params={"workout_id": workout_id})

		if df_table.empty:
			continue

		output_file = workout_dir / f"{table_name}.csv"
		df_table.to_csv(output_file, index=False)
		exported_files += 1

	return exported_files


def copy_gpx_files(gpx_dir: Path | None, workout_id: int, workout_dir: Path) -> int:
	if not gpx_dir:
		return 0

	pattern = f"workout_{workout_id}_*"
	matching_files = [path for path in gpx_dir.glob(pattern) if path.is_file()]

	if not matching_files:
		return 0

	workout_dir.mkdir(parents=True, exist_ok=True)
	for source_path in matching_files:
		destination = workout_dir / source_path.name
		shutil.copy2(source_path, destination)

	return len(matching_files)


def main() -> None:
	parser = argparse.ArgumentParser(
		description=(
			"Download all Huawei workout-related table data per workout_id "
			"from the Gadgetbridge database."
		)
	)
	parser.add_argument(
		"--db-path",
		default=None,
		help="Path to Gadgetbridge db (or Huawei health sqlite db file).",
	)
	parser.add_argument(
		"--output-dir",
		default=None,
		help="Directory where workout folders and csv files will be written.",
	)
	args = parser.parse_args()

	db_path = resolve_db_path(args.db_path)
	output_dir = resolve_output_dir(args.output_dir)
	output_dir.mkdir(parents=True, exist_ok=True)
	gpx_dir = resolve_gpx_dir()

	engine = create_engine(f"sqlite:///{db_path}")

	workout_ids = get_workout_ids(engine)
	if not workout_ids:
		print("No workouts found in summary table.")
		return

	workout_tables = get_workout_tables(engine)
	if not workout_tables:
		print("No tables with WORKOUT_ID found. Nothing to export.")
		return

	print(f"Database: {db_path}")
	print(f"Workouts found: {len(workout_ids)}")
	print(f"Workout-related tables: {len(workout_tables)}")

	skipped = 0
	exported = 0

	for workout_id in workout_ids:
		workout_dir = output_dir / str(workout_id)
		if False: #workout_already_exported(workout_dir):
			print(f"Skipping workout {workout_id}: files already exist in {workout_dir}")
			skipped += 1
			continue

		print(f"Exporting workout {workout_id}...")
		copied_gpx = copy_gpx_files(gpx_dir, workout_id, workout_dir)
		if copied_gpx:
			print(f"  Copied {copied_gpx} gpx file(s)")
		exported_files = export_workout(engine, workout_id, workout_tables, output_dir)
		print(f"  Saved {exported_files} file(s)")
		exported += 1

	print(
		"Done. "
		f"Exported workouts: {exported}, Skipped workouts: {skipped}, Total workouts: {len(workout_ids)}"
	)


if __name__ == "__main__":
	main()
