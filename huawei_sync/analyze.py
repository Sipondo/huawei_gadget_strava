import json
import sys
from pathlib import Path
from typing import Optional, Tuple, Union

sys.path.append(str(Path(__file__).resolve().parent))

from analyze_cycling import analyze_workout as analyze_cycling
from analyze_swimming import analyze_workout as analyze_swimming


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


def detect_workout_type(workout_dir: Path) -> Optional[str]:
	swim_segments = workout_dir / "HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE.csv"
	if swim_segments.exists():
		return "swimming"

	gpx_matches = list(workout_dir.glob("workout_*.gpx"))
	if gpx_matches:
		return "cycling"

	return None


def main() -> None:
	config = load_config()
	workout_location = config.get("workout_location", "")
	if not workout_location:
		raise ValueError("file_config.json is missing 'workout_location'.")
	fit_location = config.get("fit_location", "")
	if not fit_location:
		raise ValueError("file_config.json is missing 'fit_location'.")

	workout_root = Path(workout_location)
	if not workout_root.exists():
		raise FileNotFoundError(f"Workout folder does not exist: {workout_root}")
	fit_root = Path(fit_location)
	fit_root.mkdir(parents=True, exist_ok=True)

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
		return

	for workout_dir in workout_dirs:
		print("\n")
		workout_type = detect_workout_type(workout_dir)
		if workout_type == "swimming":
			print(f"Analyzing swimming workout in {workout_dir}...")
			analyze_swimming(workout_dir, fit_root, pool_length=25)
			continue

		if workout_type == "cycling":
			print(f"Analyzing cycling workout in {workout_dir}...")
			analyze_cycling(workout_dir, fit_root)
			continue

		print(f"Skipping {workout_dir}: no recognizable workout files.")


if __name__ == "__main__":
	main()
