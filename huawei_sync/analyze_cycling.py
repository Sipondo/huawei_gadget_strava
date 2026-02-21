import argparse
import math
from datetime import datetime
from pathlib import Path
from typing import cast
import xml.etree.ElementTree as ET

import pandas as pd
from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
from fit_tool.profile.messages.file_id_message import FileIdMessage, FileIdTypeField
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import Activity, Event, EventType, Sport, SubSport


def parse_iso_timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def to_semicircles(value: float) -> int:
    return int(value * (2**31) / 180)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def parse_gpx_points(path: Path) -> pd.DataFrame:
    tree = ET.parse(path)
    root = tree.getroot()
    if "}" in root.tag:
        ns = {"gpx": root.tag.split("}")[0].strip("{")}
        trkpt_path = ".//gpx:trkpt"
        time_path = "gpx:time"
        ele_path = "gpx:ele"
    else:
        ns = {}
        trkpt_path = ".//trkpt"
        time_path = "time"
        ele_path = "ele"

    records = []
    for point in root.findall(trkpt_path, ns):
        lat_raw = point.attrib.get("lat")
        lon_raw = point.attrib.get("lon")
        if lat_raw is None or lon_raw is None:
            continue
        lat = float(lat_raw)
        lon = float(lon_raw)
        time_node = point.find(time_path, ns)
        if time_node is None or not time_node.text:
            continue

        ele_node = point.find(ele_path, ns)
        ele = float(ele_node.text) if ele_node is not None and ele_node.text else None
        timestamp = parse_iso_timestamp(time_node.text.strip())
        records.append({"timestamp": timestamp, "lat": lat, "lon": lon, "ele": ele})

    if not records:
        raise ValueError("No track points found in GPX file")

    return pd.DataFrame(records).sort_values("timestamp").reset_index(drop=True)


def analyze_workout(workout_dir: Path, output_dir: Path) -> Path:
    workout_dir = Path(workout_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    workout_id = workout_dir.name
    output_fname = output_dir / f"{workout_id}_cycling.fit"

    data_fname = workout_dir / "HUAWEI_WORKOUT_DATA_SAMPLE.csv"
    summary_fname = workout_dir / "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv"
    additional_fname = workout_dir / "HUAWEI_WORKOUT_SUMMARY_ADDITIONAL_VALUES_SAMPLE.csv"

    if not data_fname.exists() or not summary_fname.exists():
        raise FileNotFoundError(
            "Missing cycling files. Expected HUAWEI_WORKOUT_DATA_SAMPLE.csv and "
            "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv in the workout folder."
        )

    gpx_matches = list(workout_dir.glob(f"workout_*.gpx"))
    if not gpx_matches:
        raise FileNotFoundError(f"No GPX file found in {workout_dir}")

    gpx_fname = gpx_matches[0]

    print("Reading data files...")
    df_data = pd.read_csv(data_fname)
    df_summary = pd.read_csv(summary_fname)
    if additional_fname.exists():
        pd.read_csv(additional_fname)

    if "WORKOUT_ID" in df_summary.columns:
        df_summary = df_summary[df_summary["WORKOUT_ID"] == int(workout_id)]
    if "WORKOUT_ID" in df_data.columns:
        df_data = df_data[df_data["WORKOUT_ID"] == int(workout_id)]

    if df_summary.empty:
        raise ValueError("Summary file does not contain the workout ID.")

    df_summary = df_summary.iloc[0]

    print("Processing cycling data...")

    df_data = df_data[["TIMESTAMP", "HEART_RATE"]].copy()
    df_data.loc[df_data["HEART_RATE"] < 0, "HEART_RATE"] += 255
    df_data = df_data[df_data["HEART_RATE"] > 0]
    df_data["TIMESTAMP"] = df_data["TIMESTAMP"].astype(float)
    df_data = df_data.sort_values("TIMESTAMP").reset_index(drop=True)

    df_gpx = parse_gpx_points(gpx_fname)

    distances = [0.0]
    speeds = [0.0]

    for idx in range(1, len(df_gpx)):
        prev = df_gpx.iloc[idx - 1]
        cur = df_gpx.iloc[idx]
        delta = haversine_m(prev["lat"], prev["lon"], cur["lat"], cur["lon"])
        dt = cur["timestamp"] - prev["timestamp"]

        speeds.append(delta / dt if dt > 0 else 0.0)
        distances.append(distances[-1] + delta)

    df_gpx["distance"] = distances
    df_gpx["speed"] = speeds

    if not df_data.empty:
        df_gpx = pd.merge_asof(
            df_gpx,
            df_data,
            left_on="timestamp",
            right_on="TIMESTAMP",
            direction="nearest",
            tolerance=5,
        )

    start_timestamp = int(df_gpx["timestamp"].iloc[0])
    end_timestamp = int(df_gpx["timestamp"].iloc[-1])
    total_time = max(end_timestamp - start_timestamp, 1)
    total_distance = float(df_gpx["distance"].iloc[-1])

    avg_speed = total_distance / total_time if total_time > 0 else 0

    avg_heart_rate = int(df_data["HEART_RATE"].mean()) if not df_data.empty else 0
    max_heart_rate = int(df_data["HEART_RATE"].max()) if not df_data.empty else 0
    calories = int(df_summary.get("CALORIES", 0))

    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print(f"Total distance: {total_distance:.1f}m")
    print(f"Avg speed: {avg_speed:.2f} m/s")
    print(f"Avg HR: {avg_heart_rate} bpm, Max HR: {max_heart_rate} bpm")

    print("Building FIT file...")

    builder = FitFileBuilder(auto_define=True)

    file_id = FileIdMessage()
    file_id.type = 4  # type: ignore[assignment]
    file_id.manufacturer = 1
    file_id.product = 0
    file_id.serial_number = 12345
    file_id.time_created = start_timestamp * 1000
    builder.add(file_id)

    device_info = DeviceInfoMessage()
    device_info.device_index = 0
    device_info.manufacturer = 1
    device_info.product = 0
    device_info.product_name = "Huawei"  # generic
    device_info.serial_number = 12345
    device_info.software_version = 1.0
    device_info.timestamp = start_timestamp * 1000
    builder.add(device_info)

    session = SessionMessage()
    session.timestamp = end_timestamp * 1000
    session.start_time = start_timestamp * 1000
    session.total_elapsed_time = total_time
    session.total_timer_time = total_time
    session.total_distance = total_distance
    session.total_calories = calories
    session.avg_heart_rate = avg_heart_rate
    session.max_heart_rate = max_heart_rate
    session.enhanced_avg_speed = avg_speed
    session.enhanced_max_speed = df_gpx["speed"].max() if not df_gpx.empty else 0
    session.sport = Sport.CYCLING
    if hasattr(SubSport, "ROAD"):
        session.sub_sport = SubSport.ROAD
    elif hasattr(SubSport, "GENERIC"):
        session.sub_sport = SubSport.GENERIC
    else:
        session.sub_sport = cast(SubSport, 0)
    session.event = Event.SESSION
    session.event_type = EventType.STOP
    builder.add(session)

    print(f"Adding {len(df_gpx)} track points...")
    for _, row in df_gpx.iterrows():
        record = RecordMessage()
        record.timestamp = int(row["timestamp"] * 1000)
        record.position_lat = to_semicircles(float(row["lat"]))
        record.position_long = to_semicircles(float(row["lon"]))
        record.distance = float(row["distance"])
        record.speed = float(row["speed"])

        if row.get("ele") is not None and not pd.isna(row.get("ele")):
            record.altitude = float(row["ele"])

        if "HEART_RATE" in row and not pd.isna(row["HEART_RATE"]):
            record.heart_rate = int(row["HEART_RATE"])

        builder.add(record)

    activity = ActivityMessage()
    activity.timestamp = end_timestamp * 1000
    activity.total_timer_time = total_time
    activity.num_sessions = 1
    activity.type = cast(Activity, 0)
    activity.event = Event.ACTIVITY
    activity.event_type = EventType.STOP
    builder.add(activity)

    output_fname.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing FIT file to {output_fname}...")
    fit_file = builder.build()
    fit_file.to_file(str(output_fname))

    print(f"✓ Successfully created {output_fname}")
    print(f"  - Duration: {total_time/60:.1f} minutes")
    print(f"  - Distance: {total_distance/1000:.2f} km")
    print(f"  - Avg speed: {avg_speed*3.6:.2f} km/h")

    return output_fname


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a cycling workout folder.")
    parser.add_argument("--workout-dir", required=True, help="Workout folder path.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for the FIT file (defaults to workout folder).",
    )
    args = parser.parse_args()

    workout_dir = Path(args.workout_dir)
    output_dir = Path(args.output_dir) if args.output_dir else workout_dir

    analyze_workout(workout_dir, output_dir)


if __name__ == "__main__":
    main()
