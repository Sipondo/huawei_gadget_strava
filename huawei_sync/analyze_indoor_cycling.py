import argparse
from pathlib import Path
from typing import cast

import pandas as pd
from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import Activity, Event, EventType, Sport, SubSport


MAX_FIT_SPEED_MS = 65.535


def analyze_workout(workout_dir: Path, output_dir: Path) -> Path:
    workout_dir = Path(workout_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    workout_id = workout_dir.name
    output_fname = output_dir / f"{workout_id}_indoor_cycling.fit"

    data_fname = workout_dir / "HUAWEI_WORKOUT_DATA_SAMPLE.csv"
    summary_fname = workout_dir / "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv"

    if not data_fname.exists() or not summary_fname.exists():
        raise FileNotFoundError(
            "Missing indoor cycling files. Expected HUAWEI_WORKOUT_DATA_SAMPLE.csv "
            "and HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv in the workout folder."
        )

    print("Reading indoor cycling data files...")
    df_data = pd.read_csv(data_fname)
    df_summary = pd.read_csv(summary_fname)

    if "WORKOUT_ID" in df_data.columns:
        df_data = df_data[df_data["WORKOUT_ID"] == int(workout_id)]
    if "WORKOUT_ID" in df_summary.columns:
        df_summary = df_summary[df_summary["WORKOUT_ID"] == int(workout_id)]

    if df_summary.empty:
        raise ValueError("Summary file does not contain the workout ID.")

    summary = df_summary.iloc[0]

    required_columns = ["TIMESTAMP", "HEART_RATE", "SPEED"]
    for column_name in required_columns:
        if column_name not in df_data.columns:
            raise ValueError(f"Missing required cycling data column: {column_name}")

    df_data = df_data[required_columns].copy()
    df_data = df_data.sort_values("TIMESTAMP").reset_index(drop=True)
    df_data["HEART_RATE"] = df_data["HEART_RATE"].abs()
    df_data = df_data[df_data["HEART_RATE"] > 0]
    df_data["SPEED_MS"] = df_data["SPEED"].astype(float) / 10.0
    df_data.loc[df_data["SPEED_MS"] < 0, "SPEED_MS"] = 0.0

    if df_data.empty:
        raise ValueError("No valid heart-rate records found for indoor cycling workout.")

    start_timestamp = int(summary["START_TIMESTAMP"]) if "START_TIMESTAMP" in summary else int(df_data["TIMESTAMP"].min())
    end_timestamp = int(summary["END_TIMESTAMP"]) if "END_TIMESTAMP" in summary else int(df_data["TIMESTAMP"].max())
    total_time = max(1, end_timestamp - start_timestamp)

    summary_distance = float(summary.get("DISTANCE", 0.0) or 0.0)
    total_calories = int(summary.get("CALORIES", 0) or 0)

    df_data["TIMESTAMP"] = df_data["TIMESTAMP"].astype(float)
    cumulative_distance = [0.0]
    for idx in range(1, len(df_data)):
        prev_ts = float(df_data.iloc[idx - 1]["TIMESTAMP"])
        cur_ts = float(df_data.iloc[idx]["TIMESTAMP"])
        dt = max(0.0, cur_ts - prev_ts)
        speed_ms = float(df_data.iloc[idx - 1]["SPEED_MS"])
        cumulative_distance.append(cumulative_distance[-1] + speed_ms * dt)

    df_data["DISTANCE_M"] = cumulative_distance
    derived_distance = float(df_data["DISTANCE_M"].iloc[-1]) if not df_data.empty else 0.0
    total_distance = summary_distance if summary_distance > 0 else derived_distance

    if total_distance > 0 and derived_distance > 0:
        scale = total_distance / derived_distance
        df_data["DISTANCE_M"] = df_data["DISTANCE_M"] * scale

    avg_heart_rate = int(df_data["HEART_RATE"].mean())
    max_heart_rate = int(df_data["HEART_RATE"].max())
    avg_speed = total_distance / total_time if total_time > 0 else 0.0
    max_speed = float(df_data["SPEED_MS"].max()) if not df_data.empty else 0.0

    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print(f"Total distance: {total_distance:.1f}m")
    print(f"Avg speed: {avg_speed:.2f} m/s")
    print(f"Avg HR: {avg_heart_rate} bpm, Max HR: {max_heart_rate} bpm")

    builder = FitFileBuilder(auto_define=True)

    file_id = FileIdMessage()
    file_id.type = 4  # type: ignore[assignment]
    file_id.manufacturer = 1
    file_id.product = 0
    file_id.serial_number = 12345
    file_id.time_created = start_timestamp * 1000
    builder.add(file_id)

    session = SessionMessage()
    session.timestamp = end_timestamp * 1000
    session.start_time = start_timestamp * 1000
    session.total_elapsed_time = total_time
    session.total_timer_time = total_time
    session.total_distance = total_distance
    session.total_calories = total_calories
    session.avg_heart_rate = avg_heart_rate
    session.max_heart_rate = max_heart_rate
    session.enhanced_avg_speed = avg_speed
    session.enhanced_max_speed = max_speed

    if hasattr(Sport, "CYCLING"):
        session.sport = Sport.CYCLING
    else:
        session.sport = cast(Sport, 2)

    if hasattr(SubSport, "INDOOR_CYCLING"):
        session.sub_sport = SubSport.INDOOR_CYCLING
    elif hasattr(SubSport, "GENERIC"):
        session.sub_sport = SubSport.GENERIC
    else:
        session.sub_sport = cast(SubSport, 0)

    session.event = Event.SESSION
    session.event_type = EventType.STOP
    builder.add(session)

    for _, row in df_data.iterrows():
        record = RecordMessage()
        record.timestamp = int(float(row["TIMESTAMP"]) * 1000)
        record.heart_rate = int(row["HEART_RATE"])
        safe_speed = max(0.0, min(float(row["SPEED_MS"]), MAX_FIT_SPEED_MS))
        record.speed = safe_speed
        record.distance = float(row["DISTANCE_M"])
        builder.add(record)

    activity = ActivityMessage()
    activity.timestamp = end_timestamp * 1000
    activity.total_timer_time = total_time
    activity.num_sessions = 1
    activity.type = cast(Activity, 0)
    activity.event = Event.ACTIVITY
    activity.event_type = EventType.STOP
    builder.add(activity)

    print(f"Writing FIT file to {output_fname}...")
    fit_file = builder.build()
    fit_file.to_file(str(output_fname))

    print(f"✓ Successfully created {output_fname}")
    return output_fname


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze an indoor cycling workout folder.")
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
