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


def analyze_workout(workout_dir: Path, output_dir: Path) -> Path:
    workout_dir = Path(workout_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    workout_id = workout_dir.name
    output_fname = output_dir / f"{workout_id}_strength.fit"

    data_fname = workout_dir / "HUAWEI_WORKOUT_DATA_SAMPLE.csv"
    summary_fname = workout_dir / "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv"

    if not data_fname.exists() or not summary_fname.exists():
        raise FileNotFoundError(
            "Missing strength files. Expected HUAWEI_WORKOUT_DATA_SAMPLE.csv and "
            "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv in the workout folder."
        )

    print("Reading strength data files...")
    df_data = pd.read_csv(data_fname)
    df_summary = pd.read_csv(summary_fname)

    if "WORKOUT_ID" in df_data.columns:
        df_data = df_data[df_data["WORKOUT_ID"] == int(workout_id)]
    if "WORKOUT_ID" in df_summary.columns:
        df_summary = df_summary[df_summary["WORKOUT_ID"] == int(workout_id)]

    if df_summary.empty:
        raise ValueError("Summary file does not contain the workout ID.")

    summary = df_summary.iloc[0]

    df_data = df_data[["TIMESTAMP", "HEART_RATE"]].copy()
    df_data.loc[df_data["HEART_RATE"] < 0, "HEART_RATE"] += 255
    df_data = df_data[df_data["HEART_RATE"] > 0]
    df_data = df_data.sort_values("TIMESTAMP").reset_index(drop=True)

    start_timestamp = int(summary["START_TIMESTAMP"]) if "START_TIMESTAMP" in summary else None
    end_timestamp = int(summary["END_TIMESTAMP"]) if "END_TIMESTAMP" in summary else None

    if start_timestamp is None and not df_data.empty:
        start_timestamp = int(df_data["TIMESTAMP"].min())
    if end_timestamp is None and not df_data.empty:
        end_timestamp = int(df_data["TIMESTAMP"].max())

    if start_timestamp is None or end_timestamp is None:
        raise ValueError("Could not determine workout start/end timestamps.")

    total_time = max(1, end_timestamp - start_timestamp)
    total_calories = int(summary.get("CALORIES", 0))
    total_distance = float(summary.get("DISTANCE", 0.0) or 0.0)
    avg_heart_rate = int(df_data["HEART_RATE"].mean()) if not df_data.empty else 0
    max_heart_rate = int(df_data["HEART_RATE"].max()) if not df_data.empty else 0

    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print(f"Total distance: {total_distance:.1f}m")
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

    if hasattr(Sport, "TRAINING"):
        session.sport = Sport.TRAINING
    elif hasattr(Sport, "GENERIC"):
        session.sport = Sport.GENERIC
    else:
        session.sport = cast(Sport, 0)

    if hasattr(SubSport, "STRENGTH_TRAINING"):
        session.sub_sport = SubSport.STRENGTH_TRAINING
    elif hasattr(SubSport, "GENERIC"):
        session.sub_sport = SubSport.GENERIC
    else:
        session.sub_sport = cast(SubSport, 0)

    session.event = Event.SESSION
    session.event_type = EventType.STOP
    builder.add(session)

    for _, row in df_data.iterrows():
        record = RecordMessage()
        record.timestamp = int(row["TIMESTAMP"]) * 1000
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

    print(f"Writing FIT file to {output_fname}...")
    fit_file = builder.build()
    fit_file.to_file(str(output_fname))

    print(f"✓ Successfully created {output_fname}")
    return output_fname


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a strength workout folder.")
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
