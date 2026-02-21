import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.length_message import LengthMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import Event, EventType, Sport, SubSport, SwimStroke


def analyze_workout(workout_dir: Path, output_dir: Path, pool_length: int) -> Path:
    workout_dir = Path(workout_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_fname = workout_dir / "HUAWEI_WORKOUT_SUMMARY_SAMPLE.csv"
    raw_heart_fname = workout_dir / "HUAWEI_WORKOUT_DATA_SAMPLE.csv"
    raw_segment_fname = workout_dir / "HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE.csv"

    if not raw_heart_fname.exists() or not raw_segment_fname.exists():
        raise FileNotFoundError(
            "Missing swim files. Expected HUAWEI_WORKOUT_DATA_SAMPLE.csv and "
            "HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE.csv in the workout folder."
        )

    workout_id = workout_dir.name
    output_fname = output_dir / f"{workout_id}_swimming.fit"

    print("Reading data files...")
    df_heart_data = pd.read_csv(raw_heart_fname)
    df_segment_data = pd.read_csv(raw_segment_fname)

    if "WORKOUT_ID" in df_heart_data.columns:
        df_heart_data = df_heart_data[df_heart_data["WORKOUT_ID"] == int(workout_id)]
    if "WORKOUT_ID" in df_segment_data.columns:
        df_segment_data = df_segment_data[df_segment_data["WORKOUT_ID"] == int(workout_id)]

    print(f"Processing {len(df_segment_data)} segments...")

    df_segment_data = df_segment_data.sort_values("SEGMENT_INDEX").reset_index(drop=True)
    df_heart_data["HEART_RATE"] = df_heart_data["HEART_RATE"].abs()
    df_heart_data["SPEED_MS"] = df_heart_data["SPEED"] / 10.0

    start_timestamp = df_heart_data["TIMESTAMP"].min()

    cumulative_time = 0
    segment_start_times = []
    segment_end_times = []

    for _, row in df_segment_data.iterrows():
        segment_start_times.append(start_timestamp + cumulative_time)
        cumulative_time += row["TIME"]
        segment_end_times.append(start_timestamp + cumulative_time)

    df_segment_data["START_TIMESTAMP"] = segment_start_times
    df_segment_data["END_TIMESTAMP"] = segment_end_times

    total_time = df_segment_data["TIME"].sum()
    total_distance = df_segment_data["DISTANCE"].sum()
    total_strokes = df_segment_data["STROKES"].sum()
    avg_speed = total_distance / total_time if total_time > 0 else 0
    max_speed = 0

    for _, row in df_segment_data.iterrows():
        if row["TIME"] > 0:
            segment_speed = row["DISTANCE"] / row["TIME"]
            max_speed = max(max_speed, segment_speed)

    avg_heart_rate = int(df_heart_data["HEART_RATE"].mean())
    max_heart_rate = int(df_heart_data["HEART_RATE"].max())

    calories = int(total_time / 60 * 8)
    if summary_fname.exists():
        df_summary = pd.read_csv(summary_fname)
        if "WORKOUT_ID" in df_summary.columns:
            df_summary = df_summary[df_summary["WORKOUT_ID"] == int(workout_id)]
        if not df_summary.empty and "CALORIES" in df_summary.columns:
            calories = int(df_summary.iloc[0]["CALORIES"])

    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print(f"Total distance: {total_distance}m")
    print(f"Total strokes: {total_strokes}")
    print(f"Avg speed: {avg_speed:.2f} m/s")
    print(f"Avg HR: {avg_heart_rate} bpm, Max HR: {max_heart_rate} bpm")

    print("Building FIT file...")

    builder = FitFileBuilder(auto_define=True)

    file_id = FileIdMessage()
    file_id.type = 4  # type: ignore[assignment]
    file_id.manufacturer = 1
    file_id.product = 0
    file_id.serial_number = 12345
    file_id.time_created = int(start_timestamp * 1000)
    builder.add(file_id)

    session = SessionMessage()
    session.timestamp = int((start_timestamp + total_time) * 1000)
    session.start_time = int(start_timestamp * 1000)
    session.total_elapsed_time = total_time
    session.total_timer_time = total_time
    session.total_distance = total_distance
    session.sport = Sport.SWIMMING
    session.sub_sport = SubSport.LAP_SWIMMING
    session.total_calories = calories
    session.avg_heart_rate = avg_heart_rate
    session.max_heart_rate = max_heart_rate
    session.pool_length = pool_length
    session.num_lengths = len(df_segment_data)
    session.total_cycles = total_strokes
    session.enhanced_avg_speed = avg_speed
    session.enhanced_max_speed = max_speed
    session.event = Event.SESSION
    session.event_type = EventType.STOP
    builder.add(session)

    print(
        f"Adding {len(df_segment_data)} laps and {len(df_heart_data)} heart rate records..."
    )

    lap_hr_data = {}
    for segment_idx, (_, row) in enumerate(df_segment_data.iterrows()):
        segment_start = row["START_TIMESTAMP"]
        segment_end = row["END_TIMESTAMP"]

        segment_hr = df_heart_data[
            (df_heart_data["TIMESTAMP"] >= segment_start)
            & (df_heart_data["TIMESTAMP"] <= segment_end)
        ]

        if len(segment_hr) > 0:
            lap_hr_data[segment_idx] = {
                "avg": int(segment_hr["HEART_RATE"].mean()),
                "max": int(segment_hr["HEART_RATE"].max()),
                "min": int(segment_hr["HEART_RATE"].min()),
            }
        else:
            lap_hr_data[segment_idx] = {
                "avg": avg_heart_rate,
                "max": max_heart_rate,
                "min": 0,
            }

    for segment_idx, (_, row) in enumerate(df_segment_data.iterrows()):
        lap = LapMessage()
        lap.timestamp = int(row["END_TIMESTAMP"] * 1000)
        lap.start_time = int(row["START_TIMESTAMP"] * 1000)
        lap.total_elapsed_time = row["TIME"]
        lap.total_timer_time = row["TIME"]
        lap.total_distance = row["DISTANCE"]

        hr_stats = lap_hr_data.get(
            segment_idx, {"avg": avg_heart_rate, "max": max_heart_rate, "min": 0}
        )
        lap.avg_heart_rate = hr_stats["avg"]
        lap.max_heart_rate = hr_stats["max"]

        lap.total_cycles = int(row["STROKES"])
        lap_speed = row["DISTANCE"] / row["TIME"] if row["TIME"] > 0 else 0
        lap.enhanced_avg_speed = lap_speed
        lap.enhanced_max_speed = lap_speed
        lap.event = Event.LAP
        lap.event_type = EventType.STOP
        lap.message_index = segment_idx
        builder.add(lap)

        length = LengthMessage()
        length.timestamp = int(row["END_TIMESTAMP"] * 1000)
        length.start_time = int(row["START_TIMESTAMP"] * 1000)
        length.total_elapsed_time = row["TIME"]
        length.total_timer_time = row["TIME"]
        length.total_strokes = int(row["STROKES"])

        length_speed = row["DISTANCE"] / row["TIME"] if row["TIME"] > 0 else 0
        length.avg_speed = length_speed
        length.swim_stroke = (
            SwimStroke.FREESTYLE if row["SWIM_TYPE"] == 2 else SwimStroke.FREESTYLE
        )

        if row["TIME"] > 0:
            length.avg_swimming_cadence = int((row["STROKES"] / row["TIME"]) * 60)
        else:
            length.avg_swimming_cadence = 0

        length.length_type = 1  # type: ignore[assignment]
        length.event = Event.LENGTH
        length.event_type = EventType.STOP
        length.message_index = segment_idx
        builder.add(length)

    for _, row in df_heart_data.iterrows():
        if row["TIMESTAMP"] >= start_timestamp and row["TIMESTAMP"] <= (
            start_timestamp + total_time
        ):
            record = RecordMessage()
            record.timestamp = int(row["TIMESTAMP"] * 1000)
            record.heart_rate = int(row["HEART_RATE"])

            if row["SPEED_MS"] > 0:
                record.speed = row["SPEED_MS"]

            builder.add(record)

    activity = ActivityMessage()
    activity.timestamp = int((start_timestamp + total_time) * 1000)
    activity.total_timer_time = total_time
    activity.num_sessions = 1
    activity.type = Event.ACTIVITY  # type: ignore[assignment]
    activity.event = Event.ACTIVITY
    activity.event_type = EventType.STOP
    builder.add(activity)

    print(f"Writing FIT file to {output_fname}...")
    fit_file = builder.build()
    fit_file.to_file(str(output_fname))

    print(f"✓ Successfully created {output_fname}")
    print(f"  - Duration: {total_time/60:.1f} minutes")
    print(f"  - Distance: {total_distance}m ({len(df_segment_data)} lengths)")
    print(f"  - Avg pace: {(total_time/60)/(total_distance/1000):.2f} min/km")
    print(f"  - Calories: {calories} kcal")

    return output_fname


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a swimming workout folder.")
    parser.add_argument("--workout-dir", required=True, help="Workout folder path.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for the FIT file (defaults to workout folder).",
    )
    parser.add_argument(
        "--pool-length",
        type=int,
        default=25,
        help="Pool length in meters.",
    )
    args = parser.parse_args()

    workout_dir = Path(args.workout_dir)
    output_dir = Path(args.output_dir) if args.output_dir else workout_dir

    analyze_workout(workout_dir, output_dir, args.pool_length)


if __name__ == "__main__":
    main()
