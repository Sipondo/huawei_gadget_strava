import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, cast

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


def get_summary_total_time_seconds(df_summary: pd.DataFrame) -> Optional[float]:
    if df_summary.empty:
        return None

    row = df_summary.iloc[0]
    for column_name in ("DURATION", "TOTAL_TIME"):
        if column_name in row and pd.notna(row[column_name]):
            return float(row[column_name])

    if (
        "START_TIMESTAMP" in row
        and "END_TIMESTAMP" in row
        and pd.notna(row["START_TIMESTAMP"])
        and pd.notna(row["END_TIMESTAMP"])
    ):
        return float(row["END_TIMESTAMP"] - row["START_TIMESTAMP"])

    return None


def detect_sprint_session(
    df_segment_data: pd.DataFrame, workout_total_time_seconds: Optional[float]
) -> bool:
    active_segments = df_segment_data[df_segment_data["DISTANCE"] > 0].reset_index(drop=True)
    active_count = len(active_segments)
    if active_count < 8 or active_count % 4 != 0:
        return False

    times = active_segments["TIME"].astype(float)
    reference = cast(pd.Series, times[times > 0])
    if reference.empty:
        return False

    typical_length_time = float(reference.quantile(0.6))
    if typical_length_time <= 0:
        typical_length_time = float(reference.median())

    long_threshold = max(typical_length_time * 1.9, typical_length_time + 25.0)

    block_count = active_count // 4
    boundary_long_count = 0
    for block_idx in range(block_count - 1):
        boundary_end_idx = block_idx * 4 + 3
        boundary_next_idx = boundary_end_idx + 1
        end_time = float(active_segments.iloc[boundary_end_idx]["TIME"])
        next_time = float(active_segments.iloc[boundary_next_idx]["TIME"])
        if max(end_time, next_time) >= long_threshold:
            boundary_long_count += 1

    min_boundary_hits = max(1, int((block_count - 1) * 0.35))
    if boundary_long_count >= min_boundary_hits:
        return True

    if workout_total_time_seconds is None:
        return False

    active_time = float(active_segments["TIME"].sum())
    rest_gap = workout_total_time_seconds - active_time
    return rest_gap >= 120 and block_count >= 2


def add_sprint_rest_segments(df_segment_data: pd.DataFrame) -> pd.DataFrame:
    active_segments = cast(
        pd.DataFrame,
        df_segment_data[df_segment_data["DISTANCE"] > 0].reset_index(drop=True),
    )
    active_count = len(active_segments)
    if active_count < 4:
        return cast(pd.DataFrame, active_segments.reset_index(drop=True))

    times = active_segments["TIME"].astype(float)
    reference = cast(pd.Series, times[times > 0])
    if reference.empty:
        return cast(pd.DataFrame, active_segments.reset_index(drop=True))

    typical_length_time = float(reference.quantile(0.6))
    if typical_length_time <= 0:
        typical_length_time = float(reference.median())

    long_threshold = max(typical_length_time * 1.9, typical_length_time + 25.0)
    min_swim_time = max(8.0, typical_length_time * 0.45)

    adjusted_times = [float(value) for value in active_segments["TIME"].tolist()]
    before_rest = {}
    after_rest = {}

    block_count = active_count // 4
    for block_idx in range(block_count - 1):
        boundary_end_idx = block_idx * 4 + 3
        boundary_next_idx = boundary_end_idx + 1

        end_time = adjusted_times[boundary_end_idx]
        next_time = adjusted_times[boundary_next_idx]

        candidate_idx = boundary_end_idx if end_time >= next_time else boundary_next_idx
        candidate_time = adjusted_times[candidate_idx]

        detected_rest = 0.0
        if candidate_time >= long_threshold:
            detected_rest = candidate_time - typical_length_time
            detected_rest = min(detected_rest, candidate_time - min_swim_time)
            detected_rest = max(0.0, detected_rest)

        if detected_rest >= 8.0:
            adjusted_times[candidate_idx] = max(min_swim_time, candidate_time - detected_rest)
            if candidate_idx == boundary_end_idx:
                after_rest[boundary_end_idx] = after_rest.get(boundary_end_idx, 0.0) + detected_rest
            else:
                before_rest[boundary_next_idx] = before_rest.get(boundary_next_idx, 0.0) + detected_rest
            continue

        block_start = block_idx * 4
        block_swim_time = sum(adjusted_times[block_start : block_start + 4])
        fallback_rest = max(0.0, 180.0 - block_swim_time)
        if fallback_rest >= 8.0:
            after_rest[boundary_end_idx] = after_rest.get(boundary_end_idx, 0.0) + fallback_rest

    transformed_rows = []
    for index in range(active_count):
        if index in before_rest and before_rest[index] > 0:
            rest_template = active_segments.iloc[index].to_dict()
            rest_template["TIME"] = before_rest[index]
            rest_template["DISTANCE"] = 0
            rest_template["STROKES"] = 0
            rest_template["SWIM_TYPE"] = 0
            transformed_rows.append(rest_template)

        row = active_segments.iloc[index].to_dict()
        row["TIME"] = adjusted_times[index]
        transformed_rows.append(row)

        if index in after_rest and after_rest[index] > 0:
            rest_template = active_segments.iloc[index].to_dict()
            rest_template["TIME"] = after_rest[index]
            rest_template["DISTANCE"] = 0
            rest_template["STROKES"] = 0
            rest_template["SWIM_TYPE"] = 0
            transformed_rows.append(rest_template)

    result = pd.DataFrame(transformed_rows)
    return result.reset_index(drop=True)


def fix_collapsed_start_lengths(
    df_segment_data: pd.DataFrame, pool_length: int
) -> Tuple[pd.DataFrame, bool]:
    active_segments = df_segment_data[df_segment_data["DISTANCE"] > 0].reset_index()
    if len(active_segments) < 4:
        return df_segment_data, False

    first_active = active_segments.iloc[0]
    first_global_index = int(first_active["index"])
    first_time = float(first_active["TIME"])
    first_distance = float(first_active["DISTANCE"])

    if first_distance <= 0 or first_distance > (pool_length * 1.5):
        return df_segment_data, False

    comparison_window = active_segments.iloc[1:9]
    if comparison_window.empty:
        return df_segment_data, False

    comparison_times = comparison_window["TIME"]
    comparison_times = comparison_times[comparison_times > 0]
    if comparison_times.empty:
        return df_segment_data, False

    typical_length_time = float(comparison_times.median())
    inflated_threshold = max(45.0, typical_length_time * 2.2)
    if first_time < inflated_threshold:
        return df_segment_data, False

    template = df_segment_data.iloc[first_global_index].to_dict()

    split_times = [first_time / 3.0, first_time / 3.0, first_time - 2.0 * (first_time / 3.0)]
    original_strokes = float(template.get("STROKES", 0) or 0)
    split_strokes = [
        int(round(original_strokes / 3.0)),
        int(round(original_strokes / 3.0)),
        int(round(original_strokes - 2.0 * round(original_strokes / 3.0))),
    ]

    transformed_rows = []
    for index, (_, row) in enumerate(df_segment_data.iterrows()):
        if index != first_global_index:
            transformed_rows.append(row.to_dict())
            continue

        for split_idx in range(3):
            repaired = dict(template)
            repaired["TIME"] = split_times[split_idx]
            repaired["DISTANCE"] = first_distance
            repaired["STROKES"] = max(0, split_strokes[split_idx])
            transformed_rows.append(repaired)

    repaired_df = pd.DataFrame(transformed_rows).reset_index(drop=True)
    return repaired_df, True


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
    df_summary: pd.DataFrame = pd.DataFrame()
    if summary_fname.exists():
        df_summary = cast(pd.DataFrame, pd.read_csv(summary_fname))

    if "WORKOUT_ID" in df_heart_data.columns:
        df_heart_data = df_heart_data[df_heart_data["WORKOUT_ID"] == int(workout_id)]
    if "WORKOUT_ID" in df_segment_data.columns:
        df_segment_data = df_segment_data[df_segment_data["WORKOUT_ID"] == int(workout_id)]
    if not df_summary.empty and "WORKOUT_ID" in df_summary.columns:
        workout_id_int = int(workout_id)
        df_summary = cast(pd.DataFrame, df_summary.query("WORKOUT_ID == @workout_id_int"))

    print(f"Processing {len(df_segment_data)} segments...")
    df_segment_data = df_segment_data.groupby("SEGMENT").first().reset_index(drop=True)
    if "SEGMENT_INDEX" in df_segment_data.columns:
        sorted_idx = df_segment_data["SEGMENT_INDEX"].astype(float).sort_values().index
        df_segment_data = df_segment_data.loc[sorted_idx].reset_index(drop=True)
    df_segment_data = cast(pd.DataFrame, df_segment_data)

    df_segment_data, fixed_collapsed_start = fix_collapsed_start_lengths(
        df_segment_data, pool_length
    )
    if fixed_collapsed_start:
        print(
            "Detected collapsed start (first 75m logged as one length); "
            "split into 3 lengths."
        )

    workout_total_time_seconds = get_summary_total_time_seconds(cast(pd.DataFrame, df_summary))
    is_sprint_session = detect_sprint_session(df_segment_data, workout_total_time_seconds)
    if is_sprint_session:
        print(
            "Detected sprint swim session; splitting boundary long lengths into "
            "swim + rest and inserting fallback rest where needed..."
        )
        df_segment_data = add_sprint_rest_segments(df_segment_data)

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

        length.length_type = 1 if row["DISTANCE"] > 0 else 0  # type: ignore[assignment]
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
