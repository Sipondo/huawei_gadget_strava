import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

SPEED = 34
TARGET_DISTANCE = 100

path = r"C:\Users\tt_ro\Nextcloud\Gadgetbridge\db"
# path = r"C:\Users\Ties Robroek\Nextcloud\Gadgetbridge\db"
db_path = f"{path}\Gadgetbridge.db"
engine = create_engine(f"sqlite:///{db_path}")
df_summary = pd.read_sql_table("HUAWEI_WORKOUT_SUMMARY_SAMPLE", engine)


df_workout_ids = pd.read_sql_query(
    "SELECT DISTINCT WORKOUT_ID FROM HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE",
    engine,
)


# for workout_id in df_workout_ids["WORKOUT_ID"]:
for workout_id in [25]:
    print("WORKOUT_ID: ", workout_id)
    filename = f"sessions/{workout_id}_swimming."
    filename_fit = filename + "fit"
    filename_csv = filename + "csv"

    if Path(filename_fit).exists():
        print(f"{filename_fit} exists")
        continue

    ds_summary = df_summary.set_index("WORKOUT_ID").loc[workout_id]

    df_heart_data = pd.read_sql_query(
        f"SELECT * FROM HUAWEI_WORKOUT_DATA_SAMPLE WHERE WORKOUT_ID = {workout_id}",
        engine,
    )

    df_segment_data = pd.read_sql_query(
        f"SELECT * FROM HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE WHERE WORKOUT_ID = {workout_id} AND TYPE = 0",
        engine,
    ).groupby("SEGMENT").first().reset_index(drop=True)





    ################## Beginning Segment Fix
    df_segment_data.drop(["SEGMENT_INDEX"], inplace=True, axis=1)
    # Fix missing segments
    if df_segment_data.loc[0, "TIME"] > 2 * df_segment_data.loc[1, "TIME"]:
        segment_time  = df_segment_data.loc[0, "TIME"] // 3
        df_segment_data.index += 2
        df_segment_data.loc[0] = df_segment_data.loc[2]
        df_segment_data.loc[1] = df_segment_data.loc[2]

        df_segment_data.sort_index(inplace=True)

        df_segment_data.loc[0, "TIME"] -= (segment_time * 2)
        df_segment_data.loc[1, "TIME"] = segment_time
        df_segment_data.loc[2, "TIME"] = segment_time


    def insert_row(df, index, row):
        """
        Inserts a row into a pandas DataFrame at the specified index,
        shifting subsequent rows down and resetting the index to be numerical and increasing.
        Args:
            df (pd.DataFrame): The DataFrame to insert into.
            index (int): The index at which to insert the row.
            row (dict or pd.Series): The row data to insert.
        Returns:
            pd.DataFrame: The DataFrame with the new row inserted.
        """
        df = pd.concat([
            df.iloc[:index],
            pd.DataFrame([row]),
            df.iloc[index:]
        ]).reset_index(drop=True)
        return df



    if df_segment_data["TIME"].std() > 8:
        # Probably a sprint session
        ################## Sprint Segment Fix
        # Add a pause after every 4 segments
        i = 0
        total_time = 0
        total_time_since = 0
        segments_since_last = 0
        while i < len(df_segment_data) - 1:
            print(i, len(df_segment_data))
            total_time += df_segment_data.loc[i, "TIME"]
            total_time_since += df_segment_data.loc[i, "TIME"]
            segments_since_last += 1
            if total_time_since > 180:
                if segments_since_last > 4:
                    pause_insert_loc = i - segments_since_last + 5
                    print("over", total_time_since)
                    df_segment_data = insert_row(
                        df_segment_data, pause_insert_loc, df_segment_data.loc[i].copy())
                    overtime = total_time_since - 180
                    df_segment_data.loc[pause_insert_loc, "TIME"] -= overtime
                    df_segment_data.loc[pause_insert_loc, "STROKES"] = 65535
                    df_segment_data.loc[pause_insert_loc, "DISTANCE"] = 0
                    df_segment_data.loc[i+1, "TIME"] = overtime
                    total_time_since = 0#df_segment_data.loc[i+1, "TIME"]
                    segments_since_last = 0
                else: 
                    # Swap
                    total_time -= df_segment_data.loc[i, "TIME"]
                    total_time_since -= df_segment_data.loc[i, "TIME"]

                    # Swap rows i and i+1
                    temp = df_segment_data.loc[i].copy()
                    df_segment_data.loc[i] = df_segment_data.loc[i+1]
                    df_segment_data.loc[i+1] = temp
                    segments_since_last -= 1
                    continue
                
            i += 1




    df_segment_data["AVG_SWOLF"] = df_segment_data["STROKES"] + df_segment_data["TIME"]
    df_segment_data["PACE"] = 4*df_segment_data["TIME"]

    pd.set_option('display.max_rows', 120)
    df_segment_data.head(120)

    # TODO: figure out what the type = 1 data means
    # TODO: add pool length as data to session message



    df_heart_data = df_heart_data[["TIMESTAMP", "HEART_RATE"]]
    df_heart_data.loc[df_heart_data["HEART_RATE"] < 0, "HEART_RATE"] += 255
    df_heart_data = df_heart_data[df_heart_data["HEART_RATE"] != 0]

    total_duration = ds_summary["END_TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
    total_calories = ds_summary["CALORIES"]
    calories_per_second = total_calories / total_duration

    df_heart_data["CALORIES"] = (
        df_heart_data["TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
    ) * calories_per_second

    # print(ds_summary)


    # ---
    from fit_tool.profile.messages.session_message import SessionMessage, SessionSportField
    from fit_tool.profile.messages.file_id_message import FileIdMessage
    from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
    from fit_tool.profile.messages.activity_message import ActivityMessage
    from fit_tool.profile.messages.length_message import LengthMessage
    from fit_tool.profile.messages.lap_message import LapMessage
    from fit_tool.fit_file_builder import FitFileBuilder
    from fit_tool.profile.messages.record_message import (
        RecordMessage,
        RecordHeartRateField,
        RecordPowerField,
    )

    builder = FitFileBuilder(auto_define=True)


    message = FileIdMessage()
    message.type = 4
    message.manufacturer = 255
    message.product = 1
    message.time_created = int(ds_summary["START_TIMESTAMP"] * 1000)
    message.serial_number = 0x1
    builder.add(message)

    message = DeviceInfoMessage()
    message.device_index = 0
    message.manufacturer = 255
    message.product = 1
    message.product_name = "Ties Huawei Strava Sync"
    message.serial_number = 0x1
    message.software_version = 1.0
    message.timestamp = int(ds_summary["START_TIMESTAMP"] * 1000)
    builder.add(message)

    ### Swimming data
    lap_time = 0
    lap_distance = 0

    laps = []

    start_time = int(ds_summary["START_TIMESTAMP"] * 1000)
    for index, row in df_segment_data.iterrows():
        message = LengthMessage()
        message.timestamp = int(ds_summary["START_TIMESTAMP"] * 1000)
        message.start_time = start_time # TODO: fix for the skippy ones!!!
        message.total_elapsed_time = int(row["TIME"])
        message.total_timer_time = int(row["TIME"])
        message.message_index = int(index)
        message.total_strokes = int(row["STROKES"])
        message.avg_speed = row["DISTANCE"] / row["TIME"]                  
        message.event = 28
        message.event_type = 1 if row["DISTANCE"] > 0 else 0  # 1 for active length
        message.swim_stroke = 0 if row["DISTANCE"] > 0 else 255  # 1 for active length
        message.avg_swim_cadence = int(row["STROKES"] / (row["TIME"] * 60))
        message.event_group = 255
        message.length_type = 1
        builder.add(message)
        
        if row["DISTANCE"] < 1:
            message = LapMessage()
            message.timestamp = int(ds_summary["START_TIMESTAMP"] * 1000)
            message.start_time = start_time
            message.total_elapsed_time = int(row["TIME"])
            message.total_distance = 0
            message.num_lengths = 0
            message.total_timer_time = int(row["TIME"])
            message.swim_stroke = 255


            laps.append(message)
        else:
            lap_time += int(row["TIME"])
            lap_distance += 25

        if lap_distance >= TARGET_DISTANCE:
            message = LapMessage()
            message.timestamp = int(ds_summary["START_TIMESTAMP"] * 1000)
            message.start_time = start_time
            message.total_elapsed_time = lap_time
            message.total_distance = lap_distance
            message.num_lengths = int(lap_distance // 25)
            message.total_timer_time = lap_time
            message.swim_stroke = 0
            lap_time = 0
            lap_distance = 0
            # total cycles
            # total work
            # total_moving_time,4,bytes,time_standing,4,bytes,avg_left_power_phase,4,bytes,avg_left_power_phase_peak,4,bytes,avg_right_power_phase,4,bytes,avg_right_power_phase_peak,4,bytes,avg_power_position,4,bytes,max_power_position,4,bytes,enhanced_avg_speed,4,bytes,enhanced_max_speed,4,bytes,enhanced_avg_altitude,4,bytes,enhanced_min_altitude,4,bytes,enhanced_max_altitude,4,bytes,message_index,2,bytes,total_calories,2,bytes,total_fat_calories,2,bytes,avg_speed,2,bytes,max_speed,2,bytes,avg_power,2,bytes,max_power,2,bytes,total_ascent,2,bytes,total_descent,2,bytes,num_lengths,2,bytes,normalized_power,2,bytes,left_right_balance,2,bytes,first_length_index,2,bytes,avg_stroke_distance,2,bytes,num_active_lengths,2,bytes,wkt_step_index,2,bytes,avg_vertical_oscillation,2,bytes,avg_stance_time_percent,2,bytes,avg_stance_time,2,bytes,stand_count,2,bytes,avg_vertical_ratio,2,bytes,avg_stance_time_balance,2,bytes,avg_step_length,2,bytes,event,1,bytes,event_type,1,bytes,avg_heart_rate,1,bytes,max_heart_rate,1,bytes,avg_cadence,1,bytes,max_cadence,1,bytes,intensity,1,bytes,lap_trigger,1,bytes,sport,1,bytes,event_group,1,bytes,swim_stroke,1,bytes,sub_sport,1,bytes,avg_temperature,1,bytes,max_temperature,1,bytes,avg_fractional_cadence,1,bytes,max_fractional_cadence,1,bytes,total_fractional_cycles,1,bytes,avg_left_torque_effectiveness,1,bytes,avg_right_torque_effectiveness,1,bytes,avg_left_pedal_smoothness,1,bytes,avg_right_pedal_smoothness,1,bytes,avg_combined_pedal_smoothness,1,bytes,avg_left_pco,1,bytes,avg_right_pco,1,bytes,avg_cadence_position,2,bytes,max_cadence_position,2,bytes
            laps.append(message)

        start_time += int(row["TIME"]) * 1000

    ### Swimming end



    builder.add_all(laps)




    for index, row in df_heart_data.iterrows():
        message = RecordMessage()
        message.timestamp = int(row["TIMESTAMP"]) * 1000
        message.heart_rate = int(row["HEART_RATE"])
        message.calories = int(row["CALORIES"])
        builder.add(message)

    message = SessionMessage()
    message.timestamp = int(ds_summary["END_TIMESTAMP"] * 1000)
    message.start_time = int(ds_summary["START_TIMESTAMP"] * 1000)
    message.total_elapsed_time = int(
        ds_summary["END_TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
    )
    message.total_calories = ds_summary["CALORIES"]
    message.total_distance = int(
        df_segment_data["DISTANCE"].sum()
    )
    message.sport = 5
    message.sub_sport = 17
    builder.add(message)

    message = ActivityMessage()
    message.timestamp = ds_summary["END_TIMESTAMP"] * 1000
    message.num_sessions = 1
    builder.add(message)

    modified_file = builder.build()
    modified_file.to_file(filename_fit)
    modified_file.to_csv(filename_csv)
