import pandas as pd
from sqlalchemy import create_engine

SPEED = 34

path = r"C:\Users\tt_ro\Nextcloud\Gadgetbridge\db"
# path = r"C:\Users\Ties Robroek\Nextcloud\Gadgetbridge\db"
db_path = f"{path}\Gadgetbridge.db"
engine = create_engine(f"sqlite:///{db_path}")
df_summary = pd.read_sql_table("HUAWEI_WORKOUT_SUMMARY_SAMPLE", engine)


df_workout_ids = pd.read_sql_query(
    "SELECT DISTINCT WORKOUT_ID FROM HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE",
    engine,
)


workout_id = int(df_workout_ids.iloc[-1, 0])

ds_summary = df_summary.set_index("WORKOUT_ID").loc[workout_id]

df_heart_data = pd.read_sql_query(
    f"SELECT * FROM HUAWEI_WORKOUT_DATA_SAMPLE WHERE WORKOUT_ID = {workout_id}",
    engine,
)

df_segment_data = pd.read_sql_query(
    f"SELECT * FROM HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE WHERE WORKOUT_ID = {workout_id} AND TYPE = 0",
    engine,
).groupby("SEGMENT").first()
# TODO: figure out what the type = 1 data means

df_heart_data = df_heart_data[["TIMESTAMP", "HEART_RATE"]]
df_heart_data.loc[df_heart_data["HEART_RATE"] < 0, "HEART_RATE"] += 255
df_heart_data = df_heart_data[df_heart_data["HEART_RATE"] != 0]

total_duration = ds_summary["END_TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
total_calories = ds_summary["CALORIES"]
calories_per_second = total_calories / total_duration

df_heart_data["CALORIES"] = (
    df_heart_data["TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
) * calories_per_second




# ---
from fit_tool.profile.messages.session_message import SessionMessage, SessionSportField
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.length_message import LengthMessage
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

start_time = int(ds_summary["START_TIMESTAMP"] * 1000)
for index, row in df_segment_data.iterrows():
    message = LengthMessage()
    message.timestamp = int(ds_summary["START_TIMESTAMP"] * 1000)
    message.start_time = start_time
    start_time += int(row["TIME"]) * 1000
    message.total_elapsed_time = int(row["TIME"])
    message.total_timer_time = int(row["TIME"])
    message.message_index = int(index)
    message.total_strokes = int(row["STROKES"])
    message.avg_speed = row["DISTANCE"] / row["TIME"]                  
    message.event = 28
    message.event_type = 1  # 0 for active length
    message.swim_stroke = 0  # Freestyle
    message.avg_swim_cadence = int(row["STROKES"] / (row["TIME"] * 60))
    message.event_group = 255
    message.length_type = 1
    builder.add(message)


### Swimming end








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
modified_file.to_file("output/swimming_output.fit")
modified_file.to_csv("output/swimming_output.csv")
