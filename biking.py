import pandas as pd
from sqlalchemy import create_engine

SPEED = 34

# path = r"C:\Users\tt_ro\Nextcloud\Gadgetbridge"
path = r"C:\Users\Ties Robroek\Nextcloud\Gadgetbridge"
db_path = f"{path}\Gadgetbridge.db"
engine = create_engine(f"sqlite:///{db_path}")
df_summary = pd.read_sql_table("HUAWEI_WORKOUT_SUMMARY_SAMPLE", engine)


ds_summary = df_summary.iloc[-1]

df_data = pd.read_sql_query(
    f"SELECT * FROM HUAWEI_WORKOUT_DATA_SAMPLE WHERE WORKOUT_ID = {ds_summary['WORKOUT_ID']}",
    engine,
)
df_data = df_data[["TIMESTAMP", "HEART_RATE"]]
df_data.loc[df_data["HEART_RATE"] < 0, "HEART_RATE"] += 255
df_data = df_data[df_data["HEART_RATE"] != 0]

total_duration = ds_summary["END_TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
total_calories = ds_summary["CALORIES"]
calories_per_second = total_calories / total_duration

df_data["CALORIES"] = (
    df_data["TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
) * calories_per_second

df_data


# ---
from fit_tool.profile.messages.session_message import SessionMessage, SessionSportField
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
from fit_tool.profile.messages.activity_message import ActivityMessage
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

for index, row in df_data.iterrows():
    message = RecordMessage()
    message.timestamp = int(row["TIMESTAMP"]) * 1000
    message.heart_rate = int(row["HEART_RATE"])
    message.calories = int(row["CALORIES"])

    # Strava requires this
    message.speed = SPEED
    message.distance = int(
        (row["TIMESTAMP"] - ds_summary["START_TIMESTAMP"]) * SPEED / 3.6
    )

    builder.add(message)

message = SessionMessage()
message.timestamp = int(ds_summary["END_TIMESTAMP"] * 1000)
message.start_time = int(ds_summary["START_TIMESTAMP"] * 1000)
message.total_elapsed_time = int(
    ds_summary["END_TIMESTAMP"] - ds_summary["START_TIMESTAMP"]
)
message.total_calories = ds_summary["CALORIES"]
message.total_distance = int(
    (ds_summary["END_TIMESTAMP"] - ds_summary["START_TIMESTAMP"]) * SPEED / 3.6
)
message.sport = 2
message.sub_sport = 6
builder.add(message)

message = ActivityMessage()
message.timestamp = ds_summary["END_TIMESTAMP"] * 1000
message.num_sessions = 1
builder.add(message)

modified_file = builder.build()
modified_file.to_file("output/bike_output.fit")
modified_file.to_csv("output/bike_output.csv")
