import pandas as pd

# READ AND PREPARE DATA

workout_id = 25

raw_heart_fname = f"raw/{workout_id}_heart_swimming.csv"
raw_segment_fname = f"raw/{workout_id}_segments_swimming.csv"
df_heart_data = pd.read_csv(raw_heart_fname)
df_segment_data = pd.read_csv(raw_segment_fname)






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

# CONVERT TO STRAVA .FIT FILE

builder = FitFileBuilder(auto_define=True)
