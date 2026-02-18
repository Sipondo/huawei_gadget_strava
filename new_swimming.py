import pandas as pd
from datetime import datetime
from fit_tool.profile.messages.session_message import SessionMessage, SessionSportField
from fit_tool.profile.messages.file_id_message import FileIdMessage, FileIdTypeField
from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.length_message import LengthMessage, LengthLengthTypeField
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.record_message import (
    RecordMessage,
    RecordHeartRateField,
)
from fit_tool.profile.profile_type import Sport, SubSport, SwimStroke, Event, EventType

# ============================================================================
# READ AND PREPARE DATA
# ============================================================================

workout_id = 402
pool_length = 25  # meters

raw_heart_fname = f"raw/{workout_id}_heart_swimming.csv"
raw_segment_fname = f"raw/{workout_id}_segments_swimming.csv"
output_fname = f"output/{workout_id}_swimming.fit"

print(f"Reading data files...")
df_heart_data = pd.read_csv(raw_heart_fname)
df_segment_data = pd.read_csv(raw_segment_fname)

# ============================================================================
# CLEAN AND PROCESS DATA
# ============================================================================

print(f"Processing {len(df_segment_data)} segments...")

# Sort segments by SEGMENT_INDEX to get chronological order
df_segment_data = df_segment_data.sort_values('SEGMENT_INDEX').reset_index(drop=True)

# Fix negative heart rates (take absolute value)
df_heart_data['HEART_RATE'] = df_heart_data['HEART_RATE'].abs()

# Convert speed from dm/s to m/s
df_heart_data['SPEED_MS'] = df_heart_data['SPEED'] / 10.0

# Get start timestamp from first heart rate record
start_timestamp = df_heart_data['TIMESTAMP'].min()

# Calculate actual timestamps for each segment based on cumulative time
cumulative_time = 0
segment_start_times = []
segment_end_times = []

for idx, row in df_segment_data.iterrows():
    segment_start_times.append(start_timestamp + cumulative_time)
    cumulative_time += row['TIME']
    segment_end_times.append(start_timestamp + cumulative_time)

df_segment_data['START_TIMESTAMP'] = segment_start_times
df_segment_data['END_TIMESTAMP'] = segment_end_times

# Calculate session statistics
total_time = df_segment_data['TIME'].sum()
total_distance = df_segment_data['DISTANCE'].sum()
total_strokes = df_segment_data['STROKES'].sum()
avg_speed = total_distance / total_time if total_time > 0 else 0
max_speed = 0

# Calculate max speed from segments
for idx, row in df_segment_data.iterrows():
    if row['TIME'] > 0:
        segment_speed = row['DISTANCE'] / row['TIME']
        max_speed = max(max_speed, segment_speed)

# Heart rate statistics
avg_heart_rate = int(df_heart_data['HEART_RATE'].mean())
max_heart_rate = int(df_heart_data['HEART_RATE'].max())

# Estimate calories (rough approximation)
calories = int(total_time / 60 * 8)  # ~8 cal/min for swimming

print(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
print(f"Total distance: {total_distance}m")
print(f"Total strokes: {total_strokes}")
print(f"Avg speed: {avg_speed:.2f} m/s")
print(f"Avg HR: {avg_heart_rate} bpm, Max HR: {max_heart_rate} bpm")

# ============================================================================
# BUILD FIT FILE
# ============================================================================

print(f"Building FIT file...")

builder = FitFileBuilder(auto_define=True)

# File ID Message
file_id = FileIdMessage()
file_id.type = 4 # FileIdTypeField.ACTIVITY
file_id.manufacturer = 1  # Garmin (generic)
file_id.product = 0
file_id.serial_number = 12345
file_id.time_created = int(start_timestamp * 1000)  # Convert to milliseconds
builder.add(file_id)

# Session Message (overall workout summary)
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
session.total_cycles = total_strokes  # Total strokes
session.enhanced_avg_speed = avg_speed
session.enhanced_max_speed = max_speed
session.event = Event.SESSION
session.event_type = EventType.STOP
builder.add(session)

# Lap Message (single lap for entire session)
lap = LapMessage()
lap.timestamp = int((start_timestamp + total_time) * 1000)
lap.start_time = int(start_timestamp * 1000)
lap.total_elapsed_time = total_time
lap.total_timer_time = total_time
lap.total_distance = total_distance
lap.total_calories = calories
lap.avg_heart_rate = avg_heart_rate
lap.max_heart_rate = max_heart_rate
lap.total_cycles = total_strokes
lap.enhanced_avg_speed = avg_speed
lap.enhanced_max_speed = max_speed
lap.event = Event.LAP
lap.event_type = EventType.STOP
builder.add(lap)

# Add Length Messages (one per pool length)
for idx, row in df_segment_data.iterrows():
    length = LengthMessage()
    length.timestamp = int(row['END_TIMESTAMP'] * 1000)
    length.start_time = int(row['START_TIMESTAMP'] * 1000)
    length.total_elapsed_time = row['TIME']
    length.total_timer_time = row['TIME']
    length.total_strokes = int(row['STROKES'])
    
    # Calculate speed for this length
    length_speed = row['DISTANCE'] / row['TIME'] if row['TIME'] > 0 else 0
    length.avg_speed = length_speed
    
    # Swim stroke - 2 is freestyle
    length.swim_stroke = SwimStroke.FREESTYLE if row['SWIM_TYPE'] == 2 else SwimStroke.FREESTYLE
    
    # Calculate swimming cadence (strokes per minute)
    if row['TIME'] > 0:
        length.avg_swimming_cadence = int((row['STROKES'] / row['TIME']) * 60)
    else:
        length.avg_swimming_cadence = 0
    
    length.length_type = 1 #LengthLengthTypeField.ACTIVE
    length.event = Event.LENGTH
    length.event_type = EventType.STOP
    length.message_index = idx
    
    builder.add(length)

# Add Record Messages (heart rate data points)
print(f"Adding {len(df_heart_data)} heart rate records...")
for idx, row in df_heart_data.iterrows():
    # Only add records within the workout timeframe
    if row['TIMESTAMP'] >= start_timestamp and row['TIMESTAMP'] <= (start_timestamp + total_time):
        record = RecordMessage()
        record.timestamp = int(row['TIMESTAMP'] * 1000)
        record.heart_rate = int(row['HEART_RATE'])
        
        # Add speed if available
        if row['SPEED_MS'] > 0:
            record.speed = row['SPEED_MS']
        
        builder.add(record)

# Activity Message (end of activity)
activity = ActivityMessage()
activity.timestamp = int((start_timestamp + total_time) * 1000)
activity.total_timer_time = total_time
activity.num_sessions = 1
activity.type = Event.ACTIVITY
activity.event = Event.ACTIVITY
activity.event_type = EventType.STOP
builder.add(activity)

# ============================================================================
# WRITE FIT FILE
# ============================================================================

print(f"Writing FIT file to {output_fname}...")
fit_file = builder.build()

# with open(output_fname, 'wb') as f:
#     fit_file.to_bytes(f)
fit_file.to_file(output_fname)

print(f"✓ Successfully created {output_fname}")
print(f"  - Duration: {total_time/60:.1f} minutes")
print(f"  - Distance: {total_distance}m ({len(df_segment_data)} lengths)")
print(f"  - Avg pace: {(total_time/60)/(total_distance/1000):.2f} min/km")
print(f"  - Calories: {calories} kcal")
