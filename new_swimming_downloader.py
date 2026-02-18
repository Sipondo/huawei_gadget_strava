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


for workout_id in df_workout_ids["WORKOUT_ID"]:
    print("WORKOUT_ID: ", workout_id)

    ds_summary = df_summary.set_index("WORKOUT_ID").loc[workout_id]

    df_heart_data = pd.read_sql_query(
        f"SELECT * FROM HUAWEI_WORKOUT_DATA_SAMPLE WHERE WORKOUT_ID = {workout_id}",
        engine,
    )

    df_segment_data = pd.read_sql_query(
        f"SELECT * FROM HUAWEI_WORKOUT_SWIM_SEGMENTS_SAMPLE WHERE WORKOUT_ID = {workout_id} AND TYPE = 0",
        engine,
    ).groupby("SEGMENT").first().reset_index(drop=True)

    raw_heart_fname = f"raw/{workout_id}_heart_swimming.csv"
    raw_segment_fname = f"raw/{workout_id}_segments_swimming.csv"
    df_heart_data.to_csv(raw_heart_fname, index=False)
    df_segment_data.to_csv(raw_segment_fname, index=False)
