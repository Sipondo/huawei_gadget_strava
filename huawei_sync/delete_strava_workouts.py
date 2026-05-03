import sqlite3
import requests
import time
import json
import argparse
from pathlib import Path

# Load Strava configuration
STRAVA_ACCESS_TOKEN = ""
config_file = Path('strava_config.json')
if config_file.exists():
    with open(config_file, 'r') as f:
        config = json.load(f)
        STRAVA_ACCESS_TOKEN = config.get('access_token', "")

# Load File configuration
SYNC_DB_LOCATION = ""
sync_config_file = Path('file_config.json')
if sync_config_file.exists():
    with open(sync_config_file, 'r') as f:
        sync_config = json.load(f)
        SYNC_DB_LOCATION = sync_config.get('sync_db_location', '')

def resolve_sync_db_path(sync_db_location):
    if not sync_db_location:
        return None
    path = Path(sync_db_location)
    if path.suffix.lower() == '.db':
        return path
    return path / 'workout_sync.db'

def check_strava_activity(activity_id):
    """
    Check if an activity still exists on Strava.
    Returns:
        True if it exists
        False if it's 404 (deleted)
        None if there's an error (e.g., rate limiting)
    """
    if not STRAVA_ACCESS_TOKEN:
        print("Error: No Strava access token found.")
        return None

    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {'Authorization': f'Bearer {STRAVA_ACCESS_TOKEN}'}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        elif response.status_code == 429:
            print("Warning: Strava API rate limit exceeded.")
            return None
        else:
            print(f"Warning: Strava API returned status code {response.status_code}")
            return None
    except Exception as e:
        print(f"Error checking activity {activity_id}: {e}")
        return None

def run_delete_mode():
    sync_db_path = resolve_sync_db_path(SYNC_DB_LOCATION)
    if not sync_db_path or not sync_db_path.exists():
        print(f"Error: Sync database not found at {sync_db_path}")
        return

    print(f"Running Strava delete mode on {sync_db_path}...")
    connection = sqlite3.connect(sync_db_path)
    
    # Get all synced activities
    cursor = connection.execute(
        "SELECT workout_id, strava_activity_id, strava_activity_url FROM workouts WHERE strava_synced = 1 AND strava_activity_id IS NOT NULL"
    )
    synced_workouts = cursor.fetchall()
    
    if not synced_workouts:
        print("No synced workouts found to check.")
        connection.close()
        return

    print(f"Checking {len(synced_workouts)} synced workouts...")
    
    updates = []
    skipped = 0
    deleted_count = 0
    
    for workout_id, activity_id, url in synced_workouts:
        print(f"Checking workout {workout_id} (Strava ID: {activity_id})...", end=" ", flush=True)
        
        exists = check_strava_activity(activity_id)
        
        if exists is True:
            print("Present.")
            skipped += 1
        elif exists is False:
            print("DELETED.")
            updates.append(workout_id)
            deleted_count += 1
        else:
            print("ERROR/RATE-LIMIT. Stopping check.")
            break
        
        # Small delay to avoid hitting rate limits too fast
        time.sleep(1)

    if updates:
        print(f"\nUpdating {len(updates)} workouts in DB as no longer synced...")
        for workout_id in updates:
            connection.execute(
                "UPDATE workouts SET strava_synced = 0, strava_activity_id = NULL, strava_activity_url = NULL, updated_at = CURRENT_TIMESTAMP WHERE workout_id = ?",
                (workout_id,)
            )
        connection.commit()
        print("Database updated successfully.")
    
    print(f"\nSummary:")
    print(f"  - Total checked: {skipped + deleted_count}")
    print(f"  - Still present: {skipped}")
    print(f"  - Detected as deleted: {deleted_count}")
    
    connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Strava Delete Mode: Sync local DB with Strava deletions.")
    args = parser.parse_args()
    
    run_delete_mode()
