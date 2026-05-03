import requests
import json
import sqlite3
import os
import time
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

def load_config():
    strava_config = {}
    config_file = Path('strava_config.json')
    if config_file.exists():
        with open(config_file, 'r') as f:
            strava_config = json.load(f)
    
    # Environment variable overrides
    strava_config['client_id'] = os.getenv('STRAVA_CLIENT_ID', strava_config.get('client_id', ''))
    strava_config['client_secret'] = os.getenv('STRAVA_CLIENT_SECRET', strava_config.get('client_secret', ''))
    strava_config['access_token'] = os.getenv('STRAVA_ACCESS_TOKEN', strava_config.get('access_token', ''))
    strava_config['refresh_token'] = os.getenv('STRAVA_REFRESH_TOKEN', strava_config.get('refresh_token', ''))
    
    return strava_config

def load_db_path():
    sync_config_file = Path('file_config.json')
    if not sync_config_file.exists():
        sync_config_file = Path(r'huawei_sync\file_config.json')

    if sync_config_file.exists():
        with open(sync_config_file, 'r') as f:
            sync_config = json.load(f)
            sync_db_location = sync_config.get('sync_db_location', '')
            
            if not sync_db_location:
                return None
            
            path = Path(sync_db_location)
            if path.suffix.lower() == '.db':
                return path
            return path / 'workout_sync.db'
    
    # Fallback to local file if config doesn't exist
    return Path('workout_sync.db')

def refresh_token(config):
    print("Refreshing Strava access token...")
    try:
        response = requests.post(
            'https://www.strava.com/oauth/token',
            data={
                'client_id': config['client_id'],
                'client_secret': config['client_secret'],
                'refresh_token': config['refresh_token'],
                'grant_type': 'refresh_token'
            }
        )
        response.raise_for_status()
        token_data = response.json()
        
        config['access_token'] = token_data['access_token']
        config['refresh_token'] = token_data['refresh_token']
        
        # Save back to config file
        config_file = Path('strava_config.json')
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
            
        print("Token refreshed successfully.")
        return True
    except Exception as e:
        print(f"Failed to refresh token: {e}")
        return False

def delete_strava_activity(activity_id, config):
    headers = {'Authorization': f"Bearer {config['access_token']}"}
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    
    try:
        response = requests.delete(url, headers=headers)
        if response.status_code == 204:
            print(f"Successfully deleted activity {activity_id}")
            return True
        elif response.status_code == 404:
            print(f"Activity {activity_id} not found on Strava (already deleted?)")
            return True
        elif response.status_code == 401:
            print("Access token expired.")
            if refresh_token(config):
                # Retry once
                headers = {'Authorization': f"Bearer {config['access_token']}"}
                response = requests.delete(url, headers=headers)
                if response.status_code == 204 or response.status_code == 404:
                    return True
        
        print(f"Failed to delete activity {activity_id}: {response.status_code} {response.text}")
        return False
    except Exception as e:
        print(f"Error deleting activity {activity_id}: {e}")
        return False

def reset_db_status(connection, workout_id):
    try:
        connection.execute(
            """
            UPDATE workouts 
            SET strava_synced = 0, 
                strava_activity_id = NULL, 
                strava_activity_url = NULL 
            WHERE workout_id = ?
            """,
            (workout_id,)
        )
        connection.commit()
        return True
    except Exception as e:
        print(f"Failed to update database for workout {workout_id}: {e}")
        return False

def main():
    config = load_config()
    if not config.get('access_token'):
        print("Error: Strava access token not found in strava_config.json or environment.")
        return

    db_path = load_db_path()
    if not db_path or not db_path.exists():
        print(f"Error: Sync database not found at {db_path}")
        return

    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    
    # Find synced workouts, ordered from last to first
    cursor = conn.execute(
        "SELECT workout_id, workout_type, workout_date, strava_activity_id, strava_activity_url FROM workouts WHERE strava_synced = 1 AND strava_activity_id IS NOT NULL ORDER BY workout_id DESC"
    )
    workouts = cursor.fetchall()
    
    if not workouts:
        print("No synced workouts found in the database.")
        conn.close()
        return

    print(f"Found {len(workouts)} synced workouts.")
    
    confirm_all = False
    
    for workout in workouts:
        workout_id, w_type, w_date, activity_id, url = workout
        print(f"\nWorkout: {w_type} on {w_date}")
        print(f"Strava ID: {activity_id}")
        print(f"URL: {url}")
        
        if not confirm_all:
            choice = input(f"Delete this activity? [y]es / [n]o / [a]ll / [q]uit: ").lower()
            if choice == 'q':
                break
            if choice == 'n':
                continue
            if choice == 'a':
                confirm_all = True
        
        if delete_strava_activity(activity_id, config):
            if reset_db_status(conn, workout_id):
                print(f"Database updated for workout {workout_id}")
            
            # Rate limiting safety
            time.sleep(0.5)
            
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
