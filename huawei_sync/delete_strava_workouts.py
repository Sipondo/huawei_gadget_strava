import requests
import json
import sqlite3
import os
import time
import msvcrt
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
    
    # Environment variable overrides with sanitization
    def get_val(key, default):
        val = os.getenv(key)
        if val is not None and val.strip():
            return val.strip()
        return str(default).strip() if default else ''

    config = {
        'client_id': get_val('STRAVA_CLIENT_ID', strava_config.get('client_id', '')),
        'client_secret': get_val('STRAVA_CLIENT_SECRET', strava_config.get('client_secret', '')),
        'access_token': get_val('STRAVA_ACCESS_TOKEN', strava_config.get('access_token', '')),
        'refresh_token': get_val('STRAVA_REFRESH_TOKEN', strava_config.get('refresh_token', '')),
        'scope': strava_config.get('scope', '')
    }
    return config

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
    print("\n[Auth] Refreshing Strava access token...")
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
        if response.status_code != 200:
            print(f"[Auth] Token refresh failed: {response.status_code}")
            return False
            
        token_data = response.json()
        config['access_token'] = token_data['access_token']
        config['refresh_token'] = token_data['refresh_token']
        
        # Save back to config file
        config_file = Path('strava_config.json')
        existing_config = {}
        if config_file.exists():
            with open(config_file, 'r') as f:
                existing_config = json.load(f)
        
        existing_config.update({
            'access_token': config['access_token'],
            'refresh_token': config['refresh_token'],
            'expires_at': token_data.get('expires_at')
        })
        
        with open(config_file, 'w') as f:
            json.dump(existing_config, f, indent=4)
            
        print("[Auth] Token refreshed successfully.")
        return True
    except Exception as e:
        print(f"[Auth] Error: {e}")
        return False

def check_activity_exists(activity_id, config):
    """Check if activity exists via API. Returns True if exists, False if 404 (deleted)."""
    headers = {'Authorization': f"Bearer {config['access_token']}"}
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        elif response.status_code == 401:
            if refresh_token(config):
                return check_activity_exists(activity_id, config)
        return True # Default to True on errors to avoid false positives for deletion
    except Exception:
        return True

def reset_db_status(connection, workout_id):
    try:
        connection.execute(
            "UPDATE workouts SET strava_synced = 0, strava_activity_id = NULL, strava_activity_url = NULL WHERE workout_id = ?",
            (workout_id,)
        )
        connection.commit()
        return True
    except Exception as e:
        print(f"Failed to update database: {e}")
        return False

def main():
    config = load_config()
    if not config.get('access_token'):
        print("Error: Access token missing. Run get_strava_token.py first.")
        return

    db_path = load_db_path()
    if not db_path or not db_path.exists():
        print(f"Error: Sync database not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.execute(
        "SELECT workout_id, workout_type, workout_date, strava_activity_id, strava_activity_url FROM workouts WHERE strava_synced = 1 AND strava_activity_id IS NOT NULL ORDER BY workout_id DESC"
    )
    workouts = cursor.fetchall()
    
    if not workouts:
        print("No synced workouts found.")
        conn.close()
        return

    print(f"Found {len(workouts)} synced workouts.")
    print("\nInstructions:")
    print("1. Open Strava in your browser.")
    print("2. The script will look at activities one by one.")
    print("3. Manualy delete the activity in your browser.")
    print("4. The script polls every 10s and updates your DB once deleted.")
    print("   Press 's' to skip current activity, 'q' to quit.")

    for workout in workouts:
        workout_id, w_type, w_date, activity_id, url = workout
        print(f"\n" + "="*60)
        print(f"WAITING FOR DELETION: {w_type} on {w_date}")
        print(f"Activity ID: {activity_id}")
        print(f"URL: {url}")
        
        quit_script = False
        while True:
            exists = check_activity_exists(activity_id, config)
            if not exists:
                print(f"\nConfirmed! Activity {activity_id} is gone.")
                if reset_db_status(conn, workout_id):
                    print(f"Database updated for workout {workout_id}")
                break
            
            # Non-blocking wait with keypress check
            print(".", end="", flush=True)
            start_wait = time.time()
            skip_activity = False
            
            while time.time() - start_wait < 10:
                if msvcrt.kbhit():
                    ch = msvcrt.getch().lower()
                    if ch == b's':
                        skip_activity = True
                        print("\nSkipping...")
                        break
                    if ch == b'q':
                        quit_script = True
                        print("\nQuitting...")
                        break
                time.sleep(0.1)
            
            if skip_activity or quit_script:
                break
        
        if quit_script:
            break
            
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
