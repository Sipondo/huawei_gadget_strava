import requests
import time
import os
import json
import sqlite3
from datetime import datetime
from pathlib import Path

# ============================================================================
# STRAVA API CONFIGURATION
# ============================================================================

# To get your Strava API credentials:
# 1. Go to https://www.strava.com/settings/api
# 2. Create an app if you haven't already
# 3. Note your Client ID and Client Secret
# 4. Get an access token using OAuth or from your app settings

# You can store these in environment variables or a config file
STRAVA_CLIENT_ID = os.getenv('STRAVA_CLIENT_ID', '')
STRAVA_CLIENT_SECRET = os.getenv('STRAVA_CLIENT_SECRET', '')
STRAVA_ACCESS_TOKEN = os.getenv('STRAVA_ACCESS_TOKEN', '')

# Or set them directly (not recommended for production):
# STRAVA_CLIENT_ID = 'your_client_id'
# STRAVA_CLIENT_SECRET = 'your_client_secret'
# STRAVA_ACCESS_TOKEN = 'your_access_token'

# Load from config file if it exists
config_file = Path('strava_config.json')
if config_file.exists():
    with open(config_file, 'r') as f:
        config = json.load(f)
        STRAVA_CLIENT_ID = config.get('client_id', STRAVA_CLIENT_ID)
        STRAVA_CLIENT_SECRET = config.get('client_secret', STRAVA_CLIENT_SECRET)
        STRAVA_ACCESS_TOKEN = config.get('access_token', STRAVA_ACCESS_TOKEN)

SYNC_DB_LOCATION = ""
sync_config_file = Path('file_config.json')
if not sync_config_file.exists():
    sync_config_file = Path(r'huawei_sync\file_config.json')

if sync_config_file.exists():
    with open(sync_config_file, 'r') as f:
        sync_config = json.load(f)
        SYNC_DB_LOCATION = sync_config.get('sync_db_location', '')


def resolve_sync_db_path(sync_db_location):
    if not sync_db_location:
        return None

    path = Path(sync_db_location)
    if path.suffix.lower() == '.db':
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    path.mkdir(parents=True, exist_ok=True)
    return path / 'workout_sync.db'


def parse_workout_id_from_path(file_path):
    stem = Path(file_path).stem
    workout_prefix = stem.split('_')[0]
    return int(workout_prefix) if workout_prefix.isdigit() else None


def infer_activity_type(file_path):
    stem = Path(file_path).stem.lower()

    if stem.endswith("_swimming"):
        return "Swim"
    if stem.endswith("_cycling"):
        return "Ride"
    if stem.endswith("_indoor_cycling"):
        return "Ride"
    if stem.endswith("_indoor_running"):
        return "Run"
    if stem.endswith("_strength"):
        return "WeightTraining"

    return "Workout"


def infer_activity_type_from_workout_type(workout_type):
    if workout_type == "swimming":
        return "Swim"
    if workout_type == "cycling":
        return "Ride"
    if workout_type == "indoor_cycling":
        return "Ride"
    if workout_type == "indoor_running":
        return "Run"
    if workout_type == "strength":
        return "WeightTraining"
    return "Workout"


def day_period_from_iso(iso_value):
    if not iso_value:
        return "workout"

    try:
        parsed = datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
        hour = parsed.hour
        if 5 <= hour < 12:
            return "morning"
        if 12 <= hour < 17:
            return "afternoon"
        if 17 <= hour < 22:
            return "evening"
        return "night"
    except Exception:
        return "workout"


def workout_label(workout_type):
    mapping = {
        "swimming": "swim",
        "cycling": "ride",
        "indoor_running": "run",
        "strength": "strength session",
    }
    return mapping.get(workout_type, "workout")


def build_activity_name(workout_row):
    period = day_period_from_iso(workout_row.get("workout_date"))
    label = workout_label(workout_row.get("workout_type"))
    return f"{period.capitalize()} {label}".capitalize()


def build_activity_description(workout_row):
    details = []

    duration = workout_row.get("duration_seconds")
    if duration is not None:
        details.append(f"Duration: {float(duration)/60:.1f} min")

    distance = workout_row.get("total_distance_m")
    if distance is not None and float(distance) > 0:
        details.append(f"Distance: {float(distance)/1000:.2f} km")

    calories = workout_row.get("total_calories")
    if calories is not None:
        details.append(f"Calories: {int(calories)}")

    workout_id = workout_row.get("workout_id")
    if workout_id is not None:
        details.append(f"Workout ID: {int(workout_id)}")

    details.append("Synced with my custom syncing solution :)")
    return " | ".join(details)


def should_upload_private(workout_row):
    workout_type = workout_row.get("workout_type")
    has_gps = int(workout_row.get("has_gps") or 0)
    if workout_type == "indoor_cycling":
        return True
    return workout_type == "cycling" and has_gps == 0


def fetch_unsynced_workouts(connection):
    cursor = connection.execute(
        """
        SELECT
            workout_id,
            workout_type,
            workout_date,
            duration_seconds,
            total_distance_m,
            total_calories,
            has_gps,
            fit_file_path
        FROM workouts
        WHERE strava_synced = 0
        ORDER BY workout_id ASC
        """
    )

    rows = []
    columns = [column[0] for column in cursor.description]
    for values in cursor.fetchall():
        row = dict(zip(columns, values))
        fit_file = row.get("fit_file_path")
        if fit_file and Path(fit_file).exists():
            rows.append(row)
    return rows


def update_sync_status(file_path, upload_result):
    sync_db_path = resolve_sync_db_path(SYNC_DB_LOCATION)
    if sync_db_path is None:
        return

    workout_id = parse_workout_id_from_path(file_path)
    if workout_id is None:
        return

    connection = sqlite3.connect(sync_db_path)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS workouts (
            workout_id INTEGER PRIMARY KEY,
            workout_number INTEGER,
            workout_type TEXT NOT NULL,
            workout_date TEXT,
            duration_seconds REAL,
            total_distance_m REAL,
            total_calories INTEGER,
            has_gps INTEGER NOT NULL DEFAULT 0,
            source_workout_dir TEXT NOT NULL,
            fit_file_path TEXT,
            fit_generated_at TEXT,
            last_analyzed_at TEXT NOT NULL,
            strava_synced INTEGER NOT NULL DEFAULT 0,
            strava_activity_id INTEGER,
            strava_activity_url TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    connection.execute(
        """
        UPDATE workouts
        SET strava_synced = 1,
            strava_activity_id = ?,
            strava_activity_url = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE workout_id = ?
        """,
        (
            upload_result.get('activity_id'),
            upload_result.get('url'),
            workout_id,
        ),
    )
    connection.commit()
    connection.close()

# ============================================================================
# UPLOAD FUNCTIONS
# ============================================================================

def check_access_token():
    """Verify that we have a valid access token"""
    if not STRAVA_ACCESS_TOKEN:
        print("\nERROR: No Strava access token found!")
        print("\nTo upload to Strava, you need to:")
        print("1. Create a Strava API app at: https://www.strava.com/settings/api")
        print("2. Get your access token")
        print("3. Either:")
        print("   a) Set environment variable: STRAVA_ACCESS_TOKEN")
        print("   b) Create strava_config.json with your credentials:")
        print('      {')
        print('        "client_id": "your_client_id",')
        print('        "client_secret": "your_client_secret",')
        print('        "access_token": "your_access_token"')
        print('      }')
        return False
    return True

def upload_to_strava(file_path, activity_name=None, activity_type=None, description=None, is_private=False):
    """
    Upload a FIT file to Strava
    
    Args:
        file_path: Path to the .fit file
        activity_name: Optional name for the activity
        activity_type: Type of activity (if None, inferred from file name)
        description: Optional description
    
    Returns:
        Dictionary with upload result and activity ID
    """
    
    if not check_access_token():
        return None

    resolved_activity_type = activity_type or infer_activity_type(file_path)
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return None
    
    print(f"\nUploading {file_path} to Strava...")
    print(f"Activity type: {resolved_activity_type}")
    
    # Strava upload endpoint
    upload_url = 'https://www.strava.com/api/v3/uploads'
    
    # Prepare the request
    headers = {
        'Authorization': f'Bearer {STRAVA_ACCESS_TOKEN}'
    }
    
    # Read the file
    response = None

    with open(file_path, 'rb') as f:
        files = {
            'file': (os.path.basename(file_path), f, 'application/octet-stream')
        }
        
        data = {
            'data_type': 'fit',
            'activity_type': resolved_activity_type,
            'private': 1 if is_private else 0
        }
        
        if activity_name:
            data['name'] = activity_name
        if description:
            data['description'] = description
        
        # Make the upload request
        try:
            response = requests.post(upload_url, headers=headers, files=files, data=data)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Upload failed: {e}")
            if response is not None:
                print(f"Response: {response.text}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Network error: {e}")
            return None
    
    upload_data = response.json()
    upload_id = upload_data.get('id')
    
    if not upload_id:
        print(f"Upload failed: {upload_data}")
        return None
    
    print(f"Upload started (ID: {upload_id})")
    print(f"Waiting for Strava to process the file...")
    
    # Poll for upload status
    status_url = f'https://www.strava.com/api/v3/uploads/{upload_id}'
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        time.sleep(2)  # Wait 2 seconds between checks
        attempt += 1
        
        try:
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            status_data = status_response.json()
            
            activity_id = status_data.get('activity_id')
            error = status_data.get('error')
            status = status_data.get('status')
            
            if error:
                print(f"Upload error: {error}")
                return None
            
            if activity_id:
                print(f"\nUpload successful!")
                print(f"Activity ID: {activity_id}")
                print(f"View at: https://www.strava.com/activities/{activity_id}")
                result = {
                    'upload_id': upload_id,
                    'activity_id': activity_id,
                    'url': f'https://www.strava.com/activities/{activity_id}'
                }
                update_sync_status(file_path, result)
                return result
            
            print(f"   Status: {status} (attempt {attempt}/{max_attempts})")
            
        except requests.exceptions.RequestException as e:
            print(f"Error checking status: {e}")
            return None
    
    print(f"Upload timed out. Check Strava manually: https://www.strava.com/")
    return None

def upload_multiple_files(file_pattern, activity_type=None):
    """
    Upload multiple FIT files matching a pattern
    
    Args:
        file_pattern: Glob pattern for files (e.g., "output/*.fit")
        activity_type: Type of activity override for all files (None = infer per file)
    """
    from glob import glob
    
    files = glob(file_pattern)
    
    if not files:
        print(f"No files found matching: {file_pattern}")
        return
    
    print(f"\nFound {len(files)} file(s) to upload")
    
    results = []
    for file_path in files:
        result = upload_to_strava(file_path, activity_type=activity_type)
        results.append({
            'file': file_path,
            'result': result
        })
        
        # Wait between uploads to avoid rate limiting
        if len(files) > 1:
            time.sleep(3)
    
    # Summary
    print("\n" + "="*60)
    print("UPLOAD SUMMARY")
    print("="*60)
    successful = sum(1 for r in results if r['result'])
    print(f"Successful: {successful}/{len(results)}")
    
    for r in results:
        status = "[OK]" if r['result'] else "[FAIL]"
        print(f"{status} {r['file']}")
        if r['result']:
            print(f"   -> {r['result']['url']}")
    
    return results


def upload_pending_from_db(activity_type_override=None):
    sync_db_path = resolve_sync_db_path(SYNC_DB_LOCATION)
    if sync_db_path is None or not sync_db_path.exists():
        print("Sync DB not found. Run analyze first to populate workouts.")
        return []

    connection = sqlite3.connect(sync_db_path)
    pending = fetch_unsynced_workouts(connection)
    connection.close()

    if not pending:
        print("No unsynced workouts found in DB.")
        return []

    print(f"Found {len(pending)} unsynced workout(s) in DB")
    results = []

    for workout_row in pending:
        file_path = workout_row["fit_file_path"]
        resolved_type = (
            activity_type_override
            or infer_activity_type_from_workout_type(workout_row.get("workout_type"))
        )
        is_private = should_upload_private(workout_row)
        title = build_activity_name(workout_row)
        description = build_activity_description(workout_row)

        print(f"\nUploading workout_id={workout_row.get('workout_id')} from DB...")
        print(f"Title: {title}")
        print(f"Visibility: {'private' if is_private else 'public'}")

        result = upload_to_strava(
            file_path,
            activity_name=title,
            activity_type=resolved_type,
            description=description,
            is_private=is_private,
        )
        results.append({"file": file_path, "result": result})

        if result:
            time.sleep(2)

    print("\n" + "="*60)
    print("DB UPLOAD SUMMARY")
    print("="*60)
    successful = sum(1 for item in results if item["result"])
    print(f"Successful: {successful}/{len(results)}")

    for item in results:
        status = "[OK]" if item["result"] else "[FAIL]"
        print(f"{status} {item['file']}")
        if item["result"]:
            print(f"   -> {item['result']['url']}")

    return results

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload FIT files to Strava')
    parser.add_argument('file', nargs='?', default='output/402_swimming.fit',
                        help='FIT file to upload (default: output/402_swimming.fit)')
    parser.add_argument('--name', help='Activity name')
    parser.add_argument('--description', help='Activity description')
    parser.add_argument('--type', default=None, help='Activity type override (default: infer from file name)')
    parser.add_argument('--multiple', help='Upload multiple files matching pattern (e.g., "output/*.fit")')
    parser.add_argument('--pending-db', action='store_true', help='Upload all unsynced workouts from sync DB')
    
    args = parser.parse_args()
    
    print("="*60)
    print("STRAVA FIT FILE UPLOADER")
    print("="*60)
    
    if args.pending_db:
        upload_pending_from_db(activity_type_override=args.type)
    elif args.multiple:
        upload_multiple_files(args.multiple, activity_type=args.type)
    else:
        upload_to_strava(
            args.file,
            activity_name=args.name,
            activity_type=args.type,
            description=args.description,
            is_private=False,
        )
