import requests
import time
import os
import json
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

def upload_to_strava(file_path, activity_name=None, activity_type="Swim", description=None):
    """
    Upload a FIT file to Strava
    
    Args:
        file_path: Path to the .fit file
        activity_name: Optional name for the activity
        activity_type: Type of activity (default: "Swim")
        description: Optional description
    
    Returns:
        Dictionary with upload result and activity ID
    """
    
    if not check_access_token():
        return None
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return None
    
    print(f"\nUploading {file_path} to Strava...")
    
    # Strava upload endpoint
    upload_url = 'https://www.strava.com/api/v3/uploads'
    
    # Prepare the request
    headers = {
        'Authorization': f'Bearer {STRAVA_ACCESS_TOKEN}'
    }
    
    # Read the file
    with open(file_path, 'rb') as f:
        files = {
            'file': (os.path.basename(file_path), f, 'application/octet-stream')
        }
        
        data = {
            'data_type': 'fit',
            'activity_type': activity_type
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
                return {
                    'upload_id': upload_id,
                    'activity_id': activity_id,
                    'url': f'https://www.strava.com/activities/{activity_id}'
                }
            
            print(f"   Status: {status} (attempt {attempt}/{max_attempts})")
            
        except requests.exceptions.RequestException as e:
            print(f"Error checking status: {e}")
            return None
    
    print(f"Upload timed out. Check Strava manually: https://www.strava.com/")
    return None

def upload_multiple_files(file_pattern, activity_type="Swim"):
    """
    Upload multiple FIT files matching a pattern
    
    Args:
        file_pattern: Glob pattern for files (e.g., "output/*.fit")
        activity_type: Type of activity
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
    parser.add_argument('--type', default='Swim', help='Activity type (default: Swim)')
    parser.add_argument('--multiple', help='Upload multiple files matching pattern (e.g., "output/*.fit")')
    
    args = parser.parse_args()
    
    print("="*60)
    print("STRAVA FIT FILE UPLOADER")
    print("="*60)
    
    if args.multiple:
        upload_multiple_files(args.multiple, activity_type=args.type)
    else:
        upload_to_strava(
            args.file,
            activity_name=args.name,
            activity_type=args.type,
            description=args.description
        )
