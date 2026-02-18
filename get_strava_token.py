"""
Helper script to get Strava OAuth access token
Run this to authorize your app and get an access token with proper permissions
"""

import requests
import json
from pathlib import Path

print("="*60)
print("STRAVA OAUTH TOKEN GENERATOR")
print("="*60)

# Check if config exists
config_file = Path('strava_config.json')
if config_file.exists():
    print("\nFound existing strava_config.json")
    use_existing = input("Use existing client ID and secret? (y/n): ").lower().strip()
    if use_existing == 'y':
        with open(config_file, 'r') as f:
            config = json.load(f)
            CLIENT_ID = config.get('client_id', '')
            CLIENT_SECRET = config.get('client_secret', '')
    else:
        CLIENT_ID = ''
        CLIENT_SECRET = ''
else:
    CLIENT_ID = ''
    CLIENT_SECRET = ''

if not CLIENT_ID:
    print("\nFirst, create a Strava API app at:")
    print("   https://www.strava.com/settings/api")
    print()
    CLIENT_ID = input("Enter your Client ID: ").strip()

if not CLIENT_SECRET:
    CLIENT_SECRET = input("Enter your Client Secret: ").strip()

REDIRECT_URI = 'http://localhost'

# Generate authorization URL
auth_url = (
    f'https://www.strava.com/oauth/authorize'
    f'?client_id={CLIENT_ID}'
    f'&response_type=code'
    f'&redirect_uri={REDIRECT_URI}'
    f'&approval_prompt=force'
    f'&scope=activity:write,activity:read_all'
)

print("\n" + "="*60)
print("STEP 1: AUTHORIZE THE APP")
print("="*60)
print("\n1. Visit this URL in your browser:\n")
print(auth_url)
print("\n2. Click 'Authorize' to grant permissions")
print("3. You'll be redirected to a URL like:")
print("   http://localhost/?state=&code=ABCDEF123456789...")
print("\n4. Copy everything after 'code=' from that URL")

code = input("\nPaste the authorization code here: ").strip()

# Remove any query parameters that might have been copied
if '&' in code:
    code = code.split('&')[0]

print("\n" + "="*60)
print("STEP 2: EXCHANGE CODE FOR TOKEN")
print("="*60)
print("\nRequesting access token from Strava...")

try:
    token_response = requests.post(
        'https://www.strava.com/oauth/token',
        data={
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'code': code,
            'grant_type': 'authorization_code'
        }
    )
    token_response.raise_for_status()
    token_data = token_response.json()
    
    access_token = token_data['access_token']
    refresh_token = token_data['refresh_token']
    expires_at = token_data['expires_at']
    
    print("Success! Received tokens from Strava\n")
    
    # Save to config file
    config = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'access_token': access_token,
        'refresh_token': refresh_token,
        'expires_at': expires_at
    }
    
    with open('strava_config.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    print("Saved credentials to strava_config.json")
    print("\n" + "="*60)
    print("YOUR TOKENS")
    print("="*60)
    print(f"\nAccess Token:  {access_token}")
    print(f"Refresh Token: {refresh_token}")
    print(f"\nAccess token expires at: {expires_at}")
    print("\nYou can now use new_swimming_uploader.py to upload activities!")
    print("\nExample:")
    print("  python new_swimming_uploader.py output/25_swimming.fit")
    
except requests.exceptions.HTTPError as e:
    print(f"\nError: Failed to get token")
    print(f"Response: {token_response.text}")
    print("\nPossible issues:")
    print("- Invalid authorization code (codes expire quickly)")
    print("- Incorrect client ID or secret")
    print("- Code already used (each code can only be used once)")
    print("\nTry running this script again to get a new authorization code")
except Exception as e:
    print(f"\nUnexpected error: {e}")
