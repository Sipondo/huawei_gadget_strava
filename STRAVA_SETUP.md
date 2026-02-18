# Strava Upload Setup Guide

## Step 1: Create a Strava API Application

1. Go to https://www.strava.com/settings/api
2. Click "Create an App" (or use an existing one)
3. Fill in the details:
   - **Application Name**: (e.g., "Huawei Gadget Converter")
   - **Category**: Choose appropriate category
   - **Club**: (optional)
   - **Website**: Can use http://localhost
   - **Authorization Callback Domain**: Use `localhost`
4. Click "Create"
5. Note your **Client ID** and **Client Secret**

## Step 2: Get an Access Token

### Option A: Quick Method (for personal use)
1. After creating your app, scroll down to "Your Access Token"
2. You'll see a token that looks like: `a1b2c3d4e5f6...`
3. Copy this token - it has read/write permissions

### Option B: OAuth Flow (more secure)
Use this Python script to get a token with proper OAuth:

```python
import requests

CLIENT_ID = 'your_client_id'
CLIENT_SECRET = 'your_client_secret'
REDIRECT_URI = 'http://localhost'

# Step 1: Get authorization
auth_url = f'https://www.strava.com/oauth/authorize?client_id={CLIENT_ID}&response_type=code&redirect_uri={REDIRECT_URI}&approval_prompt=force&scope=activity:write,activity:read_all'

print("Visit this URL in your browser:")
print(auth_url)
print("\nAfter authorizing, you'll be redirected to a URL like:")
print("http://localhost/?state=&code=ABCDEF123456...")
print("\nCopy the 'code' parameter from that URL and paste it here:")
code = input("Code: ")

# Step 2: Exchange code for token
token_response = requests.post(
    'https://www.strava.com/oauth/token',
    data={
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'code': code,
        'grant_type': 'authorization_code'
    }
)

token_data = token_response.json()
print("\nYour access token:")
print(token_data['access_token'])
print("\nYour refresh token (save this for later):")
print(token_data['refresh_token'])
```

## Step 3: Configure the Uploader

Create a file named `strava_config.json` (copy from `strava_config.json.example`):

```json
{
  "client_id": "12345",
  "client_secret": "abc123def456...",
  "access_token": "xyz789..."
}
```

**OR** set environment variables:

```bash
# Windows PowerShell
$env:STRAVA_ACCESS_TOKEN="your_token_here"

# Windows CMD
set STRAVA_ACCESS_TOKEN=your_token_here

# Linux/Mac
export STRAVA_ACCESS_TOKEN="your_token_here"
```

## Step 4: Upload Your Activity

```bash
# Upload a single file
python new_swimming_uploader.py output/25_swimming.fit

# Upload with custom name
python new_swimming_uploader.py output/25_swimming.fit --name "Morning Pool Session"

# Upload with description
python new_swimming_uploader.py output/25_swimming.fit --name "Lunch Swim" --description "Great workout!"

# Upload multiple files
python new_swimming_uploader.py --multiple "output/*.fit"
```

## Troubleshooting

### "No Strava access token found"
- Make sure you created `strava_config.json` with valid credentials
- OR set the `STRAVA_ACCESS_TOKEN` environment variable

### "401 Unauthorized"
- Your access token may have expired
- Regenerate a new token from https://www.strava.com/settings/api
- Or use the refresh token to get a new access token

### "Upload failed"
- Check that your FIT file is valid
- Make sure you have the `requests` library installed: `pip install requests`
- Verify your token has `activity:write` scope

## Required Python Packages

```bash
pip install requests
```

## Notes

- Access tokens from the Strava API page may expire
- For long-term use, implement token refresh using the refresh token
- Strava has rate limits (100 requests per 15 minutes, 1000 per day)
- Uploads are processed asynchronously and may take a few seconds
