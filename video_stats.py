import requests
import json

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = 'MrBeast'

def get_playlist_id():

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        print(response)  # Output: <Response [200]>

        data = response.json()

        # # json.dumps() - converts a Python object (such as a dictionary or list) into a JSON strin
        # # indent - It formats the JSON with indentation
        # print(json.dumps(data, indent=4))

        channel_item = data['items'][0]
        channel_playlistId = channel_item['contentDetails']['relatedPlaylists']['uploads']
        print('channel_playlistId: ' + channel_playlistId)

    except requests.exceptions.RequestException as e:
        raise e

# Run this code only if this file is executed directly, not when it's imported by another Python file.
if __name__ == '__main__':
    get_playlist_id()