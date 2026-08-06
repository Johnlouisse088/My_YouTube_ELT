import requests
import json

import os
from dotenv import load_dotenv
from datetime import date

load_dotenv(dotenv_path="./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = 'MrBeast'
max_result = 1

def get_playlist_id():

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        print(response)  # Output: <Response [200]>
        # It checks the HTTP status code of the response.
        # If the request was successful (2xx), nothing happens.
        # If the request failed (4xx or 5xx), it raises an HTTPError exception.
        response.raise_for_status()
        data = response.json()

        # # json.dumps() - converts a Python object (such as a dictionary or list) into a JSON strin
        # # indent - It formats the JSON with indentation
        print(json.dumps(data, indent=4))

        channel_item = data['items'][0]
        channel_playlistId = channel_item['contentDetails']['relatedPlaylists']['uploads'] # UUX6OQ3DkcsbYNE6H8uQQuVA
        print('channel_playlistId: ' + channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e

def get_video_ids(playlistId):
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_result}&playlistId={playlistId}&key={API_KEY}'
    video_ids = []
    pageToken = None

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                print(video_id)
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken")
            if not pageToken or len(video_ids) == 10:
                break

        print(video_ids)
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e




def extracted_video_data(video_ids):
    extracted_data = []

    def batch_list(video_ids, batch_size):
        for video_id in range(0, len(video_ids), batch_size):
            yield video_ids[video_id: video_id + batch_size]

    try:
        for batch in batch_list(video_ids, max_result):
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []): # Sample: "items": [{contentDetails}, {snippet}, {statistics}]
                video_id = item['id']  # Sample: [{..., "id": "f7y2XikE7sY"....}, {...}]
                snippet = item['snippet']  # Sample: [{"snippet": {...} ... }]
                contentDetails = item['contentDetails'] # Sample: [{"contentDetails": {...} ... }]
                statistics = item['statistics']  # Sample: [{"statistics": {...} ... }]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"], # Sample: "snippet": {"title": "Paying For Food With My Car"}
                    "publishedAt": snippet["publishedAt"], # Sample:
                    "duration": contentDetails["duration"], # Sample:
                    "viewCount": statistics.get("viewCount", None), # Sample: "statistics": {"viewCount": "20762921"}
                    "likeCount": statistics.get("likeCount", None), # Sample:
                    "commentCount": statistics.get("commentCount", None), # Sample:
                }

            extracted_data.append(video_data)
        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    print(extracted_data)
    file_path = f"./data/My_YouTbe_ELT{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)


# Run this code only if this file is executed directly, not when it's imported by another Python file.
if __name__ == '__main__':
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    extracted_data = extracted_video_data(video_ids)
    save_to_json(extracted_data)