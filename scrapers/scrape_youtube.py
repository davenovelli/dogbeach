# -*- coding: utf-8 -*-

# Sample Python code for youtube.playlistItems.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

import os
import sys
import json
import time
import yaml
import logging
import requests
import urllib.parse
from pathlib import Path

import googleapiclient.discovery
import googleapiclient.errors

youtube = None

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

##################################### Config
os.chdir(os.path.dirname(sys.argv[0]))
with open("../config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.FullLoader)

sys.path.append('..')
from dogbeach import doglog
_logger = None

# What is the API endpoint
REST_API_PROTOCOL = config['common']['rest_api']['protocol']
REST_API_HOST = config['common']['rest_api']['host']
REST_API_PORT = config['common']['rest_api']['port']
REST_API_URL = f"{REST_API_PROTOCOL}://{REST_API_HOST}:{REST_API_PORT}"
CREATE_ENDPOINT = f"{REST_API_URL}/article"
PUBLISHER_ARTICLES_ENDPOINT = f"{REST_API_URL}/articleUrlsByPublisher?publisher="

# UserID and BrowswerID are required fields for creating articles, this User is the ID tied to the system account
SYSTEM_USER_ID = config['common']['system_user_id']

# This is the "blank" UUID
SCRAPER_BROWSER_ID = config['common']['browser_id']

# The URLs of the articles that have already been scraped
ALREADY_SCRAPED = set()

# The number of videos to request in each page from youtube's API
RESULTS_PER_PAGE = config['youtube']['videos_per_page']  # youtube does not permit values higher than 50

# Maximum number of empty pages to load before quitting
MAX_EMPTY_PAGES = config['youtube']['max_empty_pages']

# The minimum length of a video to scrape
MIN_VIDEO_DURATION = 3 * 60


def get_logger():
    """ Initialize and/or return existing logger object

    :return: a DogLog logger object
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath("__file__"))) / f"log/youtube_site.log"
        _logger = doglog.setup_logger(f'youtube', logfile, clevel=logging.DEBUG)
    return _logger


def get_youtube():
    """ Singleton for the youtube service object
    """
    global youtube
    # Establish the service object
    if youtube is None:
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey='AIzaSyBFN6xIuoulmTiHFcT2DRHAkYxTNH1NKNc')
    return youtube


def get_already_scraped(channel_names):
    """ For each of the channels that we're scraping, lookup all URLs we've already scraped to avoid duplicates
    """
    global ALREADY_SCRAPED

    for channel_name in channel_names:
        r = requests.get(PUBLISHER_ARTICLES_ENDPOINT + urllib.parse.quote_plus(channel_name))
        urls_json = r.json()
        channel_urls = set([x['url'] for x in urls_json])
        get_logger().debug(f"{channel_name}: {len(channel_urls)} videos found")

        ALREADY_SCRAPED.update(channel_urls)

    # get_logger().debug(ALREADY_SCRAPED)
    get_logger().debug("Found {} articles already scraped".format(len(ALREADY_SCRAPED)))
    

def get_channels():
    """ Read in the file containing the list of youtube channel ids to scrape, and use the channel names to populate the ALREADY_SCRAPED list
    """
    import pandas as pd

    df = pd.read_csv('youtube_channel_list.txt')

    channel_names = df.channel_name.tolist()
    get_already_scraped(channel_names)

    channel_ids = df.channel_id.tolist()
    
    return channel_ids


def get_playlists(channel_ids):
    """
    """
    playlist_ids = []
    for channel_id in channel_ids:
        get_logger().debug(f"Channel:  {channel_id}")
        request = get_youtube().channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        # get_logger().debug(json.dumps(response, sort_keys=True, indent=2))
        
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        get_logger().debug(f"Playlist: {uploads_playlist_id}")
        playlist_ids += [uploads_playlist_id]

    return playlist_ids


def extract_video_data(video_json):
    """ For every video, extract the data into the schema format
    
    """
    video = video_json['snippet']
    # get_logger().debug(video)
    extracted = {
        'id': video['resourceId']['videoId'],
        'publishedAt': video['publishedAt'],
        'publisher': video['videoOwnerChannelTitle'] or video['channelTitle'],
        'text_content': video['description'],
        'thumb': video['thumbnails']['maxres']['url'] if 'maxres' in video['thumbnails'] else video['thumbnails']['medium']['url'],
        'title': video['title'],
        'url': f"https://www.youtube.com/watch?v={video['resourceId']['videoId']}"        
    }

    if 'tags' in video and len(video['tags']) > 0:
        tags = video['tags']
        # get_logger().debug(tags)
        extracted['tags'] = ",".join(tags)

    # get_logger().debug(extracted)
    return extracted


def parse_duration_in_seconds(duration_string):
    """ Parse the ISOxxxx duration string returned by the Youtube API into seconds
    """
    day_time = duration_string.split('T')
    day_duration = day_time[0].replace('P', '')
    day_list = day_duration.split('D')
    if len(day_list) == 2:
        day = int(day_list[0]) * 60 * 60 * 24
        day_list = day_list[1]
    else:
        day = 0
        day_list = day_list[0]
    hour_list = day_time[1].split('H')
    if len(hour_list) == 2:
        hour = int(hour_list[0]) * 60 * 60
        hour_list = hour_list[1]
    else:
        hour = 0
        hour_list = hour_list[0]
    minute_list = hour_list.split('M')
    if len(minute_list) == 2:
        minute = int(minute_list[0]) * 60
        minute_list = minute_list[1]
    else:
        minute = 0
        minute_list = minute_list[0]
    second_list = minute_list.split('S')
    if len(second_list) == 2:
        second = int(second_list[0])
    else:
        second = 0
    
    return day + hour + minute + second


def scrape_playlist(playlist_id):
    """ Get all the videos for a specific playlist id
    """
    global ALREADY_SCRAPED
    
    new_videos = []
    more = True
    nextPageToken = None
    consecutive_empty_pages = 0
    while more:
        # Generate the request based on the current page we want
        if nextPageToken:
            request = get_youtube().playlistItems().list(
                part = "snippet,contentDetails",
                playlistId = playlist_id,
                maxResults = RESULTS_PER_PAGE,
                pageToken = nextPageToken
            )
        else:
            request = get_youtube().playlistItems().list(
                part = "snippet,contentDetails",
                playlistId = playlist_id,
                maxResults = RESULTS_PER_PAGE
            )
        
        # Execute request and set parameters for next iteration
        response = request.execute()
        time.sleep(1)
        # get_logger().debug(response)
        # get_logger().debug(json.dumps(response, sort_keys=True, indent=4))

        if not 'nextPageToken' in response:
            # No more pages to scrape
            more = False
        else:
            # We're not done yet
            nextPageToken = response['nextPageToken']
        
        page_videos = [extract_video_data(x) for x in response['items']]
        vid_count_before = len(page_videos)

        page_videos = [x for x in page_videos if x['url'] not in ALREADY_SCRAPED]
        
        # Get the IDs of the videos from this page of the current playlist
        video_ids = [x['id'] for x in page_videos]

        # Query the duration of each video so we can eliminate short videos before checking for duplicates
        video_request = get_youtube().videos().list(
            part = "contentDetails",
            id = ",".join(video_ids)
        )
        video_response = video_request.execute()
        time.sleep(1)
        # get_logger().debug(video_response)
        # get_logger().debug(json.dumps(video_response, sort_keys=True, indent=4))

        video_durations = {}
        for video_item in video_response['items']:
            video_durations[video_item['id']] = {'duration': parse_duration_in_seconds(video_item['contentDetails']['duration'])}
        
        # get_logger().debug(video_durations)

        for page_video in page_videos:
            page_video['duration'] = video_durations[page_video['id']]['duration']
        
        # Filter out any videos that aren't at least 3 minutes long...
        page_videos = [x for x in page_videos if x['duration'] >= MIN_VIDEO_DURATION]
        
        # get_logger().debug(json.dumps(page_videos, sort_keys=True, indent=4))
        get_logger().debug(f"{vid_count_before} videos were trimmed down to: {len(page_videos)}")
        get_logger().debug(f"There are {len(page_videos)} new videos of sufficient duration on this page")

        if len(page_videos) > 0:
            new_videos += page_videos
            consecutive_empty_pages = 0
        else:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= MAX_EMPTY_PAGES:
                more = False
    
    get_logger().debug(f"There are {len(new_videos)} new videos of sufficient duration on this channel to add to the database")
    # get_logger().debug(json.dumps(new_videos, sort_keys=True, indent=4))
    
    return new_videos


def create_videos(videos):
    """
    Push the videos to the database through the REST API

    :param articles:
    :return:
    """
    for video in videos:
        # Add some common fields
        video['userId'] = SYSTEM_USER_ID
        video['browserId'] = SCRAPER_BROWSER_ID
        video = {key:val for key, val in video.items() if key not in ['id', 'duration']}
        # get_logger().debug("Writing article to RDS...\n{}".format(video))
        video_str = f"WRITING: {video['publisher']} : {video['publishedAt']} : {video['title']}"
        if 'tags' in video:
            video_str +=  f":::::::::::::: {video['tags']}"
        get_logger().debug(video_str)

        header = { "Content-Type": "application/json" }
        json_data = json.dumps(video, default=str)
        r = requests.post(CREATE_ENDPOINT, headers=header, data=json_data)
        # get_logger().debug("\n\n=================================================================================\n\n")
        
        try:
            r.raise_for_status()
        except Exception as ex:
            message = f"There was a {type(ex)} error while creating article {video['url']}:...\n{r.json()}"
            get_logger().error(message)



def scrape_playlists(playlist_ids):
    """ For each channel's playlist id, scrape new video content
    """
    all_playlist_videos = []
    for playlist_id in playlist_ids:
        playlist_videos = scrape_playlist(playlist_id)
        create_videos(playlist_videos)

        all_playlist_videos += playlist_videos
    
    return all_playlist_videos


def main():
    # Get the list of channels and ids to scrape from a config file
    channels = get_channels()
    get_logger().debug(f"Channels: {channels}")
    
    # For each channel get the "Uploads" playlist id
    playlists = get_playlists(channels)
    get_logger().debug(f"Playlists: {playlists}")
    
    # For each playlist, extract the data from all videos
    videos = scrape_playlists(playlists)
    get_logger().debug(f"Scraped {len(videos)} total videos")
    
    # Cleanup TODO: Move this to a post-script function that runs every time
    get_youtube().close()
    exit()


if __name__ == "__main__":
    main()