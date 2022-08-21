import os
from tqdm import tqdm
import requests
import re
import pandas as pd
from dotenv import load_dotenv
from tidal_unofficial import TidalUnofficial
import database as DB
from time import sleep
from random import uniform
from shutil import rmtree

load_dotenv()


def get_from_api(ids):
    """
    Get the data from the API

    Leveraging the tidal_unofficial library, extract the albums' and artists' data from the
    Tidal API.
    """

    tidal = TidalUnofficial()
    cont = 0
    db_conn = DB.connect_DB()
    mycursor = db_conn.cursor()

    insert_artist = "INSERT INTO artists (id, name) VALUES (%s, %s)"
    insert_album = "INSERT INTO albums (id, title, duration, streamReady, streamStartDate, allowStreaming, premiumStreamingOnly, numberOfTracks, numberOfVideos, numberOfVolumes, releaseDate, copyright, type, version, url, cover, videoCover, explicit, upc, popularity, audioQuality, artist, vibrantColor, notFound) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)"
    insert_album_not_found = "INSERT INTO albums (id, notFound) VALUES (%s, 1)"
    insert_artist_data = "UPDATE artists SET url = %s, picture = %s, popularity = %s WHERE id = %s"

    for album_id in ids:
        # The API call might return 404 or an error if the connection is unstable, so we check for both issues
        try:
            album = tidal.get_album(album_id)
            if 'status' in album:
                if album['status'] == 404:
                    proceed = False
            else:
                proceed = True
        except:
            proceed = False

        if proceed:
            for artist in album['artists']:
                try:
                    val = (artist['id'], artist['name'])
                    mycursor.execute(insert_artist, val)
                    db_conn.commit()
                    print(
                        f"Artist {artist['id']} - {artist['name']} added.\n")
                except:
                    print(
                        f"Artist {artist['id']} - {artist['name']} is already in the database.\n")
            try:
                val = (album['id'], album['title'], album['duration'], 1 if album['streamReady'] else 0, album['streamStartDate'], 1 if album['allowStreaming'] else 0, 1 if album['premiumStreamingOnly'] else 0, album['numberOfTracks'], album['numberOfVideos'], album['numberOfVolumes'],
                       album['releaseDate'], album['copyright'], album['type'], album['version'], album['url'], album['cover'], album['videoCover'], 1 if album['explicit'] else 0, album['upc'], album['popularity'], album['audioQuality'], album['artist']['id'], album['vibrantColor'])
                mycursor.execute(insert_album, val)
                db_conn.commit()
                print(
                    f"Album {album['id']} - {album['title']} added.\n")
            except:
                print(
                    f"Error inserting album {album['id']} - {album['title']}.\n")
            for artist in album['artists']:
                try:
                    artist_api = tidal.get_artist(artist['id'])
                    if 'status' in artist_api:
                        if artist_api['status'] == 404:
                            print(f"Artist {artist['id']} not found.\n")
                    else:
                        val = (artist_api['url'], artist_api['picture'],
                               artist_api['popularity'], artist_api['id'])
                        mycursor.execute(insert_artist_data, val)
                        db_conn.commit()
                except:
                    print(f"Error finding/updating artist {artist['id']}.\n")
        else:
            val = (album_id,)
            mycursor.execute(insert_album_not_found, val)
            db_conn.commit()
            print(f"Album {album_id} not found.\n")
        
        cont += 1
        if cont == 20:
            sleep(uniform(8.0, 12.0))
            cont = 0
        else:
            sleep(uniform(2.0, 3.0))

    DB.close_connection(mycursor, db_conn)


def extract_from_db(ids):
    """
    Extract the new ids

    Extract from the DB the currently stored ids, compare them to the ones received as a
    parameter and return only the new ones.
    """

    DB.create_db()
    DB.create_tables()

    db_conn = DB.connect_DB()
    mycursor = db_conn.cursor()
    mycursor.execute(f"SELECT id FROM albums")

    stored_ids = [x[0] for x in mycursor.fetchall()]

    if len(stored_ids) != 0:
        existing_ids = set(stored_ids)
        del stored_ids
        new_ids = []
        for album_id in ids:
            if (int(album_id) not in existing_ids):
                new_ids.append(album_id)
        return new_ids

    DB.close_connection(mycursor, db_conn)
    return ids


def generate_ids(paths):
    """
    Extract ids

    This function goes over the two csv files, extracts all the existing ids and merges
    them in a single list that gets returned.
    """
    toRtn = []

    for p in paths:
        df = pd.read_csv(p, encoding='utf-8', encoding_errors='ignore')
        df_ids = df['URL'].dropna()

        ids = df_ids.tolist()
        for i in range(len(ids)):
            ids[i] = ids[i][ids[i].rfind("/")+1:]

        if len(toRtn) == 0:
            toRtn = ids
        else:
            toRtn.extend(ids)

        del df, df_ids
    return toRtn


def download_files(urls, filenames):
    """
    Download function

    This function downloads the files given in the 'urls' list and saves them
    with the corresponding name in 'filenames'. Both these lists need to be the
    same length, otherwise the filename will be extracted from the url response.
    """

    print("Downloading files...\n")
    if not os.path.isdir('./tmp'):
        os.mkdir("./tmp")
    chunkSize = 1024

    for i in range(len(urls)):
        r = requests.get(urls[i], stream=True)
        with open(f"./tmp/{filenames[i]}", 'wb') as f:
            pbar = tqdm(unit="B", total=int(
                r.headers['Content-Length']), desc=filenames[i])
            pbar.clear()  # Remove 0%
            for chunk in r.iter_content(chunk_size=chunkSize):
                if chunk:
                    pbar.update(len(chunk))
                    f.write(chunk)
            pbar.close()
        res = requests.get(urls[i], stream=True)


def start():
    urls = ["https://www.meridian-audio.info/MQA/MQA_Albums.csv",
            "https://www.meridian-audio.info/MQA/MQA_Singles.csv"]
    filenames = ["albums.csv", "singles.csv"]

    if not(os.path.isfile(f"./tmp/{filenames[0]}") and os.path.isfile(f"./tmp/{filenames[1]}")):
        download_files(urls, filenames)
    else:
        print("Files already downloaded.\n")

    for i in range(len(filenames)):
        filenames[i] = "./tmp/" + filenames[i]

    ids = generate_ids(filenames)
    new_ids = extract_from_db(ids)
    print(
        f"Total albums found: {len(ids)}\nNew albums: {len(new_ids)}\nAlbums already in database: {len(ids)-len(new_ids)}\n")

    get_from_api(new_ids)

    rmtree('./tmp/')

    print("All new MQA albums have been added to the database!")


start()
