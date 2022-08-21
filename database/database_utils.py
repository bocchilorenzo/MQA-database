import mysql.connector as mysql
from dotenv import load_dotenv
import os

load_dotenv()


def connect_DB():
    db_conn = mysql.connect(
        host=os.environ.get("MYSQL_HOST"),
        user=os.environ.get("MYSQL_USER"),
        password=os.environ.get("MYSQL_PASSWORD"),
        database=os.environ.get("MYSQL_DB"),
    )
    return db_conn


def close_connection(mycursor, db_conn):
    mycursor.close()
    db_conn.close()


def create_tables():
    """
    Create the tables

    This function creates the tables, if they don't exist.
    """
    db_conn = connect_DB()
    mycursor = db_conn.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS artists (id int NOT NULL, name varchar(255)," +
                     "url varchar(255), picture varchar(255), popularity int, PRIMARY KEY (id))")
    mycursor.execute("CREATE TABLE IF NOT EXISTS albums (id int NOT NULL, title varchar(255)," +
                     "duration int, streamReady bool, streamStartDate timestamp, allowStreaming bool," +
                     "premiumStreamingOnly bool, numberOfTracks int, numberOfVideos int, numberOfVolumes int," +
                     "releaseDate date, copyright varchar(255), type varchar(255), version varchar(255)," +
                     "url varchar(255), cover varchar(255), videoCover varchar(255), explicit bool," +
                     "upc varchar(255), popularity int, audioQuality varchar(255), artist int," +
                     "vibrantColor varchar(255), notFound bool default 0 Not NULL," +
                     "PRIMARY KEY (id), FOREIGN KEY(artist) REFERENCES artists(id))")
    close_connection(mycursor, db_conn)


def create_db():
    """
    Create the database

    This function creates the database, if it doesn't exist.
    """
    db_conn = mysql.connect(
        host="localhost",
        user=os.environ.get("MYSQL_USER"),
        password=os.environ.get("MYSQL_PASSWORD")
    )

    mycursor = db_conn.cursor()

    mycursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{os.environ.get('MYSQL_DB')}`")

    close_connection(mycursor, db_conn)
