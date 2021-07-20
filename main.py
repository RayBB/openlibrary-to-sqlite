import os
from database import create_works_database, load_database
from download import download_file_with_progress_bar
import subprocess
import logging

DATABASE_LOCATION = "openlibrary_works.sqlite3"

def do_everything():
    if not os.path.isfile(DATABASE_LOCATION):
        dump_file_location = "ol_dump_works_latest.txt.gz"
        if not os.path.isfile(dump_file_location):
            logging.info("dump not detected, downloading")
            url = "https://openlibrary.org/data/ol_dump_works_latest.txt.gz"
            download_file_with_progress_bar(url, dump_file_location)
        con = create_works_database(DATABASE_LOCATION)
        load_database(con, dump_file_location)

    subprocess.run(["datasette", DATABASE_LOCATION, "--setting", "sql_time_limit_ms", "35000"])

do_everything()