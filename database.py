# Everything related to databases
import sqlite3
import logging
from tqdm import tqdm
import gzip
from transform_works import transform_row

KEYS_FROM_FIRST_22_MILLION = ['title', 'links', 'permission', 'subject_times', 'dewey_number', 
'subject_people', 'description', 'excerpts', 'first_sentence', 'remote_ids', 'authors', 
'location', 'subtitle', 'type', 'first_publish_date', 'covers', 'lc_classifications', 
'subjects', 'subject_places', 'last_modified', 'cover_edition', 'key', 'created', 
'ospid', 'notifications', 'genres', 'works', 'translated_titles', 'notes', 'series', 'original_languages', 'ocaid']

def create_works_database(destination):
    con=sqlite3.connect(destination)
    create_table(con)
    return con

def create_table(connection):
    # basically, you'd have to scan all the data first to find all possible keys so we are just hardcoding here
    cols = ','.join(KEYS_FROM_FIRST_22_MILLION)
    connection.execute(f"create table if not exists works({cols})")

def get_question_marks(keys):
    return ",".join(["?"]*len(keys))

def load_database(con, data_file):
    ESTIMATED_WORKS = 23000000 # we have to hardcode this to avoid spending several minutes counting lines.
    query = f"insert into works ({','.join(KEYS_FROM_FIRST_22_MILLION)}) values ({get_question_marks(KEYS_FROM_FIRST_22_MILLION)});"
    batch = []
    with gzip.open(data_file, 'rt') as f:
        for i, row in tqdm(enumerate(f), total=ESTIMATED_WORKS, unit="works"):
            cleaned_row_json = transform_row(row)
            row_arr = []
            for key in KEYS_FROM_FIRST_22_MILLION:
                v = cleaned_row_json.get(key)
                if not isinstance(v,list):
                    row_arr.append(v)
                else:
                    row_arr.append(str(v).replace("'",'"'))
            # TODO: create new columns as they are encountered instead of defining them all at the top
            batch.append(row_arr)
            # this 1 million number can be decreased. That will cause less memory usage but slower processing
            if (i+1) % 1000000 == 0:
                try:
                    con.executemany(query,batch)
                    con.commit()
                    batch = []
                except Exception as e:
                    logging.error(f"row {i}")
                    logging.error(e)
    con.executemany(query,batch)
    con.commit()
