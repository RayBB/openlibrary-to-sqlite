# Everything related to databases
import sqlite3
import logging
from tqdm import tqdm
import gzip
from transform_works import transform_row

# Keys from works
KEYS_FROM_FIRST_22_MILLION = ['title', 'links', 'permission', 'subject_times', 'dewey_number', 
'subject_people', 'description', 'excerpts', 'first_sentence', 'remote_ids', 'authors', 
'location', 'subtitle', 'type', 'first_publish_date', 'covers', 'lc_classifications', 
'subjects', 'subject_places', 'last_modified', 'cover_edition', 'key', 'created', 
'ospid', 'notifications', 'genres', 'works', 'translated_titles', 'notes', 'series', 'original_languages', 'ocaid']

# Keys from authors, but doesn't work for some reason
# KEYS_FROM_FIRST_22_MILLION = ['publish_places', 'source_records', 'notes', 'subject_time', 'marc', 'website_name', 'numeration', 'ocaid', 'type', 'personal_name', 'subtitle', 'last_modified', 'photograph', 'publish_date', 'website', 'comment', 'tags', 'subject_place', 'remote_ids', 'number_of_pages', 'created', 'publish_country', 'pagination', 'fuller_name', 'oclc_numbers', 'contributions', 'location', 'title', 'create', 'covers', 'id_viaf', 'dewey_decimal_class', 'photos', 'series', 'role', 'subjects', 'bio', 'title_prefix', 'links', 'entity_type', 'wikipedia', 'publishers', 'authors', 'body', 'works', 'lc_classifications', 'birth_date', 'edition_name', 'id_wikidata', 'entiy_type', 'revision', 'alternate_names', 'date', 'death_date', 'id_librarything', 'name', 'by_statement', 'latest_revision', 'genres', 'other_titles', 'key', 'languages', 'lccn']
# Keys from authors that are useful to me
# KEYS_FROM_FIRST_22_MILLION = ['key', 'name', 'title', 'birth_date', 'death_date']
# ESTIMATED_AUTHORS = 12866037

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
