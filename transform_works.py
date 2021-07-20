# everything about preparing the data data for insertion into the database
import logging
import orjson

def get_identifier_from_sting(string):
    """
    For example, /authors/OL4714426A -> OL4714426A
    """
    return string.split("/")[-1]

def parse_everything_in_work(work):
    output = {}
    for key in work.keys():
        value = work.get(key)
        if isinstance(value, dict):
            # by default get the "value" key otherwise get the "key" key
            output[key] = value.get("value", value.get("key"))
        elif isinstance(value, list):
            if isinstance(value[0], dict):
                type_value = value[0].get('type', {})
                if type_value == '/type/author_role' or type_value.get('key') == '/type/author_role':
                    output[key] = parse_authors_array(value)
                    continue
            output[key] = value
        elif isinstance(value, str):
            if key in ['key']:
                output[key] = get_identifier_from_sting(value)
                continue
            output[key] = value
    return output

def parse_authors_array(authors):
    try:
        with_author_attribute = [author for author in authors if 'author' in author]
        return [get_identifier_from_sting(author['author']['key']) for author in with_author_attribute]
    except:
        logging.exception("Error parsing authors array:")
        logging.exception(authors)
        return []

def transform_row(row: str):
    # takes a row and returns a dict to be input to the sqlite
    json_in_mem = orjson.loads(row.split("\t")[4])
    return parse_everything_in_work(json_in_mem)

def test_parse_authors_array():
    input = [{'type': {'key': '/type/author_role'}, 'author': {'key': '/authors/OL4714426A'}}, {'type': {'key': '/type/author_role'}, 'author': {'key': '/authors/OL27714A'}}]
    output = parse_authors_array(input)
    assert output == ['OL4714426A', 'OL27714A']
    input = [{'author': {'key': '/authors/OL9087100A'}, 'type': {'key': '/type/author_role'}}, {'type': {'key': '/type/author_role'}}]
    output = parse_authors_array(input)
    assert output == ['OL9087100A']
