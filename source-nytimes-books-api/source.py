# source.py
import argparse  # helps parse commandline arguments
import json
import sys
import os
import requests
import datetime
import json


def get_input_file_path(path):
    if os.path.isabs(path):
        return path
    else: 
        return os.path.join(os.getcwd(), path)


def log(message):
    log_json = {"type": "LOG", "log": message}
    print(json.dumps(log_json))
    

def read_json(filepath):
    with open(filepath, "r") as f:
        return json.loads(f.read())

def _call_api(endpoint, api_key):
    return requests.get("https://api.nytimes.com/svc/books/v3/" + endpoint + "?api-key=" + api_key)


def check(config):
    response = _call_api(endpoint="lists/best-sellers/history.json", api_key=config["api_key"])
    if response.status_code == 200:
        result = {"status": "SUCCEEDED"}
    elif response.status_code == 403:
        # HTTP code 403 means authorization failed so the API key is incorrect
        result = {"status": "FAILED", "message": "API Key is incorrect."}
    else:
        result = {"status": "FAILED", "message": "Input configuration is incorrect. Please verify the input stock ticker and API key."}

    output_message = {"type": "CONNECTION_STATUS", "connectionStatus": result}
    print(json.dumps(output_message))


def discover():
    catalog = {
        "streams": [{
            "name": "best_sellers",
            "supported_sync_modes": ["full_refresh"],
            "json_schema": {
                "properties": {
                    "title": {
                        "type":"string"
                    },
                    "description": {
                        "type":"string"
                    },
                    "contributor": {
                        "type":"string"
                    },
                    "author": {
                        "type":"string"
                    },
                    "contributor_note": {
                        "type":"string"
                    },
                    "price": {
                        "type":"number"
                    },
                    "age_group": {
                        "type":"string"
                    },
                    "publisher": {
                        "type":"string"
                    },
                    "isbns": {
                        "type":"object",
                        "properties": {
                           "isbn10": {
                              "type":"string"
                           },
                           "isbn13": {
                              "type":"string"
                           }
                        }
                     },
                    "ranks_history": {
                        "type":"object",
                        "properties": {
                           "primary_isbn10": {
                              "type":"string"
                           },
                           "primary_isbn13": {
                              "type":"string"
                           },
                           "rank": {
                              "type":"number"
                           },
                           "list_name": {
                              "type":"string"
                           },
                           "display_name": {
                              "type":"string"
                           },
                           "published_date": {
                              "type":"string"
                           },
                           "bestsellers_date": {
                              "type":"string"
                           },
                           "weeks_on_list": {
                              "type":"number"
                           },
                           "ranks_last_week": {
                              "type":"string"
                           },
                           "asterisk": {
                              "type":"number"
                           },
                           "dagger": {
                              "type":"number"
                           }
                        }
                    },
                    "reviews": {
                        "type":"object",
                        "properties": {
                           "book_review_link": {
                              "type":"string"
                           },
                           "first_chapter_link": {
                              "type":"string"
                           },
                           "sunday_review_link": {
                              "type":"string"
                           },
                           "article_chapter_link": {
                              "type":"string"
                           }
                        }
                    }
                }
            }
        }]
    }
    airbyte_message = {"type": "CATALOG", "catalog": catalog}
    print(json.dumps(airbyte_message))


def spec():
    # Read the file named spec.json from the module directory as a JSON file
    current_script_directory = os.path.dirname(os.path.realpath(__file__))
    spec_path = os.path.join(current_script_directory, "spec.json")
    specification = read_json(spec_path)

    # form an Airbyte Message containing the spec and print it to stdout
    airbyte_message = {"type": "SPEC", "spec": specification}
    # json.dumps converts the JSON (python dict) to a string
    print(json.dumps(airbyte_message))


def read(config, catalog):
    # Find the best_sellers stream if it is present in the input catalog
    best_sellers_stream = None
    for configured_stream in catalog["streams"]:
        if configured_stream["stream"]["name"] == "best_sellers":
            best_sellers_stream = configured_stream

    if best_sellers_stream is None:
        log("No streams selected")
        return

    # We only support full_refresh at the moment, so verify the user didn't ask for another sync mode
    if best_sellers_stream["sync_mode"] != "full_refresh":
        log("This connector only supports full refresh syncs! (for now)")
        sys.exit(1)

    response = _call_api(endpoint="lists/best-sellers/history.json", api_key=config["api_key"])
    if response.status_code != 200:
        # In a real scenario we'd handle this error better :)
        log("Failure occurred when calling API")
        sys.exit(1)
    else:
        data = response.json()["results"]
        for book in data:
            book_data = {"title": book["title"], "description": book["description"], "contributor": book["contributor"], "author": book["author"], "contributor_note": book["contributor_note"], "price": book["price"], "age_group": book["age_group"], "publisher": book["publisher"], "isbns": json.loads(json.dumps(book["isbns"])), "ranks_history": json.loads(json.dumps(book["ranks_history"])), "reviews": json.loads(json.dumps(book["reviews"]))}
            record = {"stream": "best_sellers", "data": book_data, "emitted_at": int(datetime.datetime.now().timestamp()) * 1000}
            output_message = {"type": "RECORD", "record": record}
            print(json.dumps(output_message))


def run(args):
    parent_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser()
    subparsers = main_parser.add_subparsers(title="commands", dest="command")

    # Accept the spec command
    subparsers.add_parser("spec", help="outputs the json configuration specification", parents=[parent_parser])

    # Accept the check command
    check_parser = subparsers.add_parser("check", help="checks the config used to connect", parents=[parent_parser])
    required_check_parser = check_parser.add_argument_group("required named arguments")
    required_check_parser.add_argument("--config", type=str, required=True, help="path to the json configuration file")

    # Accept the discover command
    discover_parser = subparsers.add_parser("discover", help="outputs a catalog describing the source's schema", parents=[parent_parser])
    required_discover_parser = discover_parser.add_argument_group("required named arguments")
    required_discover_parser.add_argument("--config", type=str, required=True, help="path to the json configuration file")

    # Accept the read command
    read_parser = subparsers.add_parser("read", help="reads the source and outputs messages to STDOUT", parents=[parent_parser])
    read_parser.add_argument("--state", type=str, required=False, help="path to the json-encoded state file")
    required_read_parser = read_parser.add_argument_group("required named arguments")
    required_read_parser.add_argument("--config", type=str, required=True, help="path to the json configuration file")
    required_read_parser.add_argument("--catalog", type=str, required=True, help="path to the catalog used to determine which data to read")

    parsed_args = main_parser.parse_args(args)
    command = parsed_args.command

    if command == "spec":
        spec()
    elif command == "check":
        config_file_path = get_input_file_path(parsed_args.config)
        config = read_json(config_file_path)
        check(config)
    elif command == "discover":
        discover()
    elif command == "read":
        config = read_json(get_input_file_path(parsed_args.config))
        configured_catalog = read_json(get_input_file_path(parsed_args.catalog))
        read(config, configured_catalog)
    else:
        # If we don't recognize the command log the problem and exit with an error code greater than 0 to indicate the process
        # had a failure
        log("Invalid command. Allowable commands: [spec, check, discover, read]")
        sys.exit(1)

    # A zero exit code means the process successfully completed
    sys.exit(0)

def main():
    arguments = sys.argv[1:]
    run(arguments)


if __name__ == "__main__":
    main()
