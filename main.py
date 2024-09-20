import json
import sqlite3

import requests
import pandas


def scaffold_database(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()

    cursor.execute(
        """
        
        """
    )

    connection.commit()


def fetch_data() -> list | None:
    url = "https://pokeapi.co/api/v2/pokemon?limit=10"
    response = requests.get(url)

    if response.status_code != 200:
        return None

    data = json.loads(response.json())

    return data.results


def transform_data(raw_data: list) -> pandas.DataFrame | None:
    df = pandas.DataFrame(raw_data)

    return None


def load_data(data_frame: pandas.DataFrame, connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()

    cursor.execute(
        """
        
        """
    )

    connection.commit()


def main():
    connection = sqlite3.connect("local.db")
    scaffold_database(connection)

    raw_data = fetch_data()

    if raw_data is None:
        print("Pokemon not found")
        return

    data_frame = transform_data(raw_data)

    if data_frame is None:
        print("Data frame is empty")
        return

    load_data(data_frame, connection)
    connection.close()


if __name__ == "__main__":
    main()
