import json
import sqlite3

import requests
import pandas


def scaffold_database(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pokemon (
            id INTEGER PRIMARY KEY,
            name TEXT,
            height REAL,
            weight REAL,
            imc REAL
        )
        """
    )

    cursor.close()


def fetch_data() -> list | None:
    url = "https://beta.pokeapi.co/graphql/v1beta"
    body = """ 
    query { 
      result: pokemon_v2_pokemon(limit: 151) {
        id
        name
        height
        weight
      }
    } 
    """
    response = requests.post(url, json={"query": body})

    if response.status_code != 200:
        return None

    raw_data = response.json()

    return raw_data['data']['result']


def transform_data(raw_data: list) -> pandas.DataFrame | None:
    df = pandas.DataFrame(raw_data)

    if df.empty:
        return None

    df = df[['id', 'name', 'height', 'weight']]
    df['weight'] = df['weight'] / 10
    df['height'] = df['height'] / 10
    df['imc'] = round(df['weight'] / (df['height'] ** 2), 2)

    return df


def load_data(data_frame: pandas.DataFrame, connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()

    for item in data_frame.itertuples():
        cursor.execute(
            """
            INSERT INTO pokemon (id, name, height, weight, imc)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET name = excluded.name, height = excluded.height, weight = excluded.weight, imc = excluded.imc
            """,
            (item.id, item.name, item.height, item.weight, item.imc)
        )

    cursor.close()


def main() -> None:
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

    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
