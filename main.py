import math
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
            imc REAL,
            imc_category TEXT
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
    df['imc_category'] = pandas.cut(
        df['imc'],
        bins=[0, 18.5, 24.9, 29.9, 34.9, 39.9, math.inf],
        labels=['Underweight', 'Normal weight', 'Overweight', 'Obesity I', 'Obesity II', 'Obesity III']
    )

    return df


def load_data(data_frame: pandas.DataFrame, connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()

    for item in data_frame.itertuples():
        cursor.execute(
            """
            INSERT INTO pokemon (id, name, height, weight, imc, imc_category)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET name = excluded.name, height = excluded.height, weight = excluded.weight, imc = excluded.imc, imc_category = excluded.imc_category
            """,
            (item.id, item.name, item.height, item.weight, item.imc, item.imc_category)
        )

    cursor.close()


def query_heaviest_pokemon_by_imc_category(connection: sqlite3.Connection) -> list:
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT DISTINCT  p.*
        FROM pokemon AS p
        INNER JOIN (
            SELECT imc_category, MAX(imc) as max_imc
            FROM pokemon
            GROUP BY imc_category
        ) AS max_p on p.imc_category = max_p.imc_category AND p.imc = max_p.max_imc
        """
    )

    data = cursor.fetchall()
    cursor.close()

    return data


def query_weakest_pokemon_by_imc_category(connection: sqlite3.Connection) -> list:
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT p.*
        FROM pokemon AS p
        INNER JOIN (
            SELECT imc_category, MIN(imc) as min_imc
            FROM pokemon
            GROUP BY imc_category
        ) AS min_p on p.imc_category = min_p.imc_category AND p.imc = min_p.min_imc
        """
    )

    data = cursor.fetchall()
    cursor.close()

    return data


def query_count_by_imc_category(connection: sqlite3.Connection) -> list:
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT imc_category, COUNT(*) as count 
        FROM pokemon
        GROUP BY imc_category
        """
    )

    data = cursor.fetchall()
    cursor.close()

    return data


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

    count_by_imc_category = query_count_by_imc_category(connection)
    heaviest_pokemon_by_imc_category = query_heaviest_pokemon_by_imc_category(connection)
    weakest_pokemon_by_imc_category = query_weakest_pokemon_by_imc_category(connection)

    print("Pokemon by IMC category:", count_by_imc_category)
    print("Heaviest Pokemon by IMC category:", heaviest_pokemon_by_imc_category)
    print("Weakest Pokemon by IMC category:", weakest_pokemon_by_imc_category)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
