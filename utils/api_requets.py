import pickle
from dataclasses import dataclass

import requests
from pandas import Timestamp


@dataclass
class User:
    id: int
    name: str
    gender: str
    birthdate: Timestamp
    country: str


def user_wants_to_try_again():
    user_input = input("Try again? [y/n]:")

    if len(user_input) < 1:
        return user_wants_to_try_again()

    match user_input[0].lower():
        case "y":
            True
        case "n":
            False
        case _:
            return user_wants_to_try_again()


def filter_utf16(entry: dict):
    """
    Remove entries with names containing utf-16 encoded characters like arabic or japanese
    """
    if ord(entry["name"]["first"][0]) > 255:
        return False

    return True


def clean_data(users: list[dict]):
    user_id = 0
    res = []

    for user in filter(filter_utf16, users):
        res.append(
            User(
                id=user_id,
                name=f"{user["name"]["first"]} {user["name"]["last"]}",
                gender=user["gender"][0],
                birthdate=Timestamp(user["dob"]["date"]).tz_localize(None),
                country=user["location"]["country"],
            )
        )
        user_id += 1

    return res


def get_api_request(url: str, n_results=3000, seed="southwind") -> list[dict]:
    """
    Get user data of 2000 "people" from api.
    Users whose name contain utf-16 encoded characters are discarded.
    """
    headers = {
        "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36"
    }

    response = requests.get(
        url=url,
        headers=headers,
        params={
            "dataType": "json",
            "seed": seed,
            "results": n_results,
        },
    )

    if not response.ok:
        print("API responded with code {reponse.status_code}")

        if user_wants_to_try_again():
            return get_api_request(url, n_results)
        else:
            return None

    data = clean_data(response.json()["results"])

    if len(data) < 2000:
        print(f"Not enought users found. Expected 2000 found {len(data)}.")
        exit()

    return data[:2000]


def save_api_data(path, data):
    with open(path, "wb") as file:
        pickle.dump(data, file)


def load_api_data(path):
    with open(path, "rb") as file:
        return pickle.load(file)
