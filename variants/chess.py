from reretry import retry
import datetime
import requests
from typing import Iterator, List, Sequence

import dlt
from dlt.common.typing import TDataItem, StrAny
from dlt.extract.source import DltResource

OFFICIAL_CHESS_API_URL = "https://api.chess.com/pub/"
UNOFFICIAL_CHESS_API_URL = "https://www.chess.com/callback/"


@dlt.source
def chess(players: List[str], start_month: str = None, end_month: str = None) -> Sequence[DltResource]:
    """A dlt source for the chess.com api. It groups several resources (in this case chess.com API endpoints) containing various types of data: ie user profiles
    or chess match results

    Args:
        chess_url (str): Url of the chess.com api
        players (list): A list of the player usernames for which to get the data
        start_month ("YYYY/MM", optional): Filters out all the matches happening before `start_month`
        end_month ("YYYY/MM", optional): Filters out all the matches happening after `end_month`

    Returns:
        A list of following resources that you can select from
            "players_profiles" - yields profiles of the `players`,
            "players_archives" - yields list of archives with games available to the `players`,
            "players_games" - yields games of `players` in specified time period,
            "players_online_status" - yields online status of players,
    """
    return (
        players_profiles(players),
        players_archives(players),
        players_games(players, start_month=start_month, end_month=end_month),
        players_online_status(players)
    )


@retry(tries=10, delay=1, backoff=1.1, logger=None)
def _get_url_with_retry(url: str) -> StrAny:
    r = requests.get(url)
    r.raise_for_status()
    return r.json()  # type: ignore


def _get_path_with_retry(path: str) -> StrAny:
    return _get_url_with_retry(f"{OFFICIAL_CHESS_API_URL}{path}")  # type: ignore


@dlt.resource(write_disposition="replace")
def players_profiles(players: List[str]) -> Iterator[TDataItem]:
    """Yields player profiles for a list of player usernames"""

    # get profiles in parallel by decorating the http request with defer
    @dlt.defer
    def _get_profile(username: str) -> TDataItem:
        return _get_path_with_retry(f"player/{username}")

    for username in players:
        yield _get_profile(username)


@dlt.resource(write_disposition="replace", selected=False)
def players_archives(players: List[str]) -> Iterator[List[TDataItem]]:
    """Yields url to game archives for specified players."""

    for username in players:
        data = _get_path_with_retry(f"player/{username}/games/archives")
        yield data.get("archives", [])


@dlt.resource(write_disposition="append")
def players_games(players: List[str], start_month: str = None, end_month: str = None) -> Iterator[List[TDataItem]]:
    """Yields `players` games that happened between `start_month` and `end_month`. See the `chess` source documentation for details."""
    # do a simple validation to prevent common mistakes in month format
    if start_month and start_month[4] != "/":
        raise ValueError(start_month)
    if end_month and end_month[4] != "/":
        raise ValueError(end_month)

    # get a list of already checked archives, you will read more about the dlt.state on Day 3 of our workshop
    # from your point of view, the state is python dictionary that will have the same content the next time this function is called
    checked_archives = dlt.state().setdefault("archives", [])
    # get player archives, note that you can call the resource like any other function and just iterate it like a list
    archives = players_archives(players)

    # get archives in parallel by decorating the http request with defer
    @dlt.defer
    def _get_archive(url) -> TDataItem:
        print(f"Getting archive from {url}")
        return _get_url_with_retry(url).get("games", [])

    # enumerate the archives
    url: str = None
    for url in archives:  # type: ignore
        # the `url` format is https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
        if start_month and url[-7:] < start_month:
            continue
        if end_month and url[-7:] > end_month:
            continue
        # do not download archive again
        if url in checked_archives:
            continue
        else:
            checked_archives.append(url)
        # get the filtered archive
        yield _get_archive(url)


@dlt.resource(write_disposition="append")
def players_online_status(players: List[str]) -> Iterator[TDataItem]:
    """Returns current online status for a list of players"""
    # we'll use unofficial endpoint to get online status, the official seems to be removed
    for player in players:
        status = _get_url_with_retry("%suser/popup/%s" % (UNOFFICIAL_CHESS_API_URL, player))
        # return just relevant selection
        yield {
            "username": player,
            "onlineStatus": status["onlineStatus"],
            "lastLoginDate": status["lastLoginDate"],
            "check_time": datetime.datetime.now()  # dlt can deal with native python dates
        }


if __name__ == "__main__" :

    # our pipeline will
    # 1. Request the monthly archives available per player
    # 2. Optionally filter the archives for the ones containing the time frame requested
    # 3. Get the games of those archives
    # 4. Get players online status (optional)

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(pipeline_name="chess", destination="duckdb", dataset_name="chess_data", full_refresh=False)

    # load the data from the chess source
    info = pipeline.run(
        chess(
            ['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'],
            start_month='2022/10',
            end_month='2022/12'
        ).with_resources("players_games", "players_profiles")
    )
    print(info)
