from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
import mysql.connector


@dataclass(frozen=True)
class Tweet:
    """
    Data container representing a tweet.

    Parameters
    ----------
    tweet_id : int
        Unique identifier of the tweet.
    user_id : int
        ID of the user who posted the tweet.
    tweet_ts : str
        Timestamp when the tweet was created.
    tweet_text : str
        Content of the tweet.

    Returns
    -------
    Tweet
        A Tweet object.
    """
    tweet_id: int
    user_id: int
    tweet_ts: str
    tweet_text: str


class TwitterMySQLAPI:
    """
    API layer for a simplified Twitter backend using MySQL.
    Benchmark programs must call these methods instead of writing SQL directly.
    """

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        """
        Initialize the Twitter MySQL API.

        Parameters
        ----------
        host : str
            MySQL server hostname.
        user : str
            MySQL username.
        password : str
            MySQL password.
        database : str
            Name of the database to connect to.
        port : int, optional
            MySQL server port (default is 3306).

        Returns
        -------
        None
        """
        self.conn_args = dict(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )

    def _connect(self):
        """
        Create a new MySQL database connection.

        Parameters
        ----------
        None

        Returns
        -------
        mysql.connector.connection.MySQLConnection
            An active MySQL connection.
        """
        return mysql.connector.connect(**self.conn_args)

    def post_tweet(self, user_id: int, tweet_text: str) -> int:
        """
        Insert a single tweet into the database.
        (Must be called one tweet at a time; no batching.)

        Parameters
        ----------
        user_id : int
            ID of the user posting the tweet.
        tweet_text : str
            Text content of the tweet (<= 140 characters).

        Returns
        -------
        int
            The auto-generated tweet_id of the inserted tweet.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO TWEET (user_id, tweet_text) VALUES (%s, %s)",
                (user_id, tweet_text),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()

    def get_home_timeline(self, user_id: int) -> List[Tweet]:
        """
        Retrieve the home timeline for a user.
        The timeline consists of the 10 most recent tweets
        from users that the given user follows.

        Parameters
        ----------
        user_id : int
            ID of the user requesting the timeline.

        Returns
        -------
        List[Tweet]
            A list of up to 10 Tweet objects ordered
            from newest to oldest.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT t.tweet_id, t.user_id, t.tweet_ts, t.tweet_text
                FROM FOLLOWS f
                JOIN TWEET t ON t.user_id = f.followee_id
                WHERE f.follower_id = %s
                ORDER BY t.tweet_ts DESC
                LIMIT 10
                """,
                (user_id,),
            )
            rows = cur.fetchall()
            return [Tweet(*row) for row in rows]
        finally:
            conn.close()

    def get_random_follower_id(self) -> Optional[int]:
        """
        Select a random follower_id from the FOLLOWS table.
        Used for benchmarking timeline queries.

        Parameters
        ----------
        None

        Returns
        -------
        Optional[int]
            A randomly selected follower_id, or None if
            the FOLLOWS table is empty.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT follower_id FROM FOLLOWS ORDER BY RAND() LIMIT 1")
            row = cur.fetchone()
            return int(row[0]) if row else None
        finally:
            conn.close()

    def load_follows_csv(self, csv_path: str, has_header: bool = True) -> int:
        """
        Load follow relationships from a CSV file into the database.
        Duplicate relationships are ignored.

        Parameters
        ----------
        csv_path : str
            File path to follows.csv.
        has_header : bool, optional
            Whether the CSV file contains a header row
            (default is True).

        Returns
        -------
        int
            Number of rows inserted into the FOLLOWS table.
        """
        inserted = 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            with open(csv_path, "r", encoding="utf-8") as f:
                if has_header:
                    next(f, None)
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    follower_id_str, followee_id_str = line.split(",", 1)
                    cur.execute(
                        "INSERT IGNORE INTO FOLLOWS (follower_id, followee_id) VALUES (%s, %s)",
                        (int(follower_id_str), int(followee_id_str)),
                    )
                    inserted += cur.rowcount
            conn.commit()
            return inserted
        finally:
            conn.close()
