from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
import mysql.connector


@dataclass(frozen=True)
class Tweet:
    """
    Represents a single tweet returned by the API.

    Parameters:
        tweet_id (int): Unique identifier for the tweet.
        user_id (int): ID of the user who posted the tweet.
        tweet_ts (str): Timestamp when the tweet was created.
        tweet_text (str): Content of the tweet.

    Returns:
        Tweet: An immutable Tweet object.
    """
    tweet_id: int
    user_id: int
    tweet_ts: str
    tweet_text: str


class TwitterMySQLAPI:
    """
    MySQL-backed implementation of the Twitter API.

    The timing / benchmark code must interact ONLY with this API.
    The underlying database implementation is hidden from callers.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
    ) -> None:
        """
        Initialize the API and establish a database connection.

        Parameters:
            host (str): MySQL host name.
            user (str): MySQL username.
            password (str): MySQL password.
            database (str): Database name.
            port (int): MySQL port (default 3306).

        Returns:
            None
        """
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            autocommit=False,
        )
        self.cur = self.conn.cursor()

    def post_tweet(self, user_id: int, tweet_text: str) -> int:
        """
        Post a single tweet.

        Parameters:
            user_id (int): ID of the user posting the tweet.
            tweet_text (str): Text content of the tweet.

        Returns:
            int: The tweet_id of the newly inserted tweet.
        """
        self.cur.execute(
            "INSERT INTO TWEET (user_id, tweet_text) VALUES (%s, %s)",
            (user_id, tweet_text),
        )
        self.conn.commit()
        return self.cur.lastrowid

    def get_home_timeline(self, user_id: int) -> List[Tweet]:
        """
        Retrieve the home timeline for a user.

        The home timeline consists of the 10 most recent tweets
        from users that the given user follows.

        Parameters:
            user_id (int): ID of the user requesting the timeline.

        Returns:
            List[Tweet]: A list of up to 10 Tweet objects ordered
            from newest to oldest.
        """
        self.cur.execute(
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
        rows = self.cur.fetchall()
        return [Tweet(*row) for row in rows]

    def get_random_follower_id(self) -> Optional[int]:
        """
        Select a random follower ID from the FOLLOWS table.

        Used by the benchmark to generate realistic API calls.

        Parameters:
            None

        Returns:
            Optional[int]: A follower_id if one exists,
            otherwise None.
        """
        self.cur.execute(
            "SELECT follower_id FROM FOLLOWS ORDER BY RAND() LIMIT 1"
        )
        row = self.cur.fetchone()
        return int(row[0]) if row else None

    def load_follows_csv(self, csv_path: str, has_header: bool = True) -> int:
        """
        Load follow relationships from a CSV file into the database.

        Parameters:
            csv_path (str): Path to the CSV file.
            has_header (bool): Whether the first row is a header.

        Returns:
            int: Number of rows inserted (duplicates ignored).
        """
        inserted = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            if has_header:
                next(f, None)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                follower_id_str, followee_id_str = line.split(",", 1)
                self.cur.execute(
                    """
                    INSERT IGNORE INTO FOLLOWS (follower_id, followee_id)
                    VALUES (%s, %s)
                    """,
                    (int(follower_id_str), int(followee_id_str)),
                )
                inserted += self.cur.rowcount
        self.conn.commit()
        return inserted

    def close(self) -> None:
        """
        Close the database cursor and connection.

        Parameters:
            None

        Returns:
            None
        """
        self.cur.close()
        self.conn.close()
