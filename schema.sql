CREATE DATABASE IF NOT EXISTS twitter_rdb;
USE twitter_rdb;

DROP TABLE IF EXISTS TWEET;
DROP TABLE IF EXISTS FOLLOWS;

CREATE TABLE FOLLOWS (
  follower_id INT NOT NULL,
  followee_id INT NOT NULL,
  PRIMARY KEY (follower_id, followee_id),
  INDEX idx_followee (followee_id)
) ENGINE=InnoDB;

CREATE TABLE TWEET (
  tweet_id BIGINT NOT NULL AUTO_INCREMENT,
  user_id INT NOT NULL,
  tweet_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  tweet_text VARCHAR(140) NOT NULL,
  PRIMARY KEY (tweet_id),
  INDEX idx_user_ts (user_id, tweet_ts)
) ENGINE=InnoDB;
