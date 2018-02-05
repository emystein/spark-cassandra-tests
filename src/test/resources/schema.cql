CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TABLE word_count (word text PRIMARY KEY, count int);

CREATE TABLE long_tweets(timestamp timestamp, username text, tweet text, PRIMARY KEY(timestamp, username));
