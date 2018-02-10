CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TABLE long_tweets(timestamp timestamp, username text, text text, PRIMARY KEY(timestamp, username));
