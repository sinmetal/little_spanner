CREATE TABLE Tweet3 (
  Id STRING(MAX) NOT NULL,
  SearchId STRING(MAX) NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

CREATE INDEX Tweet3SearchId
ON Tweet3 (
SearchId
);