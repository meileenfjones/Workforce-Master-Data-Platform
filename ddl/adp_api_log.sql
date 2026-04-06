CREATE TABLE workforce.dbo.adp_api_log
(
  id           INTEGER PRIMARY KEY IDENTITY (1,1),
  query_type   VARCHAR(MAX) NOT NULL,
  logged_at    SMALLDATETIME DEFAULT GETDATE(),
  query_count  INTEGER,
  update_count INTEGER,
  insert_count INTEGER,
  failure_step VARCHAR(MAX),
  is_success   BIT           DEFAULT 0
);
