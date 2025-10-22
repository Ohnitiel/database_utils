SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle'
  AND pid <> pg_backend_pid()
  AND datname = current_database();
