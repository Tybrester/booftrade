-- Check what cron jobs currently exist
SELECT jobname, schedule, active 
FROM cron.job;

-- Create the 1-minute auto-bot cron job (will fail silently if exists)
DO $$
BEGIN
  -- Try to unschedule if exists (ignore errors)
  BEGIN
    PERFORM cron.unschedule('auto-bot-every-minute');
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  
  -- Create fresh schedule
  PERFORM cron.schedule(
    'auto-bot-every-minute',
    '* * * * *',  -- Every minute
    'SELECT net.http_post(url := ''https://isanhutzyctcjygjhzbn.supabase.co/functions/v1/auto-bot'', headers := ''{"Content-Type": "application/json", "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlzYW5odXR6eWN0Y2p5Z2poemJuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE4MjI2MjUsImV4cCI6MjA1NzM5ODYyNX0.Qo_hqGL7iKu0JCbM6aBrvZIi44C62r7i4DjCa4aKRcw"}''::jsonb, body := ''{}''::jsonb)'
  );
END $$;

-- Verify it's scheduled
SELECT jobname, schedule, active
FROM cron.job 
WHERE jobname = 'auto-bot-every-minute';
