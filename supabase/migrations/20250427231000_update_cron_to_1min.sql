-- Remove old hourly cron job
SELECT cron.unschedule('auto-bot-hourly');

-- Create new 1-minute cron job
SELECT cron.schedule(
  'auto-bot-every-minute',  -- job name
  '* * * * *',              -- every minute
  $$SELECT net.http_post(
    url := 'https://isanhutzyctcjygjhzbn.supabase.co/functions/v1/auto-bot',
    headers := '{"Content-Type": "application/json", "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlzYW5odXR6eWN0Y2p5Z2poemJuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE4MjI2MjUsImV4cCI6MjA1NzM5ODYyNX0.Qo_hqGL7iKu0JCbM6aBrvZIi44C62r7i4DjCa4aKRcw"}'::jsonb,
    body := '{}'::jsonb
  )$$
);

-- Verify it's scheduled
SELECT jobname, schedule FROM cron.job WHERE jobname = 'auto-bot-every-minute';
