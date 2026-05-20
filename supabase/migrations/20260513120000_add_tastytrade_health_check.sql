-- Create table for connection health logs
CREATE TABLE IF NOT EXISTS connection_health_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  broker TEXT NOT NULL,
  status TEXT NOT NULL,
  error TEXT,
  checked_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_health_logs_user_broker ON connection_health_logs(user_id, broker);
CREATE INDEX IF NOT EXISTS idx_health_logs_checked_at ON connection_health_logs(checked_at DESC);

-- Grant permissions
GRANT SELECT, INSERT ON connection_health_logs TO authenticated;
GRANT SELECT, INSERT ON connection_health_logs TO service_role;

-- Enable RLS
ALTER TABLE connection_health_logs ENABLE ROW LEVEL SECURITY;

-- Users can only see their own logs
CREATE POLICY "Users can view their own health logs" 
  ON connection_health_logs 
  FOR SELECT 
  USING (auth.uid() = user_id);

-- Daily cron job to check Tastytrade health at 9:00 AM ET (market open)
SELECT cron.schedule(
  'tastytrade-daily-health-check',
  '0 9 * * MON-FRI',  -- 9:00 AM ET, Monday-Friday
  $$SELECT net.http_get(
    url:='https://isanhutzyctcjygjhzbn.supabase.co/functions/v1/tasty-health-check',
    headers:=('{"Authorization": "Bearer ' || current_setting('app.service_role_key', true) || '"}')::jsonb
  )$$
);

-- Comment explaining the cron job
COMMENT ON TABLE connection_health_logs IS 'Tracks daily health checks for broker connections';
