# Supabase Migration Guide for Socrata Leads Pipeline

This guide walks you through migrating your socrata-leads pipeline from SQLite to Supabase.

## Step 1: Create Supabase Project

1. Go to [supabase.com](https://supabase.com) and sign up/login
2. Click "New Project"
3. Choose your organization
4. Enter project details:
   - **Name**: `socrata-leads` (or your preferred name)
   - **Database Password**: Generate a strong password (save this!)
   - **Region**: Choose closest to your location
5. Click "Create new project"
6. Wait for project initialization (2-3 minutes)

## Step 2: Get Connection Details

From your Supabase project dashboard:

1. Go to **Settings** → **Database**
2. Copy the connection string under "Connection string" → "URI"
3. Go to **Settings** → **API**
4. Copy your:
   - Project URL
   - `anon` public key  
   - `service_role` secret key

## Step 3: Configure Environment

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Update your `.env` file with Supabase credentials:
   ```bash
   # Replace with your Supabase connection string
   DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
   
   # Optional: Supabase API credentials (for future features)
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_ANON_KEY=your-anon-key
   SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
   ```

## Step 4: Run Database Migrations

Run the PostgreSQL migrations to set up your database schema:

```bash
npm run migrate
```

This will create all necessary tables and indexes in your Supabase database.

## Step 5: Test Connection

Test that your pipeline can connect to Supabase:

```bash
# Test with a simple extraction
npm run extract -- --city chicago --limit 10
```

## Step 6: Migrate Existing Data (Optional)

If you have existing SQLite data you want to migrate:

```bash
# Use the migration script (will be created)
npm run migrate:data
```

## Step 7: Run Your Pipeline

Your existing commands will now work with Supabase:

```bash
# Extract data
npm run extract -- --city chicago

# Normalize data  
npm run normalize:fast -- --city chicago

# Score leads
npm run score:optimized -- --city chicago

# Export results
npm run export -- --city chicago --limit 100
```

## Benefits You'll See

- **Performance**: Faster queries on large datasets
- **Memory**: No more 2.8GB memory issues during scoring
- **Concurrency**: Parallel workers will run efficiently
- **Reliability**: No SQLite file corruption risks
- **Monitoring**: Query performance insights in Supabase dashboard
- **Backups**: Automatic point-in-time recovery

## Troubleshooting

### Connection Issues
- Verify your DATABASE_URL is correct
- Check that your IP is allowed (Supabase allows all IPs by default)
- Ensure your database password is correct

### Migration Issues
- Check the Supabase logs in your dashboard
- Verify PostgreSQL migrations ran successfully
- Check for any schema conflicts

### Performance Issues
- Monitor query performance in Supabase dashboard
- Consider adding additional indexes for your specific queries
- Use connection pooling for high-concurrency workloads

## Rollback Plan

If you need to rollback to SQLite:

1. Change `DATABASE_URL` back to `sqlite://./data/pipeline.db`
2. Run `npm run migrate` to ensure SQLite schema is up to date
3. Your pipeline will continue working with local SQLite

## Next Steps

Once migrated, consider:
- Setting up real-time subscriptions for your admin dashboard
- Using Supabase's built-in API for external integrations
- Implementing row-level security for multi-tenant scenarios
- Adding database monitoring and alerting
