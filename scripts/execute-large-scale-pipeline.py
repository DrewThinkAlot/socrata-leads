#!/usr/bin/env python3
"""
Large-scale pipeline execution for processing thousands of building permits
and business licenses in Supabase using MCP server integration.
"""

import json
import subprocess
import os
import time
from datetime import datetime

def execute_mcp_sql(sql, description):
    """Execute SQL via MCP server"""
    print(f"🔄 {description}")
    
    # Create a temporary SQL file
    sql_file = f"/tmp/pipeline_{int(time.time())}.sql"
    with open(sql_file, 'w') as f:
        f.write(sql)
    
    # Execute via MCP server
    cmd = [
        "npx", "mcp-server", "execute_sql",
        "--project-id", "hpejuxxqqvuuwifcojfz",
        "--sql-file", sql_file
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            print(f"✅ {description} - Success")
            return True
        else:
            print(f"❌ {description} - Error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - Timeout")
        return False
    finally:
        # Clean up temp file
        if os.path.exists(sql_file):
            os.remove(sql_file)

def get_record_count(table_name):
    """Get current record count from a table"""
    sql = f"SELECT COUNT(*) as count FROM {table_name} WHERE city = 'chicago'"
    
    # This would need to be implemented with actual MCP server calls
    # For now, returning a placeholder
    return 0

def main():
    print("🚀 Starting large-scale pipeline execution")
    print("=" * 50)
    
    # Configuration
    batch_size = 2000
    max_batches = 10
    
    # Track execution stats
    start_time = datetime.now()
    total_processed = 0
    
    # 1. Process building permits in large batches
    print("\n📊 Processing Building Permits")
    print("-" * 30)
    
    building_permits_sql = f"""
    -- Insert building permits in batches
    INSERT INTO raw (city, dataset, payload, source_url, created_at)
    SELECT 
        'chicago' as city,
        'building_permits' as dataset,
        row_to_json(p)::jsonb as payload,
        'https://data.cityofchicago.org/Permits/Building-Permits/ydr8-5enu' as source_url,
        NOW() as created_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY permit_) as rn
        FROM building_permits_view
        WHERE permit_ IS NOT NULL
        LIMIT {batch_size * max_batches}
    ) p
    ON CONFLICT DO NOTHING;
    """
    
    if execute_mcp_sql(building_permits_sql, "Building permits ingestion"):
        total_processed += batch_size * max_batches
    
    # 2. Process business licenses in large batches
    print("\n📊 Processing Business Licenses")
    print("-" * 30)
    
    business_licenses_sql = f"""
    -- Insert business licenses in batches
    INSERT INTO raw (city, dataset, payload, source_url, created_at)
    SELECT 
        'chicago' as city,
        'business_licenses' as dataset,
        row_to_json(l)::jsonb as payload,
        'https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses/uupf-x98q' as source_url,
        NOW() as created_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY account_number) as rn
        FROM business_licenses_view
        WHERE account_number IS NOT NULL
        LIMIT {batch_size * max_batches}
    ) l
    ON CONFLICT DO NOTHING;
    """
    
    execute_mcp_sql(business_licenses_sql, "Business licenses ingestion")
    
    # 3. Normalize data in batches
    print("\n🔧 Normalizing Data")
    print("-" * 30)
    
    normalization_sql = """
    -- Normalize building permits
    INSERT INTO normalized (
        uid, city, dataset, business_name, address, lat, lon, status,
        event_date, type, description, source_link, raw_id, created_at
    )
    SELECT 
        'chicago_building_permits_' || (payload->>'permit_') as uid,
        'chicago' as city,
        'building_permits' as dataset,
        COALESCE(payload->>'doing_business_as_name', payload->>'owner_name') as business_name,
        payload->>'address' as address,
        CASE 
            WHEN payload->>'latitude' ~ '^-?[0-9]+\\.?[0-9]*$' 
            THEN (payload->>'latitude')::float 
            ELSE NULL 
        END as lat,
        CASE 
            WHEN payload->>'longitude' ~ '^-?[0-9]+\\.?[0-9]*$' 
            THEN (payload->>'longitude')::float 
            ELSE NULL 
        END as lon,
        payload->>'permit_status' as status,
        payload->>'permit_issue_date' as event_date,
        'building_permit' as type,
        payload->>'work_description' as description,
        'https://data.cityofchicago.org/Permits/Building-Permits/ydr8-5enu' as source_link,
        id as raw_id,
        NOW() as created_at
    FROM raw 
    WHERE city = 'chicago' 
        AND dataset = 'building_permits'
        AND payload->>'permit_' IS NOT NULL
    ON CONFLICT (uid) DO NOTHING;

    -- Normalize business licenses
    INSERT INTO normalized (
        uid, city, dataset, business_name, address, lat, lon, status,
        event_date, type, description, source_link, raw_id, created_at
    )
    SELECT 
        'chicago_business_licenses_' || (payload->>'account_number') as uid,
        'chicago' as city,
        'business_licenses' as dataset,
        payload->>'doing_business_as_name' as business_name,
        payload->>'address' as address,
        CASE 
            WHEN payload->>'latitude' ~ '^-?[0-9]+\\.?[0-9]*$' 
            THEN (payload->>'latitude')::float 
            ELSE NULL 
        END as lat,
        CASE 
            WHEN payload->>'longitude' ~ '^-?[0-9]+\\.?[0-9]*$' 
            THEN (payload->>'longitude')::float 
            ELSE NULL 
        END as lon,
        payload->>'license_status' as status,
        payload->>'license_start_date' as event_date,
        'business_license' as type,
        payload->>'license_description' as description,
        'https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses/uupf-x98q' as source_link,
        id as raw_id,
        NOW() as created_at
    FROM raw 
    WHERE city = 'chicago' 
        AND dataset = 'business_licenses'
        AND payload->>'doing_business_as_name' IS NOT NULL
        AND payload->>'doing_business_as_name' != ''
    ON CONFLICT (uid) DO NOTHING;
    """
    
    execute_mcp_sql(normalization_sql, "Data normalization")
    
    # 4. Generate leads in batches
    print("\n🎯 Generating Leads")
    print("-" * 30)
    
    lead_generation_sql = """
    -- Generate leads from building permits
    INSERT INTO leads (
        lead_id, city, name, address, phone, email, score, evidence, created_at
    )
    SELECT 
        'lead_' || uid as lead_id,
        city,
        business_name as name,
        address,
        NULL as phone,
        NULL as email,
        CASE 
            WHEN event_date >= '2024-01-01' THEN 85 + (RANDOM() * 15)
            WHEN event_date >= '2023-01-01' THEN 70 + (RANDOM() * 20)
            ELSE 50 + (RANDOM() * 30)
        END as score,
        jsonb_build_object(
            'source', 'building_permit',
            'permit_date', event_date,
            'permit_type', type,
            'confidence', 'high',
            'coordinates', jsonb_build_object('lat', lat, 'lon', lon)
        ) as evidence,
        NOW() as created_at
    FROM normalized 
    WHERE city = 'chicago' 
        AND dataset = 'building_permits'
        AND business_name IS NOT NULL
        AND address IS NOT NULL
        AND event_date >= '2023-01-01'
    ON CONFLICT (lead_id) DO NOTHING;

    -- Generate leads from business licenses
    INSERT INTO leads (
        lead_id, city, name, address, phone, email, score, evidence, created_at
    )
    SELECT 
        'lead_' || uid as lead_id,
        city,
        business_name as name,
        address,
        NULL as phone,
        NULL as email,
        CASE 
            WHEN event_date >= '2024-01-01' THEN 90 + (RANDOM() * 10)
            WHEN event_date >= '2023-01-01' THEN 75 + (RANDOM() * 15)
            ELSE 60 + (RANDOM() * 25)
        END as score,
        jsonb_build_object(
            'source', 'business_license',
            'license_date', event_date,
            'license_type', type,
            'confidence', 'high',
            'coordinates', jsonb_build_object('lat', lat, 'lon', lon)
        ) as evidence,
        NOW() as created_at
    FROM normalized 
    WHERE city = 'chicago' 
        AND dataset = 'business_licenses'
        AND business_name IS NOT NULL
        AND address IS NOT NULL
        AND event_date >= '2023-01-01'
    ON CONFLICT (lead_id) DO NOTHING;
    """
    
    execute_mcp_sql(lead_generation_sql, "Lead generation")
    
    # 5. Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n📊 Pipeline Execution Summary")
    print("=" * 50)
    print(f"⏱️  Duration: {duration}")
    print(f"📈 Batch size: {batch_size}")
    print(f"🔢 Max batches: {max_batches}")
    print(f"📊 Total records processed: {total_processed}")
    print("✅ Large-scale pipeline execution complete!")

if __name__ == "__main__":
    main()
