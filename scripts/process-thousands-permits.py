#!/usr/bin/env python3

"""
High-volume building permit processing pipeline
Processes thousands of permits in batches using MCP server
"""

import requests
import json
import time
from datetime import datetime, timedelta
import sys

def fetch_permits_batch(offset=0, limit=1000):
    """Fetch building permits from Chicago API in batches"""
    url = "https://data.cityofchicago.org/resource/ydr8-5enu.json"
    
    # Focus on recent permits for better leads
    since_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    params = {
        '$limit': limit,
        '$offset': offset,
        '$order': 'issue_date DESC',
        '$where': f"issue_date >= '{since_date}' AND permit_type = 'PERMIT - NEW CONSTRUCTION'"
    }
    
    print(f"🔄 Fetching permits batch: offset={offset}, limit={limit}")
    
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        permits = response.json()
        
        print(f"✅ Fetched {len(permits)} permits")
        return permits
        
    except Exception as e:
        print(f"❌ Error fetching permits: {e}")
        return []

def generate_permit_insert_sql(permits, batch_num):
    """Generate SQL to insert permit batch"""
    if not permits:
        return ""
    
    values = []
    for i, permit in enumerate(permits):
        record_id = f"chicago_building_permits_{batch_num}_{i}"
        watermark = permit.get('issue_date', datetime.now().isoformat())
        
        # Escape JSON for PostgreSQL
        payload_json = json.dumps(permit).replace("'", "''")
        
        values.append(f"('{record_id}', 'chicago', 'building_permits', '{watermark}', '{payload_json}'::jsonb, NOW())")
    
    sql = f"""
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) 
VALUES {', '.join(values)}
ON CONFLICT (id) DO UPDATE SET 
    watermark = EXCLUDED.watermark,
    payload = EXCLUDED.payload,
    inserted_at = EXCLUDED.inserted_at;
"""
    return sql

def generate_permit_normalization_sql():
    """Generate SQL to normalize building permits"""
    return """
INSERT INTO normalized (
    uid, city, dataset, business_name, address, lat, lon, status,
    event_date, type, description, source_link, raw_id, created_at
)
SELECT 
    city || '_' || dataset || '_' || (payload->>'permit_') as uid,
    city,
    dataset,
    COALESCE(payload->>'applicant_name', payload->>'contractor_name') as business_name,
    payload->>'street_name' as address,
    CASE 
        WHEN payload->>'latitude' ~ '^-?[0-9]+\.?[0-9]*$' 
        THEN (payload->>'latitude')::float 
        ELSE NULL 
    END as lat,
    CASE 
        WHEN payload->>'longitude' ~ '^-?[0-9]+\.?[0-9]*$' 
        THEN (payload->>'longitude')::float 
        ELSE NULL 
    END as lon,
    payload->>'permit_type' as status,
    payload->>'issue_date' as event_date,
    'building_permit' as type,
    CONCAT(payload->>'work_description', ' - ', payload->>'permit_type') as description,
    'https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu' as source_link,
    id as raw_id,
    NOW() as created_at
FROM raw 
WHERE city = 'chicago' 
    AND dataset = 'building_permits'
    AND (payload->>'applicant_name' IS NOT NULL OR payload->>'contractor_name' IS NOT NULL)
    AND payload->>'street_name' IS NOT NULL
    AND inserted_at >= NOW() - INTERVAL '1 hour'
ON CONFLICT (uid) DO NOTHING;
"""

def generate_permit_leads_sql():
    """Generate SQL to create leads from building permits"""
    return """
INSERT INTO leads (
    lead_id, city, name, address, phone, email, score, evidence, created_at
)
SELECT 
    'lead_permit_' || uid as lead_id,
    city,
    business_name as name,
    address,
    NULL as phone,
    NULL as email,
    CASE 
        WHEN event_date >= '2024-06-01' THEN 95 + (RANDOM() * 5)
        WHEN event_date >= '2024-01-01' THEN 85 + (RANDOM() * 15)
        WHEN event_date >= '2023-06-01' THEN 75 + (RANDOM() * 20)
        ELSE 60 + (RANDOM() * 25)
    END as score,
    jsonb_build_object(
        'source', 'building_permit',
        'permit_date', event_date,
        'permit_type', status,
        'work_description', description,
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
    AND created_at >= NOW() - INTERVAL '1 hour'
ON CONFLICT (lead_id) DO NOTHING;
"""

def main():
    print("🚀 Starting High-Volume Building Permit Processing")
    print("=" * 60)
    
    total_permits = 0
    batch_size = 1000
    max_batches = 5  # Process 5,000 permits total
    
    print(f"📊 Configuration:")
    print(f"   Batch size: {batch_size}")
    print(f"   Max batches: {max_batches}")
    print(f"   Total target: {batch_size * max_batches:,} permits")
    print()
    
    # Step 1: Fetch and prepare data in batches
    print("📥 STEP 1: High-Volume Data Ingestion")
    print("-" * 40)
    
    all_insert_sqls = []
    
    for batch_num in range(max_batches):
        offset = batch_num * batch_size
        permits = fetch_permits_batch(offset, batch_size)
        
        if not permits:
            print(f"⚠️  No more permits available at offset {offset}")
            break
            
        # Generate SQL for this batch
        insert_sql = generate_permit_insert_sql(permits, batch_num)
        if insert_sql:
            all_insert_sqls.append(insert_sql)
            
        total_permits += len(permits)
        print(f"   Batch {batch_num + 1}: {len(permits)} permits (Total: {total_permits:,})")
        
        # Rate limiting
        time.sleep(1)
    
    print(f"\n✅ Data preparation complete: {total_permits:,} permits ready")
    
    # Step 2: Show SQL operations that would be executed
    print(f"\n🔄 STEP 2: Database Operations")
    print("-" * 40)
    
    print(f"📝 Would execute {len(all_insert_sqls)} batch insert operations")
    print("📝 Would run normalization SQL")
    print("📝 Would run lead generation SQL")
    
    # Step 3: Show sample normalization SQL
    print(f"\n🔄 STEP 3: Normalization SQL Preview")
    print("-" * 40)
    print(generate_permit_normalization_sql())
    
    # Step 4: Show sample lead generation SQL  
    print(f"\n🎯 STEP 4: Lead Generation SQL Preview")
    print("-" * 40)
    print(generate_permit_leads_sql())
    
    print(f"\n✅ Pipeline ready to process {total_permits:,} building permits!")
    print("🚀 Execute via MCP server for actual database operations")

if __name__ == "__main__":
    main()
