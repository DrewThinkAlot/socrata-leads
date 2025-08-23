#!/usr/bin/env python3

import sqlite3
import json
import sys
import time
import subprocess

def escape_sql_string(s):
    """Escape single quotes in SQL strings"""
    if s is None:
        return 'NULL'
    return f"'{str(s).replace(chr(39), chr(39) + chr(39))}'"

def format_json_field(json_str):
    """Format JSON field for PostgreSQL JSONB insertion"""
    if json_str is None:
        return 'NULL'
    try:
        parsed = json.loads(json_str)
        json_escaped = json.dumps(parsed).replace("'", "''")
        return f"'{json_escaped}'::jsonb"
    except:
        return "'{}'"

def execute_via_mcp(sql_query):
    """Print SQL for manual MCP execution"""
    print("=== SQL FOR MCP EXECUTION ===")
    print(sql_query)
    print("=== END SQL ===")
    return True

def migrate_raw_ultra_batch(offset, batch_size=25000):
    """Migrate raw table data in ultra-large batches"""
    
    # Connect to SQLite
    conn = sqlite3.connect('data/pipeline.db')
    cursor = conn.cursor()
    
    # Get batch of records
    cursor.execute("""
        SELECT id, city, dataset, watermark, payload, inserted_at 
        FROM raw 
        ORDER BY id 
        LIMIT ? OFFSET ?
    """, (batch_size, offset))
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return 0, False
    
    print(f"Processing batch: {offset} to {offset + len(rows)}")
    
    # Build single massive INSERT with multiple VALUES
    values_list = []
    for row in rows:
        id_val = escape_sql_string(row[0])
        city_val = escape_sql_string(row[1])
        dataset_val = escape_sql_string(row[2])
        watermark_val = escape_sql_string(row[3])
        payload_val = format_json_field(row[4])
        inserted_at_val = escape_sql_string(row[5])
        
        values_list.append(f"({id_val}, {city_val}, {dataset_val}, {watermark_val}, {payload_val}, {inserted_at_val})")
    
    # Create single INSERT statement
    sql = f"""BEGIN;
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES 
{','.join(values_list)};
COMMIT;"""
    
    # Execute via MCP
    success = execute_via_mcp(sql)
    
    if success:
        print(f"✓ Successfully inserted {len(rows)} records")
        return len(rows), True
    else:
        print(f"✗ Failed to insert batch at offset {offset}")
        return 0, False

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 ultra-fast-migrate.py <start_offset> [batch_size]")
        sys.exit(1)
    
    start_offset = int(sys.argv[1])
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 25000
    
    print(f"Starting ultra-fast migration from offset {start_offset} with batch size {batch_size}")
    
    total_processed = 0
    current_offset = start_offset
    
    while True:
        processed, has_more = migrate_raw_ultra_batch(current_offset, batch_size)
        
        if processed == 0:
            break
            
        total_processed += processed
        current_offset += processed
        
        print(f"Progress: {current_offset} records processed")
        
        # Small delay to avoid overwhelming MCP
        time.sleep(2)
        
        if not has_more:
            break
    
    print(f"Migration completed! Total processed: {total_processed}")

if __name__ == "__main__":
    main()
