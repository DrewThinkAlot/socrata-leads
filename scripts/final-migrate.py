#!/usr/bin/env python3

import sqlite3
import json
import sys
import time

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

def process_continuous_batches(start_offset=136, batch_size=5000, max_batches=50):
    """Process continuous batches and output SQL for MCP execution"""
    
    conn = sqlite3.connect('data/pipeline.db')
    current_offset = start_offset
    
    for batch_num in range(1, max_batches + 1):
        cursor = conn.cursor()
        
        # Get batch of records
        cursor.execute("""
            SELECT id, city, dataset, watermark, payload, inserted_at 
            FROM raw 
            ORDER BY id 
            LIMIT ? OFFSET ?
        """, (batch_size, current_offset))
        
        rows = cursor.fetchall()
        
        if not rows:
            print(f"✅ Migration completed - no more records at offset {current_offset}")
            break
        
        # Build VALUES clauses
        values_list = []
        for row in rows:
            id_val = escape_sql_string(row[0])
            city_val = escape_sql_string(row[1])
            dataset_val = escape_sql_string(row[2])
            watermark_val = escape_sql_string(row[3])
            payload_val = format_json_field(row[4])
            inserted_at_val = escape_sql_string(row[5])
            
            values_list.append(f"({id_val}, {city_val}, {dataset_val}, {watermark_val}, {payload_val}, {inserted_at_val})")
        
        # Output SQL for MCP execution
        print(f"-- BATCH {batch_num}: {len(rows)} records (offset {current_offset})")
        print("BEGIN;")
        print("INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES")
        
        # Split into chunks of 1000 for better performance
        chunk_size = 1000
        for i in range(0, len(values_list), chunk_size):
            chunk = values_list[i:i + chunk_size]
            if i > 0:
                print("INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES")
            print(",\n".join(chunk) + ";")
        
        print("COMMIT;")
        print(f"-- End Batch {batch_num}")
        print()
        
        current_offset += len(rows)
        
        # Small delay between batches
        time.sleep(0.1)
    
    conn.close()
    print(f"🎯 Generated {batch_num} batches, final offset: {current_offset}")

def main():
    start_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 136
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    max_batches = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    
    print(f"🚀 Final Migration Pipeline")
    print(f"Start: {start_offset}, Batch: {batch_size}, Max: {max_batches}")
    print()
    
    process_continuous_batches(start_offset, batch_size, max_batches)

if __name__ == "__main__":
    main()
