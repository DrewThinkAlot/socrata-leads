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

def migrate_raw_batch_with_execution(offset, batch_size=5000):
    """Generate and immediately execute raw table batch via MCP"""
    
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
    
    # Build single INSERT with multiple VALUES
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
    
    # Return SQL for MCP execution
    return sql, len(rows)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 execute-migrate.py <start_offset> [batch_size]")
        sys.exit(1)
    
    start_offset = int(sys.argv[1])
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    
    print(f"Starting migration from offset {start_offset} with batch size {batch_size}")
    
    current_offset = start_offset
    batch_count = 0
    
    while True:
        result = migrate_raw_batch_with_execution(current_offset, batch_size)
        
        if isinstance(result, tuple) and len(result) == 2:
            sql, processed = result
            
            if processed == 0:
                break
            
            batch_count += 1
            print(f"=== BATCH {batch_count} SQL ===")
            print(sql)
            print(f"=== END BATCH {batch_count} ===")
            print(f"Processed: {processed} records")
            print()
            
            current_offset += processed
            
            # Small delay between batches
            time.sleep(0.5)
        else:
            break
    
    print(f"Generated SQL for {batch_count} batches starting from offset {start_offset}")

if __name__ == "__main__":
    main()
