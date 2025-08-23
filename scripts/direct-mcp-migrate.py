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

def generate_bulk_insert_sql(offset, batch_size=1000):
    """Generate optimized bulk INSERT SQL for MCP execution"""
    
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
        return None, 0
    
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
    
    # Create bulk INSERT
    sql = f"""BEGIN;
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES 
{','.join(values_list)};
COMMIT;"""
    
    return sql, len(rows)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 direct-mcp-migrate.py <start_offset> [batch_size] [max_batches]")
        sys.exit(1)
    
    start_offset = int(sys.argv[1])
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    max_batches = int(sys.argv[3]) if len(sys.argv) > 3 else 1000000
    
    print(f"🚀 Direct MCP Migration")
    print(f"Start offset: {start_offset}")
    print(f"Batch size: {batch_size}")
    
    current_offset = start_offset
    batch_count = 0
    
    while batch_count < max_batches:
        sql, record_count = generate_bulk_insert_sql(current_offset, batch_size)
        
        if sql is None or record_count == 0:
            print(f"✅ Migration completed - no more records")
            break
        
        batch_count += 1
        print(f"\n=== BATCH {batch_count} - EXECUTE VIA MCP ===")
        print(sql)
        print(f"=== END BATCH {batch_count} ===")
        
        current_offset += record_count
        print(f"📊 Batch {batch_count}: {record_count} records (offset {current_offset})")
        
        # Small delay between batches
        time.sleep(0.5)
    
    print(f"\n🎯 Generated {batch_count} batches, total offset: {current_offset}")

if __name__ == "__main__":
    main()
