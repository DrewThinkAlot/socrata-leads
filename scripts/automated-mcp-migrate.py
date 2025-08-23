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

def generate_large_batch(start_offset, batch_size=20000):
    """Generate large SQL batch for maximum speed"""
    
    conn = sqlite3.connect('data/pipeline.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, city, dataset, watermark, payload, inserted_at 
        FROM raw 
        ORDER BY id 
        LIMIT ? OFFSET ?
    """, (batch_size, start_offset))
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return None, 0
    
    # Build VALUES list efficiently
    values_list = []
    for row in rows:
        id_val = escape_sql_string(row[0])
        city_val = escape_sql_string(row[1])
        dataset_val = escape_sql_string(row[2])
        watermark_val = escape_sql_string(row[3])
        payload_val = format_json_field(row[4])
        inserted_at_val = escape_sql_string(row[5])
        
        values_list.append(f"({id_val}, {city_val}, {dataset_val}, {watermark_val}, {payload_val}, {inserted_at_val})")
    
    # Create optimized INSERT
    sql = f"""INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES {','.join(values_list)};"""
    
    return sql, len(rows)

def main():
    start_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 156
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
    max_batches = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    
    print(f"🚀 AUTOMATED HIGH-SPEED MIGRATION")
    print(f"📊 Start offset: {start_offset:,}")
    print(f"📦 Batch size: {batch_size:,}")
    print(f"🎯 Max batches: {max_batches}")
    print(f"🔥 Target speed: {batch_size * max_batches:,} records")
    print()
    
    current_offset = start_offset
    batch_count = 0
    total_processed = 0
    
    for i in range(max_batches):
        batch_count += 1
        
        print(f"⚡ Generating batch {batch_count} (offset {current_offset:,})...")
        
        sql, record_count = generate_large_batch(current_offset, batch_size)
        
        if not sql or record_count == 0:
            print("✅ No more records available")
            break
        
        print(f"📝 Generated SQL for {record_count:,} records")
        print("🔥 EXECUTE THIS VIA MCP:")
        print("=" * 80)
        print(sql)
        print("=" * 80)
        print()
        
        # Update counters
        total_processed += record_count
        current_offset += record_count
        
        # Progress update
        progress = (current_offset / 1736174) * 100
        remaining = 1736174 - current_offset
        
        print(f"📈 Batch {batch_count}: {record_count:,} records processed")
        print(f"🎯 Total progress: {current_offset:,}/1,736,174 ({progress:.2f}%)")
        print(f"⏳ Remaining: {remaining:,} records")
        print(f"⚡ Next offset: {current_offset:,}")
        print()
        
        # Brief pause between batches
        time.sleep(0.5)
    
    print(f"🎉 BATCH GENERATION COMPLETED!")
    print(f"📊 Generated {batch_count} batches")
    print(f"📈 Total records: {total_processed:,}")
    print(f"🚀 Next run: python3 scripts/automated-mcp-migrate.py {current_offset}")

if __name__ == "__main__":
    main()
