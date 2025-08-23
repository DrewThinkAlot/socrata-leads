#!/usr/bin/env python3

import sqlite3
import json
import sys

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

def generate_mega_batch_sql(start_offset, batch_size=5000):
    """Generate large SQL batch for mega processing"""
    
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
    
    # Create mega INSERT
    sql = f"INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES {','.join(values_list)};"
    
    return sql, len(rows)

def main():
    start_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 165
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    
    print(f"🚀 MEGA BATCH GENERATOR")
    print(f"📊 Offset: {start_offset:,}")
    print(f"📦 Size: {batch_size:,}")
    print()
    
    sql, count = generate_mega_batch_sql(start_offset, batch_size)
    
    if sql and count > 0:
        print(f"✅ Generated SQL for {count:,} records")
        print("🔥 EXECUTE THIS VIA MCP:")
        print("=" * 80)
        print(sql)
        print("=" * 80)
        print()
        
        next_offset = start_offset + count
        progress = (next_offset / 1736174) * 100
        remaining = 1736174 - next_offset
        
        print(f"📈 Next offset: {next_offset:,}")
        print(f"📊 Progress: {progress:.2f}%")
        print(f"⏳ Remaining: {remaining:,} records")
        print(f"🚀 Next command: python3 scripts/mega-batch-execute.py {next_offset}")
    else:
        print("✅ No more records to process")

if __name__ == "__main__":
    main()
