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

def get_batch_sql(start_offset, batch_size=1000):
    """Get SQL for a specific batch"""
    
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
    
    # Build VALUES list
    values_list = []
    for row in rows:
        id_val = escape_sql_string(row[0])
        city_val = escape_sql_string(row[1])
        dataset_val = escape_sql_string(row[2])
        watermark_val = escape_sql_string(row[3])
        payload_val = format_json_field(row[4])
        inserted_at_val = escape_sql_string(row[5])
        
        values_list.append(f"({id_val}, {city_val}, {dataset_val}, {watermark_val}, {payload_val}, {inserted_at_val})")
    
    # Create INSERT statement
    sql = f"INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES {','.join(values_list)};"
    
    return sql, len(rows)

if __name__ == "__main__":
    offset = int(sys.argv[1]) if len(sys.argv) > 1 else 159
    size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    
    sql, count = get_batch_sql(offset, size)
    if sql:
        print(sql)
    else:
        print("No records found")
