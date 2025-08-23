#!/usr/bin/env python3

"""
Data migration script using SQLite and MCP server approach
Extracts data from SQLite and generates SQL statements for manual execution
"""

import sqlite3
import json
import sys
import os
from pathlib import Path

def escape_sql_string(value):
    """Escape SQL string values"""
    if value is None:
        return 'NULL'
    return f"'{str(value).replace(chr(39), chr(39) + chr(39))}'"

def format_json_field(json_str):
    """Format JSON field for PostgreSQL"""
    if json_str is None:
        return 'NULL'
    try:
        # Parse and re-stringify to ensure valid JSON
        parsed = json.loads(json_str)
        # Escape single quotes in the JSON string for PostgreSQL
        json_escaped = json.dumps(parsed).replace("'", "''")
        return f"'{json_escaped}'::jsonb"
    except:
        return "'{}'"

def migrate_raw_batch(db_path, offset, batch_size):
    """Extract raw table data and generate INSERT statements"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT id, city, dataset, watermark, payload, inserted_at 
    FROM raw 
    LIMIT ? OFFSET ?
    """
    
    cursor.execute(query, (batch_size, offset))
    rows = cursor.fetchall()
    
    if not rows:
        conn.close()
        return []
    
    statements = []
    for row in rows:
        id_val, city, dataset, watermark, payload, inserted_at = row
        
        sql = f"""INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES (
{escape_sql_string(id_val)}, 
{escape_sql_string(city)}, 
{escape_sql_string(dataset)}, 
{escape_sql_string(watermark)}, 
{format_json_field(payload)}, 
{escape_sql_string(inserted_at)}
);"""
        statements.append(sql)
    
    conn.close()
    return statements

def migrate_normalized_batch(db_path, offset, batch_size):
    """Extract normalized table data and generate INSERT statements"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT uid, city, dataset, business_name, address, lat, lon, status, 
           event_date, type, description, source_link, raw_id, created_at 
    FROM normalized 
    LIMIT ? OFFSET ?
    """
    
    cursor.execute(query, (batch_size, offset))
    rows = cursor.fetchall()
    
    if not rows:
        conn.close()
        return []
    
    statements = []
    for row in rows:
        uid, city, dataset, business_name, address, lat, lon, status, event_date, type_val, description, source_link, raw_id, created_at = row
        
        lat_val = 'NULL' if lat is None else str(lat)
        lon_val = 'NULL' if lon is None else str(lon)
        
        sql = f"""INSERT INTO normalized (uid, city, dataset, business_name, address, lat, lon, status, event_date, type, description, source_link, raw_id, created_at) VALUES (
{escape_sql_string(uid)}, 
{escape_sql_string(city)}, 
{escape_sql_string(dataset)}, 
{escape_sql_string(business_name)}, 
{escape_sql_string(address)}, 
{lat_val}, 
{lon_val}, 
{escape_sql_string(status)}, 
{escape_sql_string(event_date)}, 
{escape_sql_string(type_val)}, 
{escape_sql_string(description)}, 
{escape_sql_string(source_link)}, 
{escape_sql_string(raw_id)}, 
{escape_sql_string(created_at)}
);"""
        statements.append(sql)
    
    conn.close()
    return statements

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 migrate-data-mcp.py <table> <offset> <batch_size>")
        print("Tables: raw, normalized")
        sys.exit(1)
    
    table = sys.argv[1]
    offset = int(sys.argv[2])
    batch_size = int(sys.argv[3])
    
    db_path = Path(__file__).parent.parent / "data" / "pipeline.db"
    
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)
    
    print(f"-- Migrating {table} table: offset {offset}, batch size {batch_size}")
    
    if table == 'raw':
        statements = migrate_raw_batch(str(db_path), offset, batch_size)
    elif table == 'normalized':
        statements = migrate_normalized_batch(str(db_path), offset, batch_size)
    else:
        print(f"Unsupported table: {table}")
        sys.exit(1)
    
    if not statements:
        print("-- No records found")
        return
    
    print(f"-- Found {len(statements)} records")
    print("BEGIN;")
    for stmt in statements:
        print(stmt)
    print("COMMIT;")

if __name__ == "__main__":
    main()
