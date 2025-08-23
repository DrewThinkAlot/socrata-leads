#!/usr/bin/env python3
"""
Continuous migration script for raw table data from SQLite to Supabase.
Runs batches of 2000-5000 records until completion.
"""

import sqlite3
import json
import sys
import os
import time
from pathlib import Path

def escape_sql_string(value):
    """Escape string for SQL insertion"""
    if value is None:
        return 'NULL'
    return "'" + str(value).replace("'", "''") + "'"

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

def migrate_raw_batch(offset, batch_size):
    """Migrate a batch of raw table records"""
    db_path = Path(__file__).parent.parent / "data" / "pipeline.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, city, dataset, watermark, payload, inserted_at
            FROM raw
            LIMIT ? OFFSET ?
        """, (batch_size, offset))
        
        rows = cursor.fetchall()
        if not rows:
            return 0
        
        # Generate SQL statements
        sql_statements = []
        sql_statements.append("BEGIN;")
        
        for row in rows:
            id, city, dataset, watermark, payload, inserted_at = row
            
            sql = f"""
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES (
{escape_sql_string(id)}, 
{escape_sql_string(city)}, 
{escape_sql_string(dataset)}, 
{escape_sql_string(watermark)}, 
{format_json_field(payload)}, 
{escape_sql_string(str(inserted_at))}
);"""
            sql_statements.append(sql.strip())
        
        sql_statements.append("COMMIT;")
        
        # Print all SQL statements
        for statement in sql_statements:
            print(statement)
        
        return len(rows)
        
    except Exception as e:
        print(f"Error migrating batch: {e}", file=sys.stderr)
        return 0
    finally:
        conn.close()

def main():
    """Main continuous migration function"""
    print("Starting continuous raw table migration...")
    
    # Get current count from Supabase
    # For now, we'll start from the current offset
    offset = 68  # Current migrated count
    batch_size = 3000  # Increased batch size for faster processing
    
    total_records = 1736174  # Total records in raw table
    
    while offset < total_records:
        print(f"\n=== Processing batch: offset={offset}, size={batch_size} ===")
        
        migrated = migrate_raw_batch(offset, batch_size)
        if migrated == 0:
            print("No more records to migrate")
            break
            
        offset += migrated
        progress = (offset / total_records) * 100
        print(f"Progress: {offset}/{total_records} ({progress:.2f}%)")
        
        # Small delay to avoid overwhelming
        time.sleep(2)
    
    print(f"\nMigration completed! Total records migrated: {offset}")

if __name__ == "__main__":
    main()
