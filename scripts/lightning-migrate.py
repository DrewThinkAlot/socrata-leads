#!/usr/bin/env python3

import sqlite3
import csv
import json
import sys
import tempfile
import os

def export_to_csv(table_name, output_file):
    """Export SQLite table to CSV format optimized for PostgreSQL COPY"""
    
    conn = sqlite3.connect('data/pipeline.db')
    cursor = conn.cursor()
    
    if table_name == 'raw':
        cursor.execute("SELECT id, city, dataset, watermark, payload, inserted_at FROM raw ORDER BY id")
    elif table_name == 'normalized':
        cursor.execute("SELECT id, raw_id, city, dataset, watermark, payload, inserted_at FROM normalized ORDER BY id")
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        
        # Write header
        if table_name == 'raw':
            writer.writerow(['id', 'city', 'dataset', 'watermark', 'payload', 'inserted_at'])
        elif table_name == 'normalized':
            writer.writerow(['id', 'raw_id', 'city', 'dataset', 'watermark', 'payload', 'inserted_at'])
        
        row_count = 0
        for row in cursor:
            # Convert JSON payload to string for CSV
            if table_name == 'raw':
                processed_row = [
                    row[0],  # id
                    row[1],  # city
                    row[2],  # dataset
                    row[3],  # watermark
                    row[4] if row[4] else '{}',  # payload (JSON as string)
                    row[5]   # inserted_at
                ]
            elif table_name == 'normalized':
                processed_row = [
                    row[0],  # id
                    row[1],  # raw_id
                    row[2],  # city
                    row[3],  # dataset
                    row[4],  # watermark
                    row[5] if row[5] else '{}',  # payload (JSON as string)
                    row[6]   # inserted_at
                ]
            
            writer.writerow(processed_row)
            row_count += 1
            
            if row_count % 100000 == 0:
                print(f"Exported {row_count} records...")
    
    conn.close()
    print(f"✅ Exported {row_count} records to {output_file}")
    return row_count

def generate_copy_sql(table_name, csv_file):
    """Generate PostgreSQL COPY command"""
    
    if table_name == 'raw':
        sql = f"""
-- Lightning-fast bulk import for raw table
COPY raw (id, city, dataset, watermark, payload, inserted_at) 
FROM STDIN 
WITH (
    FORMAT CSV, 
    HEADER true,
    DELIMITER ',',
    QUOTE '"',
    ESCAPE '"'
);
"""
    elif table_name == 'normalized':
        sql = f"""
-- Lightning-fast bulk import for normalized table  
COPY normalized (id, raw_id, city, dataset, watermark, payload, inserted_at)
FROM STDIN
WITH (
    FORMAT CSV,
    HEADER true, 
    DELIMITER ',',
    QUOTE '"',
    ESCAPE '"'
);
"""
    
    return sql

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 lightning-migrate.py <table_name> [output_dir]")
        print("Example: python3 lightning-migrate.py raw")
        sys.exit(1)
    
    table_name = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else '/tmp'
    
    if table_name not in ['raw', 'normalized']:
        print("Error: table_name must be 'raw' or 'normalized'")
        sys.exit(1)
    
    print(f"🚀 Lightning migration for {table_name} table")
    
    # Export to CSV
    csv_file = os.path.join(output_dir, f'{table_name}_export.csv')
    record_count = export_to_csv(table_name, csv_file)
    
    # Generate COPY SQL
    copy_sql = generate_copy_sql(table_name, csv_file)
    
    print(f"\n=== POSTGRESQL COPY COMMAND ===")
    print(copy_sql)
    print(f"=== CSV FILE: {csv_file} ===")
    print(f"Records to import: {record_count}")
    
    print(f"\n🎯 Next steps:")
    print(f"1. Upload {csv_file} to your server")
    print(f"2. Execute the COPY command via MCP or psql")
    print(f"3. Verify import: SELECT COUNT(*) FROM {table_name}")

if __name__ == "__main__":
    main()
