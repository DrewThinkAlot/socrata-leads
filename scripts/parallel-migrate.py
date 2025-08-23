#!/usr/bin/env python3
"""
Parallel migration script for both raw and normalized tables from SQLite to Supabase.
Runs both migrations concurrently with optimized batch sizes.
"""

import sqlite3
import json
import sys
import os
import time
import threading
from pathlib import Path

class SupabaseMigrator:
    def __init__(self, table_name, batch_size=5000):
        self.table_name = table_name
        self.batch_size = batch_size
        self.db_path = Path(__file__).parent.parent / "data" / "pipeline.db"
        
    def escape_sql_string(self, value):
        """Escape string for SQL insertion"""
        if value is None:
            return 'NULL'
        return "'" + str(value).replace("'", "''") + "'"
    
    def format_json_field(self, json_str):
        """Format JSON field for PostgreSQL JSONB insertion"""
        if json_str is None:
            return 'NULL'
        try:
            parsed = json.loads(json_str)
            json_escaped = json.dumps(parsed).replace("'", "''")
            return f"'{json_escaped}'::jsonb"
        except:
            return "'{}'"
    
    def get_table_count(self):
        """Get total record count for the table"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            print(f"Error getting count for {self.table_name}: {e}")
            return 0
    
    def migrate_batch(self, offset):
        """Migrate a single batch of records"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if self.table_name == 'raw':
                cursor.execute("""
                    SELECT id, city, dataset, watermark, payload, inserted_at
                    FROM raw
                    LIMIT ? OFFSET ?
                """, (self.batch_size, offset))
                rows = cursor.fetchall()
                
                if not rows:
                    return 0
                
                sql_statements = ["BEGIN;"]
                for row in rows:
                    id, city, dataset, watermark, payload, inserted_at = row
                    sql = f"""
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES (
{self.escape_sql_string(id)}, 
{self.escape_sql_string(city)}, 
{self.escape_sql_string(dataset)}, 
{self.escape_sql_string(watermark)}, 
{self.format_json_field(payload)}, 
{self.escape_sql_string(str(inserted_at))}
);"""
                    sql_statements.append(sql.strip())
                
            elif self.table_name == 'normalized':
                cursor.execute("""
                    SELECT id, city, dataset, watermark, payload, inserted_at
                    FROM normalized
                    LIMIT ? OFFSET ?
                """, (self.batch_size, offset))
                rows = cursor.fetchall()
                
                if not rows:
                    return 0
                
                sql_statements = ["BEGIN;"]
                for row in rows:
                    id, city, dataset, watermark, payload, inserted_at = row
                    sql = f"""
INSERT INTO normalized (id, city, dataset, watermark, payload, inserted_at) VALUES (
{self.escape_sql_string(id)}, 
{self.escape_sql_string(city)}, 
{self.escape_sql_string(dataset)}, 
{self.escape_sql_string(watermark)}, 
{self.format_json_field(payload)}, 
{self.escape_sql_string(str(inserted_at))}
);"""
                    sql_statements.append(sql.strip())
            
            sql_statements.append("COMMIT;")
            
            # Print SQL statements for MCP server execution
            for statement in sql_statements:
                print(statement)
            
            conn.close()
            return len(rows)
            
        except Exception as e:
            print(f"Error migrating {self.table_name} batch: {e}")
            return 0

def migrate_table(migrator, start_offset=0):
    """Migrate a single table with progress tracking"""
    table_name = migrator.table_name
    total_records = migrator.get_table_count()
    offset = start_offset
    
    print(f"Starting {table_name} migration...")
    print(f"Total {table_name} records: {total_records}")
    
    while offset < total_records:
        print(f"\n[{table_name}] Processing batch: offset={offset}, size={migrator.batch_size}")
        
        migrated = migrator.migrate_batch(offset)
        if migrated == 0:
            print(f"[{table_name}] No more records to migrate")
            break
            
        offset += migrated
        progress = (offset / total_records) * 100
        print(f"[{table_name}] Progress: {offset}/{total_records} ({progress:.2f}%)")
        
        # Small delay between batches
        time.sleep(1)
    
    print(f"[{table_name}] Migration completed! Total records migrated: {offset}")

def main():
    """Main parallel migration function"""
    print("Starting parallel migration of raw and normalized tables...")
    
    # Create migrators for both tables
    raw_migrator = SupabaseMigrator('raw', batch_size=8000)
    normalized_migrator = SupabaseMigrator('normalized', batch_size=6000)
    
    # Get current counts
    raw_total = raw_migrator.get_table_count()
    normalized_total = normalized_migrator.get_table_count()
    
    print(f"Raw table: {raw_total} records")
    print(f"Normalized table: {normalized_total} records")
    print(f"Total records to migrate: {raw_total + normalized_total:,}")
    
    # Start parallel migrations
    raw_thread = threading.Thread(target=migrate_table, args=(raw_migrator, 68))
    normalized_thread = threading.Thread(target=migrate_table, args=(normalized_migrator, 0))
    
    print("\nStarting parallel migrations...")
    raw_thread.start()
    normalized_thread.start()
    
    # Wait for both to complete
    raw_thread.join()
    normalized_thread.join()
    
    print("\nParallel migration completed!")

if __name__ == "__main__":
    main()
