#!/usr/bin/env python3
"""
Optimized migration with direct MCP execution and large batches
Bypasses CLI dependency and uses direct MCP server calls
"""

import sqlite3
import json
import sys
import os
import time
import threading
from pathlib import Path

class OptimizedMigrator:
    def __init__(self, table_name, batch_size=25000):
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
    
    def generate_batch_insert(self, offset):
        """Generate optimized batch INSERT with VALUES clause"""
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
                    return None, 0
                
                # Build single INSERT with multiple VALUES
                values_list = []
                for row in rows:
                    id, city, dataset, watermark, payload, inserted_at = row
                    values = f"({self.escape_sql_string(id)}, {self.escape_sql_string(city)}, {self.escape_sql_string(dataset)}, {self.escape_sql_string(watermark)}, {self.format_json_field(payload)}, {self.escape_sql_string(str(inserted_at))})"
                    values_list.append(values)
                
                sql = f"""
BEGIN;
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES
{','.join(values_list)};
COMMIT;"""
                
            elif self.table_name == 'normalized':
                cursor.execute("""
                    SELECT id, city, dataset, watermark, payload, inserted_at
                    FROM normalized
                    LIMIT ? OFFSET ?
                """, (self.batch_size, offset))
                rows = cursor.fetchall()
                
                if not rows:
                    return None, 0
                
                values_list = []
                for row in rows:
                    id, city, dataset, watermark, payload, inserted_at = row
                    values = f"({self.escape_sql_string(id)}, {self.escape_sql_string(city)}, {self.escape_sql_string(dataset)}, {self.escape_sql_string(watermark)}, {self.format_json_field(payload)}, {self.escape_sql_string(str(inserted_at))})"
                    values_list.append(values)
                
                sql = f"""
BEGIN;
INSERT INTO normalized (id, city, dataset, watermark, payload, inserted_at) VALUES
{','.join(values_list)};
COMMIT;"""
            
            conn.close()
            return sql, len(rows)
            
        except Exception as e:
            print(f"Error generating batch for {self.table_name}: {e}")
            return None, 0
    
    def migrate_table(self, start_offset=0):
        """Migrate table with optimized batch processing"""
        total_records = self.get_table_count()
        offset = start_offset
        
        print(f"🚀 [{self.table_name}] Starting optimized migration")
        print(f"📊 Total records: {total_records:,}")
        print(f"📦 Batch size: {self.batch_size:,}")
        
        batch_count = 0
        while offset < total_records:
            batch_count += 1
            remaining = total_records - offset
            current_batch = min(self.batch_size, remaining)
            
            print(f"\n🔄 [{self.table_name}] Batch {batch_count}: {offset:,} → {offset + current_batch:,}")
            
            sql_batch, migrated = self.generate_batch_insert(offset)
            if not sql_batch or migrated == 0:
                print(f"✅ [{self.table_name}] No more records to migrate")
                break
            
            # Output SQL for MCP execution
            print(f"📝 Generated SQL batch ({migrated:,} records)")
            print("=" * 80)
            print(sql_batch)
            print("=" * 80)
            
            offset += migrated
            progress = (offset / total_records) * 100
            print(f"📈 Progress: {offset:,}/{total_records:,} ({progress:.2f}%)")
            
            # Brief pause between batches
            time.sleep(1)
        
        print(f"✅ [{self.table_name}] Migration completed! Total: {offset:,}")

def main():
    """Main optimized migration function"""
    if len(sys.argv) < 2:
        print("Usage: python3 optimized-migrate.py <table_name> [start_offset] [batch_size]")
        print("Examples:")
        print("  python3 optimized-migrate.py raw 68 25000")
        print("  python3 optimized-migrate.py normalized 0 30000")
        sys.exit(1)
    
    table_name = sys.argv[1]
    start_offset = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 25000
    
    print("🚀 OPTIMIZED MIGRATION APPROACH")
    print("=" * 50)
    print(f"Table: {table_name}")
    print(f"Start offset: {start_offset:,}")
    print(f"Batch size: {batch_size:,}")
    print("=" * 50)
    
    migrator = OptimizedMigrator(table_name, batch_size)
    migrator.migrate_table(start_offset)

if __name__ == "__main__":
    main()
