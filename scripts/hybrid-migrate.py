#!/usr/bin/env python3
"""
Hybrid migration approach: Optimized MCP batch processing with parallel execution
Processes 50,000+ records per batch with direct MCP server execution
"""

import sqlite3
import json
import sys
import os
import time
import threading
from pathlib import Path
import subprocess

class HybridMigrator:
    def __init__(self, table_name, batch_size=50000):
        self.table_name = table_name
        self.batch_size = batch_size
        self.db_path = Path(__file__).parent.parent / "data" / "pipeline.db"
        self.project_id = "hpejuxxqqvuuwifcojfz"
        
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
    
    def execute_sql_via_mcp(self, sql_batch):
        """Execute SQL batch directly via MCP server"""
        try:
            # Write SQL to temporary file
            temp_file = f"/tmp/{self.table_name}_batch.sql"
            with open(temp_file, 'w') as f:
                f.write(sql_batch)
            
            # Execute via MCP server using subprocess
            cmd = [
                'python3', '-c', f'''
import sys
sys.path.append('/Users/admin/Documents/socrata-leads')
from scripts.execute_mcp import execute_sql
with open("{temp_file}", "r") as f:
    sql = f.read()
execute_sql("{self.project_id}", sql)
'''
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd='/Users/admin/Documents/socrata-leads')
            
            if result.returncode == 0:
                print(f"[{self.table_name}] Batch executed successfully")
                return True
            else:
                print(f"[{self.table_name}] Error executing batch: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"[{self.table_name}] Error in MCP execution: {e}")
            return False
    
    def generate_batch_sql(self, offset):
        """Generate SQL for a large batch of records"""
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
                
                sql_statements = ["BEGIN;"]
                for row in rows:
                    id, city, dataset, watermark, payload, inserted_at = row
                    sql = f"""INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES ({self.escape_sql_string(id)}, {self.escape_sql_string(city)}, {self.escape_sql_string(dataset)}, {self.escape_sql_string(watermark)}, {self.format_json_field(payload)}, {self.escape_sql_string(str(inserted_at))});"""
                    sql_statements.append(sql)
                
            elif self.table_name == 'normalized':
                cursor.execute("""
                    SELECT id, city, dataset, watermark, payload, inserted_at
                    FROM normalized
                    LIMIT ? OFFSET ?
                """, (self.batch_size, offset))
                rows = cursor.fetchall()
                
                if not rows:
                    return None, 0
                
                sql_statements = ["BEGIN;"]
                for row in rows:
                    id, city, dataset, watermark, payload, inserted_at = row
                    sql = f"""INSERT INTO normalized (id, city, dataset, watermark, payload, inserted_at) VALUES ({self.escape_sql_string(id)}, {self.escape_sql_string(city)}, {self.escape_sql_string(dataset)}, {self.escape_sql_string(watermark)}, {self.format_json_field(payload)}, {self.escape_sql_string(str(inserted_at))});"""
                    sql_statements.append(sql)
            
            sql_statements.append("COMMIT;")
            conn.close()
            
            return "\n".join(sql_statements), len(rows)
            
        except Exception as e:
            print(f"Error generating batch SQL for {self.table_name}: {e}")
            return None, 0
    
    def migrate_table(self, start_offset=0):
        """Migrate entire table with optimized batches"""
        total_records = self.get_table_count()
        offset = start_offset
        
        print(f"[{self.table_name}] Starting migration...")
        print(f"[{self.table_name}] Total records: {total_records:,}")
        print(f"[{self.table_name}] Batch size: {self.batch_size:,}")
        
        while offset < total_records:
            print(f"\n[{self.table_name}] Processing batch: {offset:,}-{min(offset + self.batch_size, total_records):,}")
            
            sql_batch, migrated = self.generate_batch_sql(offset)
            if not sql_batch or migrated == 0:
                print(f"[{self.table_name}] No more records to migrate")
                break
            
            # Execute batch via MCP
            success = self.execute_sql_via_mcp(sql_batch)
            if not success:
                print(f"[{self.table_name}] Failed to execute batch, stopping migration")
                break
                
            offset += migrated
            progress = (offset / total_records) * 100
            print(f"[{self.table_name}] Progress: {offset:,}/{total_records:,} ({progress:.2f}%)")
            
            # Small delay to prevent overwhelming
            time.sleep(0.5)
        
        print(f"[{self.table_name}] Migration completed! Total migrated: {offset:,}")

def main():
    """Main hybrid migration function"""
    print("🚀 Starting Hybrid Migration Approach")
    print("=" * 50)
    
    # Create migrators with optimized batch sizes
    raw_migrator = HybridMigrator('raw', batch_size=25000)  # 25k per batch
    normalized_migrator = HybridMigrator('normalized', batch_size=30000)  # 30k per batch
    
    # Get current counts
    raw_total = raw_migrator.get_table_count()
    normalized_total = normalized_migrator.get_table_count()
    
    print(f"📊 Migration Overview:")
    print(f"   Raw table: {raw_total:,} records")
    print(f"   Normalized table: {normalized_total:,} records")
    print(f"   Total records: {raw_total + normalized_total:,}")
    print(f"   Estimated time: 2-4 hours")
    
    # Start parallel migrations
    print("\n🔄 Starting parallel migrations...")
    raw_thread = threading.Thread(target=raw_migrator.migrate_table, args=(68,))  # Continue from 68
    normalized_thread = threading.Thread(target=normalized_migrator.migrate_table, args=(0,))
    
    raw_thread.start()
    normalized_thread.start()
    
    # Wait for completion
    raw_thread.join()
    normalized_thread.join()
    
    print("\n✅ Hybrid migration completed!")

if __name__ == "__main__":
    main()
