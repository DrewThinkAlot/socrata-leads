#!/usr/bin/env python3
"""
Direct MCP migration script that executes SQL via MCP server calls
Uses the working migrate-data-mcp.py approach with optimized batches
"""

import sqlite3
import json
import sys
import os
import time
import subprocess
from pathlib import Path

class MCPDirectMigrator:
    def __init__(self, table_name, batch_size=10000):
        self.table_name = table_name
        self.batch_size = batch_size
        self.db_path = Path(__file__).parent.parent / "data" / "pipeline.db"
        
    def escape_sql_string(self, value):
        if value is None:
            return 'NULL'
        return "'" + str(value).replace("'", "''") + "'"
    
    def format_json_field(self, json_str):
        if json_str is None:
            return 'NULL'
        try:
            parsed = json.loads(json_str)
            json_escaped = json.dumps(parsed).replace("'", "''")
            return f"'{json_escaped}'::jsonb"
        except:
            return "'{}'"
    
    def get_table_count(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            print(f"Error getting count: {e}")
            return 0
    
    def migrate_batch(self, offset):
        """Generate SQL for a batch and execute via subprocess call to migrate-data-mcp.py"""
        try:
            # Use the working migrate-data-mcp.py script
            cmd = [
                'python3', 
                'scripts/migrate-data-mcp.py', 
                self.table_name, 
                str(offset), 
                str(self.batch_size)
            ]
            
            print(f"🔄 Executing: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd, 
                cwd='/Users/admin/Documents/socrata-leads',
                capture_output=True, 
                text=True
            )
            
            if result.returncode == 0:
                print(f"✅ Batch completed successfully")
                return self.batch_size
            else:
                print(f"❌ Error: {result.stderr}")
                return 0
                
        except Exception as e:
            print(f"❌ Exception: {e}")
            return 0
    
    def migrate_continuous(self, start_offset=70):
        """Run continuous migration with progress tracking"""
        total_records = self.get_table_count()
        offset = start_offset
        
        print(f"🚀 Starting continuous migration for {self.table_name}")
        print(f"📊 Total records: {total_records:,}")
        print(f"📦 Batch size: {self.batch_size:,}")
        print(f"🎯 Starting from offset: {offset:,}")
        
        batch_count = 0
        while offset < total_records:
            batch_count += 1
            
            print(f"\n📋 Batch {batch_count}: {offset:,} → {min(offset + self.batch_size, total_records):,}")
            
            migrated = self.migrate_batch(offset)
            if migrated == 0:
                print("⚠️ No records migrated, stopping")
                break
            
            offset += migrated
            progress = (offset / total_records) * 100
            print(f"📈 Progress: {offset:,}/{total_records:,} ({progress:.2f}%)")
            
            # Small delay between batches
            time.sleep(2)
        
        print(f"🎉 Migration completed! Total migrated: {offset:,}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 mcp-direct-migrate.py <table_name> [start_offset] [batch_size]")
        print("Examples:")
        print("  python3 mcp-direct-migrate.py raw 70 10000")
        print("  python3 mcp-direct-migrate.py normalized 0 8000")
        sys.exit(1)
    
    table_name = sys.argv[1]
    start_offset = int(sys.argv[2]) if len(sys.argv) > 2 else 70
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 10000
    
    migrator = MCPDirectMigrator(table_name, batch_size)
    migrator.migrate_continuous(start_offset)

if __name__ == "__main__":
    main()
