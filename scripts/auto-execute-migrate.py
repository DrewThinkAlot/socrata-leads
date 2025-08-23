#!/usr/bin/env python3
"""
Auto-executing migration that directly calls MCP server for each batch
"""

import sqlite3
import json
import sys
import os
import time
from pathlib import Path

class AutoExecuteMigrator:
    def __init__(self, table_name, batch_size=5000):
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
    
    def migrate_batch(self, offset):
        """Generate and print SQL for MCP execution"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT id, city, dataset, watermark, payload, inserted_at
                FROM {self.table_name}
                LIMIT ? OFFSET ?
            """, (self.batch_size, offset))
            rows = cursor.fetchall()
            
            if not rows:
                return 0
            
            print("BEGIN;")
            for row in rows:
                id, city, dataset, watermark, payload, inserted_at = row
                sql = f"""INSERT INTO {self.table_name} (id, city, dataset, watermark, payload, inserted_at) VALUES ({self.escape_sql_string(id)}, {self.escape_sql_string(city)}, {self.escape_sql_string(dataset)}, {self.escape_sql_string(watermark)}, {self.format_json_field(payload)}, {self.escape_sql_string(str(inserted_at))});"""
                print(sql)
            print("COMMIT;")
            
            conn.close()
            return len(rows)
            
        except Exception as e:
            print(f"Error: {e}")
            return 0

def main():
    table_name = sys.argv[1] if len(sys.argv) > 1 else 'raw'
    start_offset = int(sys.argv[2]) if len(sys.argv) > 2 else 68
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    
    migrator = AutoExecuteMigrator(table_name, batch_size)
    migrator.migrate_batch(start_offset)

if __name__ == "__main__":
    main()
