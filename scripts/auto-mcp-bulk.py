#!/usr/bin/env python3

import subprocess
import time
import sys

def run_batch_and_execute(offset, batch_size=5000):
    """Generate SQL batch and execute via MCP"""
    
    # Generate SQL batch
    result = subprocess.run([
        'python3', 'scripts/migrate-data-mcp.py', 'raw', str(offset), str(batch_size)
    ], capture_output=True, text=True, cwd='/Users/admin/Documents/socrata-leads')
    
    if result.returncode != 0:
        print(f"❌ Failed to generate batch at offset {offset}")
        return False, 0
    
    # Extract SQL from output
    sql_output = result.stdout.strip()
    if not sql_output or 'INSERT INTO raw' not in sql_output:
        print(f"❌ No valid SQL generated for offset {offset}")
        return False, 0
    
    # Count records in this batch
    record_count = sql_output.count('INSERT INTO raw')
    
    print(f"📦 Generated batch: {offset} to {offset + record_count}")
    print(f"🔄 Executing {record_count} records via MCP...")
    
    # Execute via MCP (you'll need to manually execute the printed SQL)
    print("=== MCP EXECUTION NEEDED ===")
    print("BEGIN;")
    
    # Convert individual INSERTs to bulk INSERT
    lines = sql_output.split('\n')
    values_list = []
    
    for line in lines:
        if line.startswith('INSERT INTO raw') and 'VALUES' in line:
            # Extract values from INSERT statement
            start = line.find('VALUES (') + 8
            end = line.rfind(');')
            if start > 7 and end > start:
                values = line[start:end]
                values_list.append(f"({values})")
    
    if values_list:
        bulk_sql = f"INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES\n" + ",\n".join(values_list) + ";"
        print(bulk_sql)
    
    print("COMMIT;")
    print("=== END MCP EXECUTION ===")
    
    return True, record_count

def main():
    start_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    max_batches = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    print(f"🚀 Auto MCP Bulk Migration")
    print(f"Starting at offset: {start_offset}")
    print(f"Batch size: {batch_size}")
    print(f"Max batches: {max_batches}")
    
    current_offset = start_offset
    batch_count = 0
    
    while batch_count < max_batches:
        success, records = run_batch_and_execute(current_offset, batch_size)
        
        if not success or records == 0:
            print(f"✅ Migration completed or no more records")
            break
        
        current_offset += records
        batch_count += 1
        
        print(f"✅ Batch {batch_count} completed: {records} records")
        print(f"📊 Progress: {current_offset} total records processed")
        print()
        
        # Small delay between batches
        time.sleep(1)
    
    print(f"🎯 Completed {batch_count} batches, processed up to offset {current_offset}")

if __name__ == "__main__":
    main()
