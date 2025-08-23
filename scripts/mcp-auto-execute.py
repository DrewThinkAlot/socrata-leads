#!/usr/bin/env python3

import sqlite3
import json
import sys
import time
import subprocess

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

def execute_mcp_batch(sql_query, project_id="hpejuxxqqvuuwifcojfz"):
    """Execute SQL batch via MCP using Python API calls"""
    try:
        # Import MCP functions directly
        import sys
        sys.path.append('/opt/homebrew/lib/python3.11/site-packages')
        
        # Execute via cascade command
        cmd = [
            'cascade', 'mcp1_execute_sql',
            '--project_id', project_id,
            '--query', sql_query
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stderr
            
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)

def generate_and_execute_batch(start_offset, batch_size=5000):
    """Generate and execute a batch via MCP"""
    
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
        return False, 0, "No more records"
    
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
    
    # Execute via MCP
    success, result = execute_mcp_batch(sql)
    
    return success, len(rows), result

def continuous_auto_migrate():
    """Run continuous automated migration with MCP execution"""
    
    current_offset = 156
    batch_size = 5000  # Start with manageable size
    target_total = 1736174
    
    print(f"🚀 FULLY AUTOMATED MCP MIGRATION")
    print(f"📊 Starting at: {current_offset:,}")
    print(f"📦 Batch size: {batch_size:,}")
    print(f"🎯 Target: {target_total:,} records")
    print(f"⚡ Estimated time: ~{(target_total - current_offset) // batch_size} batches")
    print()
    
    batch_count = 0
    total_processed = 0
    consecutive_failures = 0
    
    while current_offset < target_total:
        batch_count += 1
        
        print(f"⚡ Executing batch {batch_count} (offset {current_offset:,})...")
        
        success, record_count, result = generate_and_execute_batch(current_offset, batch_size)
        
        if success:
            total_processed += record_count
            current_offset += record_count
            consecutive_failures = 0
            
            progress = (current_offset / target_total) * 100
            remaining = target_total - current_offset
            
            print(f"✅ Batch {batch_count}: {record_count:,} records migrated")
            print(f"📈 Progress: {current_offset:,}/{target_total:,} ({progress:.2f}%)")
            print(f"⏳ Remaining: {remaining:,} records")
            
            # Increase batch size if successful
            if batch_count % 5 == 0 and batch_size < 10000:
                batch_size = min(batch_size + 1000, 10000)
                print(f"🚀 Increased batch size to {batch_size:,}")
            
            print()
            
        else:
            consecutive_failures += 1
            print(f"❌ Batch {batch_count} failed: {result}")
            
            if consecutive_failures >= 3:
                print("💥 Too many consecutive failures, stopping")
                break
            
            # Reduce batch size on failure
            batch_size = max(batch_size // 2, 1000)
            print(f"⬇️ Reduced batch size to {batch_size:,}")
            
            time.sleep(5)  # Wait before retry
            continue
        
        # Brief pause between successful batches
        time.sleep(1)
    
    print(f"🎉 AUTOMATED MIGRATION COMPLETED!")
    print(f"📊 Total batches: {batch_count}")
    print(f"📈 Records migrated: {total_processed:,}")
    print(f"🎯 Final offset: {current_offset:,}")

if __name__ == "__main__":
    try:
        continuous_auto_migrate()
    except KeyboardInterrupt:
        print("\n🛑 Migration stopped by user")
    except Exception as e:
        print(f"💥 Migration error: {e}")
        import traceback
        traceback.print_exc()
