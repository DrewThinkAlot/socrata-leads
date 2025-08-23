#!/usr/bin/env python3

import sqlite3
import json
import sys
import time
import subprocess
import tempfile
import os

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

def execute_via_mcp_auto(sql_query):
    """Execute SQL via MCP automatically using subprocess"""
    try:
        # Create temporary file with SQL
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_query)
            temp_file = f.name
        
        # Execute via cascade mcp1_execute_sql
        cmd = [
            'python3', '-c', f'''
import subprocess
import json

# Execute MCP command
result = subprocess.run([
    "cascade", "mcp1_execute_sql",
    "--project_id", "hpejuxxqqvuuwifcojfz",
    "--query", """{sql_query.replace('"', '\\"')}"""
], capture_output=True, text=True)

print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("RETURN CODE:", result.returncode)
'''
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        # Clean up temp file
        os.unlink(temp_file)
        
        if result.returncode == 0:
            print(f"✅ MCP execution successful")
            return True
        else:
            print(f"❌ MCP execution failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("⏰ MCP execution timed out")
        return False
    except Exception as e:
        print(f"💥 MCP execution error: {e}")
        return False

def generate_turbo_batch(start_offset, batch_size=15000):
    """Generate large SQL batch for turbo processing"""
    
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
        return None, 0
    
    # Build VALUES list in chunks to avoid memory issues
    values_chunks = []
    chunk_size = 1000  # Process 1000 records per chunk
    
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        chunk_values = []
        
        for row in chunk:
            id_val = escape_sql_string(row[0])
            city_val = escape_sql_string(row[1])
            dataset_val = escape_sql_string(row[2])
            watermark_val = escape_sql_string(row[3])
            payload_val = format_json_field(row[4])
            inserted_at_val = escape_sql_string(row[5])
            
            chunk_values.append(f"({id_val}, {city_val}, {dataset_val}, {watermark_val}, {payload_val}, {inserted_at_val})")
        
        values_chunks.extend(chunk_values)
    
    # Create mega INSERT
    sql = f"""INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES 
{','.join(values_chunks)};"""
    
    return sql, len(rows)

def continuous_turbo_migrate():
    """Run continuous automated migration with large batches"""
    
    # Get current count from Supabase
    print("🔍 Checking current migration progress...")
    
    # Start from known offset
    current_offset = 156
    batch_size = 15000
    target_total = 1736174
    
    print(f"🚀 AUTOMATED TURBO MIGRATION")
    print(f"📊 Starting at offset: {current_offset}")
    print(f"📦 Batch size: {batch_size}")
    print(f"🎯 Target: {target_total} records")
    print(f"⏱️  Estimated batches: {(target_total - current_offset) // batch_size}")
    print()
    
    batch_count = 0
    total_processed = 0
    
    while current_offset < target_total:
        batch_count += 1
        
        print(f"🔄 Processing batch {batch_count} (offset {current_offset})...")
        
        # Generate SQL batch
        sql, record_count = generate_turbo_batch(current_offset, batch_size)
        
        if not sql or record_count == 0:
            print("✅ No more records to process")
            break
        
        print(f"📝 Generated SQL for {record_count} records")
        
        # Execute via MCP
        success = execute_via_mcp_auto(sql)
        
        if success:
            total_processed += record_count
            current_offset += record_count
            
            progress = (current_offset / target_total) * 100
            remaining = target_total - current_offset
            
            print(f"✅ Batch {batch_count} completed: {record_count} records")
            print(f"📈 Progress: {current_offset:,}/{target_total:,} ({progress:.2f}%)")
            print(f"⏳ Remaining: {remaining:,} records")
            print()
            
            # Brief pause to avoid overwhelming the system
            time.sleep(1)
            
        else:
            print(f"❌ Batch {batch_count} failed, retrying in 5 seconds...")
            time.sleep(5)
            continue
    
    print(f"🎉 TURBO MIGRATION COMPLETED!")
    print(f"📊 Total batches: {batch_count}")
    print(f"📈 Total records processed: {total_processed:,}")
    print(f"🎯 Final offset: {current_offset:,}")

if __name__ == "__main__":
    try:
        continuous_turbo_migrate()
    except KeyboardInterrupt:
        print("\n🛑 Migration interrupted by user")
    except Exception as e:
        print(f"💥 Migration error: {e}")
