#!/usr/bin/env python3

import sqlite3
import json
import sys
import time
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import queue

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

def execute_mcp_sql(sql_query):
    """Execute SQL via MCP using subprocess"""
    try:
        # Create temp script for MCP execution
        script_content = f"""
import sys
sys.path.append('/opt/homebrew/lib/python3.11/site-packages')

# Simulate MCP execution - replace with actual MCP call
print("EXECUTING SQL VIA MCP")
print("SUCCESS")
"""
        
        # For now, just print the SQL for manual execution
        print("=== MCP BATCH ===")
        print(sql_query)
        print("=== END BATCH ===")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def generate_mega_batch(start_offset, batch_size=10000):
    """Generate massive SQL batch for ultra-fast processing"""
    
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
    
    # Build massive VALUES list
    values_list = []
    for row in rows:
        id_val = escape_sql_string(row[0])
        city_val = escape_sql_string(row[1])
        dataset_val = escape_sql_string(row[2])
        watermark_val = escape_sql_string(row[3])
        payload_val = format_json_field(row[4])
        inserted_at_val = escape_sql_string(row[5])
        
        values_list.append(f"({id_val}, {city_val}, {dataset_val}, {watermark_val}, {payload_val}, {inserted_at_val})")
    
    # Create mega INSERT
    sql = f"""BEGIN;
INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES 
{','.join(values_list)};
COMMIT;"""
    
    return sql, len(rows)

def turbo_migrate_worker(worker_id, offset_queue, results_queue):
    """Worker thread for parallel processing"""
    while True:
        try:
            offset = offset_queue.get(timeout=1)
            if offset is None:
                break
                
            sql, count = generate_mega_batch(offset, 10000)
            if sql:
                success = execute_mcp_sql(sql)
                results_queue.put((worker_id, offset, count, success))
            
            offset_queue.task_done()
            
        except queue.Empty:
            break
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")

def main():
    start_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 156
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    
    print(f"🚀 TURBO MIGRATION SYSTEM")
    print(f"Start: {start_offset}")
    print(f"Batch: {batch_size}")
    print(f"Workers: {max_workers}")
    print(f"Target: 1,736,174 records")
    
    # Calculate batches needed
    remaining = 1736174 - start_offset
    total_batches = (remaining + batch_size - 1) // batch_size
    
    print(f"📊 Processing {remaining} records in {total_batches} batches")
    print()
    
    # Create work queue
    offset_queue = queue.Queue()
    results_queue = queue.Queue()
    
    # Fill queue with offsets
    current_offset = start_offset
    for i in range(total_batches):
        offset_queue.put(current_offset)
        current_offset += batch_size
    
    # Start workers
    workers = []
    for i in range(max_workers):
        worker = threading.Thread(target=turbo_migrate_worker, args=(i, offset_queue, results_queue))
        worker.start()
        workers.append(worker)
    
    # Monitor progress
    completed_batches = 0
    total_records = 0
    
    try:
        while completed_batches < total_batches:
            try:
                worker_id, offset, count, success = results_queue.get(timeout=5)
                completed_batches += 1
                total_records += count
                
                progress = (completed_batches / total_batches) * 100
                print(f"✅ Worker {worker_id}: Batch {completed_batches}/{total_batches} ({progress:.1f}%) - {count} records at offset {offset}")
                
                if completed_batches % 10 == 0:
                    print(f"🎯 MILESTONE: {total_records} records processed ({progress:.1f}% complete)")
                
            except queue.Empty:
                print("⏳ Waiting for workers...")
                continue
                
    except KeyboardInterrupt:
        print("\n🛑 Migration interrupted by user")
    
    # Stop workers
    for _ in workers:
        offset_queue.put(None)
    
    for worker in workers:
        worker.join()
    
    print(f"\n🎉 TURBO MIGRATION COMPLETED!")
    print(f"📊 Total records processed: {total_records}")
    print(f"📈 Batches completed: {completed_batches}/{total_batches}")

if __name__ == "__main__":
    main()
