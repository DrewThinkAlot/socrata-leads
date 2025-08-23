#!/usr/bin/env python3

import csv
import json
import sys

def csv_to_bulk_insert(csv_file, table_name, batch_size=10000):
    """Convert CSV to bulk INSERT statements"""
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        batch = []
        batch_num = 0
        
        for row in reader:
            # Escape and format values
            if table_name == 'raw':
                values = f"('{row['id'].replace(chr(39), chr(39) + chr(39))}', '{row['city'].replace(chr(39), chr(39) + chr(39))}', '{row['dataset'].replace(chr(39), chr(39) + chr(39))}', '{row['watermark'].replace(chr(39), chr(39) + chr(39))}', '{row['payload'].replace(chr(39), chr(39) + chr(39))}'::jsonb, '{row['inserted_at'].replace(chr(39), chr(39) + chr(39))}')"
            
            batch.append(values)
            
            if len(batch) >= batch_size:
                batch_num += 1
                sql = f"BEGIN;\nINSERT INTO {table_name} (id, city, dataset, watermark, payload, inserted_at) VALUES\n" + ",\n".join(batch) + ";\nCOMMIT;"
                
                print(f"=== BATCH {batch_num} ===")
                print(sql)
                print(f"=== END BATCH {batch_num} ===")
                
                batch = []
        
        # Handle remaining records
        if batch:
            batch_num += 1
            sql = f"BEGIN;\nINSERT INTO {table_name} (id, city, dataset, watermark, payload, inserted_at) VALUES\n" + ",\n".join(batch) + ";\nCOMMIT;"
            
            print(f"=== BATCH {batch_num} ===")
            print(sql)
            print(f"=== END BATCH {batch_num} ===")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 csv-to-sql.py <csv_file> <table_name> [batch_size]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    table_name = sys.argv[2]
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 10000
    
    csv_to_bulk_insert(csv_file, table_name, batch_size)

if __name__ == "__main__":
    main()
