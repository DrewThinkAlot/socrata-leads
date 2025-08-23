#!/usr/bin/env node

/**
 * Batch migration script using direct SQLite queries and console output
 * This script extracts data from SQLite and outputs SQL INSERT statements
 */

import sqlite3 from 'sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_PATH = path.resolve(__dirname, '../data/pipeline.db');
const BATCH_SIZE = 100; // Small batches for MCP server

function escapeString(str) {
  if (str === null || str === undefined) return 'NULL';
  return `'${str.toString().replace(/'/g, "''")}'`;
}

function formatTimestamp(timestamp) {
  if (!timestamp) return 'NOW()';
  return escapeString(timestamp);
}

async function migrateBatch(tableName, offset, batchSize) {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY);
    
    let query;
    if (tableName === 'raw') {
      query = `SELECT id, city, dataset, watermark, payload, inserted_at FROM raw LIMIT ${batchSize} OFFSET ${offset}`;
    } else if (tableName === 'normalized') {
      query = `SELECT uid, city, dataset, business_name, address, lat, lon, status, event_date, type, description, source_link, raw_id, created_at FROM normalized LIMIT ${batchSize} OFFSET ${offset}`;
    } else if (tableName === 'events') {
      query = `SELECT event_id, city, address, name, predicted_open_week, signal_strength, evidence, created_at FROM events LIMIT ${batchSize} OFFSET ${offset}`;
    } else if (tableName === 'leads') {
      query = `SELECT lead_id, city, name, address, phone, email, score, evidence, created_at FROM leads LIMIT ${batchSize} OFFSET ${offset}`;
    }
    
    db.all(query, [], (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      
      if (rows.length === 0) {
        resolve([]);
        return;
      }
      
      // Generate INSERT statements
      const insertStatements = [];
      
      for (const row of rows) {
        let insertSQL;
        
        if (tableName === 'raw') {
          insertSQL = `INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at) VALUES (${escapeString(row.id)}, ${escapeString(row.city)}, ${escapeString(row.dataset)}, ${escapeString(row.watermark)}, '${JSON.stringify(JSON.parse(row.payload))}'::jsonb, ${formatTimestamp(row.inserted_at)});`;
        } else if (tableName === 'normalized') {
          insertSQL = `INSERT INTO normalized (uid, city, dataset, business_name, address, lat, lon, status, event_date, type, description, source_link, raw_id, created_at) VALUES (${escapeString(row.uid)}, ${escapeString(row.city)}, ${escapeString(row.dataset)}, ${escapeString(row.business_name)}, ${escapeString(row.address)}, ${row.lat || 'NULL'}, ${row.lon || 'NULL'}, ${escapeString(row.status)}, ${escapeString(row.event_date)}, ${escapeString(row.type)}, ${escapeString(row.description)}, ${escapeString(row.source_link)}, ${escapeString(row.raw_id)}, ${formatTimestamp(row.created_at)});`;
        } else if (tableName === 'events') {
          insertSQL = `INSERT INTO events (event_id, city, address, name, predicted_open_week, signal_strength, evidence, created_at) VALUES (${escapeString(row.event_id)}, ${escapeString(row.city)}, ${escapeString(row.address)}, ${escapeString(row.name)}, ${escapeString(row.predicted_open_week)}, ${row.signal_strength}, '${JSON.stringify(JSON.parse(row.evidence))}'::jsonb, ${formatTimestamp(row.created_at)});`;
        } else if (tableName === 'leads') {
          insertSQL = `INSERT INTO leads (lead_id, city, name, address, phone, email, score, evidence, created_at) VALUES (${escapeString(row.lead_id)}, ${escapeString(row.city)}, ${escapeString(row.name)}, ${escapeString(row.address)}, ${escapeString(row.phone)}, ${escapeString(row.email)}, ${row.score}, '${JSON.stringify(JSON.parse(row.evidence))}'::jsonb, ${formatTimestamp(row.created_at)});`;
        }
        
        insertStatements.push(insertSQL);
      }
      
      db.close();
      resolve(insertStatements);
    });
  });
}

async function main() {
  const tableName = process.argv[2] || 'raw';
  const startOffset = parseInt(process.argv[3] || '0');
  const maxBatches = parseInt(process.argv[4] || '10');
  
  console.log(`-- Migrating ${tableName} table starting from offset ${startOffset}`);
  console.log(`-- Batch size: ${BATCH_SIZE}, Max batches: ${maxBatches}`);
  console.log('');
  
  for (let batch = 0; batch < maxBatches; batch++) {
    const offset = startOffset + (batch * BATCH_SIZE);
    
    try {
      const insertStatements = await migrateBatch(tableName, offset, BATCH_SIZE);
      
      if (insertStatements.length === 0) {
        console.log(`-- No more records found at offset ${offset}`);
        break;
      }
      
      console.log(`-- Batch ${batch + 1}: ${insertStatements.length} records from offset ${offset}`);
      console.log('BEGIN;');
      
      for (const statement of insertStatements) {
        console.log(statement);
      }
      
      console.log('COMMIT;');
      console.log('');
      
    } catch (error) {
      console.error(`-- Error in batch ${batch + 1}:`, error.message);
      break;
    }
  }
  
  console.log(`-- Migration batch complete for ${tableName}`);
}

main().catch(console.error);
