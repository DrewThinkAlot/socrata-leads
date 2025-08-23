#!/usr/bin/env npx tsx

import { createClient } from '@supabase/supabase-js';
import { StorageFactory } from '../src/storage/index.js';

const SUPABASE_URL = 'https://hpejuxxqqvuuwifcojfz.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhwZWp1eHhxcXZ1dXdpZmNvamZ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTUzOTk0NDksImV4cCI6MjA3MDk3NTQ0OX0.z7ZFKk7EYlbnHzZrU2VgmI3khRJA-asjAGjR32bYwXo';

async function migrateFromSupabase() {
  console.log('🔄 Starting migration from Supabase to SQLite...');
  
  // Initialize Supabase client
  const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
  
  // Initialize SQLite storage
  const storage = StorageFactory.create('sqlite://./data/pipeline.db');
  
  try {
    // Fetch all raw records from Supabase
    console.log('📥 Fetching raw records from Supabase...');
    const { data: rawRecords, error: rawError } = await supabase
      .from('raw')
      .select('*');
    
    if (rawError) {
      throw new Error(`Failed to fetch raw records: ${rawError.message}`);
    }
    
    console.log(`✅ Found ${rawRecords?.length || 0} raw records`);
    
    // Insert raw records into SQLite
    if (rawRecords && rawRecords.length > 0) {
      console.log('💾 Inserting raw records into SQLite...');
      for (const record of rawRecords) {
        await storage.insertRaw({
          id: record.id,
          city: record.city,
          dataset: record.dataset,
          watermark: record.watermark,
          payload: record.payload,
          insertedAt: new Date(record.inserted_at)
        });
      }
      console.log(`✅ Migrated ${rawRecords.length} raw records`);
    }
    
    // Fetch all normalized records from Supabase
    console.log('📥 Fetching normalized records from Supabase...');
    const { data: normalizedRecords, error: normalizedError } = await supabase
      .from('normalized')
      .select('*');
    
    if (normalizedError) {
      throw new Error(`Failed to fetch normalized records: ${normalizedError.message}`);
    }
    
    console.log(`✅ Found ${normalizedRecords?.length || 0} normalized records`);
    
    // Insert normalized records into SQLite
    if (normalizedRecords && normalizedRecords.length > 0) {
      console.log('💾 Inserting normalized records into SQLite...');
      for (const record of normalizedRecords) {
        await storage.insertNormalized({
          uid: record.uid,
          city: record.city,
          dataset: record.dataset,
          businessName: record.business_name,
          address: record.address,
          lat: record.lat,
          lon: record.lon,
          status: record.status,
          eventDate: record.event_date,
          type: record.type,
          description: record.description,
          sourceLink: record.source_link,
          rawId: record.raw_id,
          createdAt: new Date(record.created_at)
        });
      }
      console.log(`✅ Migrated ${normalizedRecords.length} normalized records`);
    }
    
    console.log('🎉 Migration completed successfully!');
    console.log(`📊 Total migrated: ${rawRecords?.length || 0} raw + ${normalizedRecords?.length || 0} normalized records`);
    
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await storage.close();
  }
}

migrateFromSupabase().catch(console.error);
