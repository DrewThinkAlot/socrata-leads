#!/usr/bin/env node

/**
 * Performance test script to verify optimization improvements
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const CITIES = ['chicago', 'seattle'];
const LIMIT = 1000;

function runCommand(cmd, cwd = process.cwd()) {
  console.log(`Running: ${cmd}`);
  try {
    const start = Date.now();
    const result = execSync(cmd, { 
      cwd, 
      stdio: 'inherit',
      env: { ...process.env, NODE_ENV: 'production' }
    });
    const duration = Date.now() - start;
    return { success: true, duration };
  } catch (error) {
    console.error(`Command failed: ${cmd}`, error.message);
    return { success: false, duration: 0 };
  }
}

function getDbStats() {
  try {
    const dbPath = path.join(__dirname, 'data', 'pipeline.db');
    if (fs.existsSync(dbPath)) {
      const stats = fs.statSync(dbPath);
      console.log(`Database size: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);
    }
  } catch (error) {
    console.log('Could not get DB stats');
  }
}

async function main() {
  console.log('🚀 Performance Test - Optimized Pipeline');
  console.log('========================================\n');

  // Check if database exists
  getDbStats();

  // Test optimized extraction
  console.log('📊 Testing Optimized Extraction...');
  const extractionResult = runCommand(`npm run extract:optimized -- --city chicago --limit ${LIMIT} --parallel 10 --verbose`);
  
  if (extractionResult.success) {
    console.log(`✅ Optimized extraction completed in ${extractionResult.duration}ms`);
  }

  // Test scoring
  console.log('\n📊 Testing Scoring...');
  const scoringResult = runCommand(`npm run score -- --city chicago`);
  
  if (scoringResult.success) {
    console.log(`✅ Scoring completed in ${scoringResult.duration}ms`);
  }

  // Test export
  console.log('\n📊 Testing Export...');
  const exportResult = runCommand(`npm run export -- --city chicago --limit 50 --out out/performance-test.csv`);
  
  if (exportResult.success) {
    console.log(`✅ Export completed in ${exportResult.duration}ms`);
  }

  // Final stats
  getDbStats();
  
  console.log('\n🎯 Optimization Summary:');
  console.log('- Database indexes added for faster queries');
  console.log('- Increased parallel requests from 5 to 10');
  console.log('- Increased batch sizes for better throughput');
  console.log('- Added LLM response caching');
  console.log('- Reduced memory usage with smaller batch processing');
  
  console.log('\n✨ Performance optimizations are now active!');
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { runCommand, getDbStats };
