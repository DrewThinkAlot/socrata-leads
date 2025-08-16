#!/usr/bin/env node

/**
 * Enhanced lead generation script for businesses opening in 30-60 days
 * Uses building permits and early indicators instead of existing businesses
 */

import Database from 'better-sqlite3';
import fs from 'fs';

const db = new Database('./data/pipeline.db');

// Calculate dates for 30-60 day prediction window
const now = new Date();
const thirtyDaysFromNow = new Date(now.getTime() + (30 * 24 * 60 * 60 * 1000));
const sixtyDaysFromNow = new Date(now.getTime() + (60 * 24 * 60 * 60 * 1000));
const sixMonthsAgo = new Date(now.getTime() - (180 * 24 * 60 * 60 * 1000));

console.log('🔍 Analyzing early indicators for businesses opening in 30-60 days...');
console.log(`Prediction window: ${thirtyDaysFromNow.toISOString().split('T')[0]} to ${sixtyDaysFromNow.toISOString().split('T')[0]}`);

// 1. Recent business license applications that predict future retail openings
const recentBusinessApplications = db.prepare(`
  SELECT 
    json_extract(payload, '$.doing_business_as_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.application_created_date') as application_date,
    json_extract(payload, '$.license_start_date') as license_start_date,
    json_extract(payload, '$.business_activity') as business_type,
    json_extract(payload, '$.license_status') as status,
    'Recent Business Application' as signal_type,
    85 as base_score
  FROM raw 
  WHERE city = 'chicago' 
    AND dataset = 'business_licenses'
    AND json_extract(payload, '$.application_created_date') >= ?
    AND json_extract(payload, '$.doing_business_as_name') IS NOT NULL
    AND json_extract(payload, '$.doing_business_as_name') != ''
    AND json_extract(payload, '$.license_status') IN ('AAC', 'AAI')
    AND (
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%RETAIL%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%STORE%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%SHOP%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%MARKET%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%BOUTIQUE%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%CLOTHING%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%MERCHANDISE%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%GOODS%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%SALES%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%VENDOR%'
    )
  ORDER BY json_extract(payload, '$.application_created_date') DESC
  LIMIT 100
`).all(sixMonthsAgo.toISOString());

// 2. Recent food license inspections for retail food businesses (grocery stores, markets)
const foodRetailInspections = db.prepare(`
  SELECT 
    json_extract(payload, '$.dba_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.inspection_date') as inspection_date,
    json_extract(payload, '$.facility_type') as business_type,
    json_extract(payload, '$.inspection_type') as inspection_type,
    'Food Retail Inspection' as signal_type,
    80 as base_score
  FROM raw 
  WHERE city = 'chicago' 
    AND dataset = 'food_inspections'
    AND json_extract(payload, '$.inspection_date') >= ?
    AND UPPER(json_extract(payload, '$.inspection_type')) LIKE '%LICENSE%'
    AND json_extract(payload, '$.dba_name') IS NOT NULL
    AND json_extract(payload, '$.dba_name') != ''
    AND (
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%GROCERY%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%MARKET%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%STORE%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%RETAIL%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%CONVENIENCE%'
    )
  ORDER BY json_extract(payload, '$.inspection_date') DESC
  LIMIT 30
`).all(sixMonthsAgo.toISOString());

// 3. Recent liquor license applications for retail liquor stores
const liquorRetailApplications = db.prepare(`
  SELECT 
    json_extract(payload, '$.doing_business_as_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.date_issued') as issue_date,
    json_extract(payload, '$.license_start_date') as start_date,
    'Liquor Retail Store' as business_type,
    'Liquor Retail Application' as signal_type,
    85 as base_score
  FROM raw 
  WHERE city = 'chicago' 
    AND dataset = 'liquor_licenses'
    AND json_extract(payload, '$.date_issued') >= ?
    AND json_extract(payload, '$.doing_business_as_name') IS NOT NULL
    AND json_extract(payload, '$.doing_business_as_name') != ''
    AND (
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%LIQUOR%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%WINE%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%SPIRITS%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%STORE%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%MARKET%'
    )
  ORDER BY json_extract(payload, '$.date_issued') DESC
  LIMIT 20
`).all(sixMonthsAgo.toISOString());

// Combine all leads
const allLeads = [
  ...recentBusinessApplications.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.application_date,
    predicted_open_date: calculateOpenDate(lead.application_date, 45), // 45 days from application
    signal_type: lead.signal_type,
    details: lead.business_type,
    contact: null,
    score: calculateScore(lead.base_score, lead.application_date)
  })),
  ...foodRetailInspections.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.inspection_date,
    predicted_open_date: calculateOpenDate(lead.inspection_date, 35), // 35 days from inspection
    signal_type: lead.signal_type,
    details: `${lead.business_type} - ${lead.inspection_type}`,
    contact: null,
    score: calculateScore(lead.base_score, lead.inspection_date)
  })),
  ...liquorRetailApplications.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.issue_date,
    predicted_open_date: calculateOpenDate(lead.issue_date, 40), // 40 days from license issue
    signal_type: lead.signal_type,
    details: lead.business_type,
    contact: null,
    score: calculateScore(lead.base_score, lead.issue_date)
  }))
];

// Helper functions
function calculateOpenDate(signalDate, daysToAdd) {
  const date = new Date(signalDate);
  date.setDate(date.getDate() + daysToAdd);
  return date.toISOString().split('T')[0];
}

function calculateScore(baseScore, signalDate) {
  const daysSinceSignal = (now - new Date(signalDate)) / (1000 * 60 * 60 * 24);
  // Higher score for more recent signals
  const recencyBonus = Math.max(0, 20 - (daysSinceSignal / 7));
  return Math.min(100, baseScore + recencyBonus + (Math.random() * 10 - 5));
}

// Filter for 30-60 day window and remove duplicates
const filteredLeads = allLeads
  .filter(lead => {
    const openDate = new Date(lead.predicted_open_date);
    return openDate >= thirtyDaysFromNow && openDate <= sixtyDaysFromNow;
  })
  .sort((a, b) => b.score - a.score)
  .slice(0, 50);

// Remove duplicates by address
const uniqueLeads = filteredLeads.filter((lead, index, arr) => 
  arr.findIndex(l => l.address === lead.address) === index
);

console.log(`Found ${uniqueLeads.length} potential leads opening in 30-60 days`);

// Create CSV output
const csvHeader = 'Business Name,Address,Predicted Open Date,Signal Type,Signal Date,Details,Contact,Score\n';
const csvRows = uniqueLeads.map(lead => 
  `"${lead.business_name}","${lead.address}","${lead.predicted_open_date}","${lead.signal_type}","${lead.signal_date}","${lead.details || ''}","${lead.contact || ''}",${lead.score.toFixed(1)}`
).join('\n');

const csvContent = csvHeader + csvRows;

// Write to file
fs.writeFileSync('./out/chicago-30-60-day-leads.csv', csvContent);

console.log('✅ 30-60 day leads exported to: out/chicago-30-60-day-leads.csv');
console.log(`📊 Total qualified leads: ${uniqueLeads.length}`);

// Show sample
console.log('\n🔍 Top 5 predicted openings:');
uniqueLeads.slice(0, 5).forEach((lead, i) => {
  console.log(`${i+1}. ${lead.business_name} at ${lead.address}`);
  console.log(`   Opens: ${lead.predicted_open_date} | Signal: ${lead.signal_type} | Score: ${lead.score.toFixed(1)}`);
});

// Show breakdown by signal type
const signalBreakdown = uniqueLeads.reduce((acc, lead) => {
  acc[lead.signal_type] = (acc[lead.signal_type] || 0) + 1;
  return acc;
}, {});

console.log('\n📈 Signal breakdown:');
Object.entries(signalBreakdown).forEach(([signal, count]) => {
  console.log(`   ${signal}: ${count} leads`);
});

db.close();
