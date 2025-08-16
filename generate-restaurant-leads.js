#!/usr/bin/env node

/**
 * Restaurant lead generation script for businesses opening in 30-60 days
 * Focuses on restaurants, bars, cafes, and food service establishments
 */

import Database from 'better-sqlite3';
import fs from 'fs';

const db = new Database('./data/pipeline.db');

// Calculate dates for 30-60 day prediction window
const now = new Date();
const thirtyDaysFromNow = new Date(now.getTime() + (30 * 24 * 60 * 60 * 1000));
const sixtyDaysFromNow = new Date(now.getTime() + (60 * 24 * 60 * 60 * 1000));
const sixMonthsAgo = new Date(now.getTime() - (180 * 24 * 60 * 60 * 1000));

console.log('🍽️  Analyzing early indicators for restaurants opening in 30-60 days...');
console.log(`Prediction window: ${thirtyDaysFromNow.toISOString().split('T')[0]} to ${sixtyDaysFromNow.toISOString().split('T')[0]}`);

// 1. Recent business license applications for restaurants and food service
const restaurantBusinessApplications = db.prepare(`
  SELECT 
    json_extract(payload, '$.doing_business_as_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.application_created_date') as application_date,
    json_extract(payload, '$.license_start_date') as license_start_date,
    json_extract(payload, '$.business_activity') as business_type,
    json_extract(payload, '$.license_status') as status,
    'Restaurant Business Application' as signal_type,
    90 as base_score
  FROM raw 
  WHERE city = 'chicago' 
    AND dataset = 'business_licenses'
    AND json_extract(payload, '$.application_created_date') >= ?
    AND json_extract(payload, '$.doing_business_as_name') IS NOT NULL
    AND json_extract(payload, '$.doing_business_as_name') != ''
    AND json_extract(payload, '$.license_status') IN ('AAC', 'AAI')
    AND (
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%RESTAURANT%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%FOOD%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%CAFE%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%BAR%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%DELI%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%BAKERY%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%PIZZA%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%GRILL%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%DINING%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%CATERING%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%TAVERN%' OR
      UPPER(json_extract(payload, '$.business_activity')) LIKE '%BISTRO%'
    )
  ORDER BY json_extract(payload, '$.application_created_date') DESC
  LIMIT 100
`).all(sixMonthsAgo.toISOString());

// 2. Food license inspections for new restaurants
const foodLicenseInspections = db.prepare(`
  SELECT 
    json_extract(payload, '$.dba_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.inspection_date') as inspection_date,
    json_extract(payload, '$.facility_type') as business_type,
    json_extract(payload, '$.inspection_type') as inspection_type,
    json_extract(payload, '$.risk') as risk_level,
    'Food License Inspection' as signal_type,
    85 as base_score
  FROM raw 
  WHERE city = 'chicago' 
    AND dataset = 'food_inspections'
    AND json_extract(payload, '$.inspection_date') >= ?
    AND UPPER(json_extract(payload, '$.inspection_type')) LIKE '%LICENSE%'
    AND json_extract(payload, '$.dba_name') IS NOT NULL
    AND json_extract(payload, '$.dba_name') != ''
    AND (
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%RESTAURANT%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%FOOD%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%CAFE%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%BAR%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%DELI%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%BAKERY%' OR
      UPPER(json_extract(payload, '$.facility_type')) LIKE '%PIZZA%' OR
      UPPER(json_extract(payload, '$.facility_type')) NOT LIKE '%GROCERY%' AND
      UPPER(json_extract(payload, '$.facility_type')) NOT LIKE '%MARKET%'
    )
  ORDER BY json_extract(payload, '$.inspection_date') DESC
  LIMIT 50
`).all(sixMonthsAgo.toISOString());

// 3. Liquor license applications for bars and restaurants
const liquorLicenseApplications = db.prepare(`
  SELECT 
    json_extract(payload, '$.doing_business_as_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.date_issued') as issue_date,
    json_extract(payload, '$.license_start_date') as start_date,
    'Restaurant/Bar Liquor License' as business_type,
    'Liquor License Application' as signal_type,
    95 as base_score
  FROM raw 
  WHERE city = 'chicago' 
    AND dataset = 'liquor_licenses'
    AND json_extract(payload, '$.date_issued') >= ?
    AND json_extract(payload, '$.doing_business_as_name') IS NOT NULL
    AND json_extract(payload, '$.doing_business_as_name') != ''
    AND (
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%RESTAURANT%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%BAR%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%GRILL%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%TAVERN%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%CAFE%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%BISTRO%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%PIZZA%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%DINER%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%EATERY%' OR
      UPPER(json_extract(payload, '$.doing_business_as_name')) LIKE '%KITCHEN%'
    )
  ORDER BY json_extract(payload, '$.date_issued') DESC
  LIMIT 30
`).all(sixMonthsAgo.toISOString());

// Combine all leads
const allLeads = [
  ...restaurantBusinessApplications.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.application_date,
    predicted_open_date: calculateOpenDate(lead.application_date, 50), // 50 days for restaurant setup
    signal_type: lead.signal_type,
    details: lead.business_type,
    contact: null,
    score: calculateScore(lead.base_score, lead.application_date)
  })),
  ...foodLicenseInspections.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.inspection_date,
    predicted_open_date: calculateOpenDate(lead.inspection_date, 30), // 30 days from inspection
    signal_type: lead.signal_type,
    details: `${lead.business_type} - Risk: ${lead.risk_level}`,
    contact: null,
    score: calculateScore(lead.base_score, lead.inspection_date)
  })),
  ...liquorLicenseApplications.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.issue_date,
    predicted_open_date: calculateOpenDate(lead.issue_date, 35), // 35 days from license issue
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

console.log(`Found ${uniqueLeads.length} restaurant leads opening in 30-60 days`);

// Create CSV output
const csvHeader = 'Business Name,Address,Predicted Open Date,Signal Type,Signal Date,Details,Contact,Score\n';
const csvRows = uniqueLeads.map(lead => 
  `"${lead.business_name}","${lead.address}","${lead.predicted_open_date}","${lead.signal_type}","${lead.signal_date}","${lead.details || ''}","${lead.contact || ''}",${lead.score.toFixed(1)}`
).join('\n');

const csvContent = csvHeader + csvRows;

// Write to file
fs.writeFileSync('./out/chicago-restaurant-leads.csv', csvContent);

console.log('✅ Restaurant leads exported to: out/chicago-restaurant-leads.csv');
console.log(`📊 Total restaurant leads: ${uniqueLeads.length}`);

// Show sample
console.log('\n🔍 Top 5 restaurant openings:');
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
