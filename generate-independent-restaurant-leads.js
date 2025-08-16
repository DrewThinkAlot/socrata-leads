#!/usr/bin/env node

/**
 * Independent restaurant lead generation script for businesses opening in 30-60 days
 * Filters out chains and franchises, focuses on local independent restaurants
 */

import Database from 'better-sqlite3';
import fs from 'fs';

const db = new Database('./data/pipeline.db');

// Calculate dates for 30-60 day prediction window
const now = new Date();
const thirtyDaysFromNow = new Date(now.getTime() + (30 * 24 * 60 * 60 * 1000));
const sixtyDaysFromNow = new Date(now.getTime() + (60 * 24 * 60 * 60 * 1000));
const sixMonthsAgo = new Date(now.getTime() - (180 * 24 * 60 * 60 * 1000));

console.log('🏪 Analyzing early indicators for INDEPENDENT restaurants opening in 30-60 days...');
console.log(`Prediction window: ${thirtyDaysFromNow.toISOString().split('T')[0]} to ${sixtyDaysFromNow.toISOString().split('T')[0]}`);

// Common chain/franchise keywords to exclude
const chainKeywords = [
  'MCDONALD', 'BURGER KING', 'SUBWAY', 'STARBUCKS', 'DUNKIN', 'KFC', 'TACO BELL',
  'PIZZA HUT', 'DOMINO', 'PAPA JOHN', 'LITTLE CAESAR', 'CHIPOTLE', 'PANERA',
  'WENDY', 'ARBY', 'DAIRY QUEEN', 'SONIC', 'CHICK-FIL-A', 'POPEYE', 'JIMMY JOHN',
  'QUIZNO', 'BLAZE PIZZA', 'FIVE GUYS', 'SHAKE SHACK', 'IN-N-OUT', 'WHATABURGER',
  'CARL JR', 'HARDEE', 'JACK IN THE BOX', 'WHITE CASTLE', 'CULVER', 'PORTILLO',
  'LOU MALNATI', 'GIORDANO', 'DEEP DISH', 'UNO', 'GINO', 'PEQUOD', 'HAROLD',
  'AL BEEF', 'ITALIAN BEEF', 'HOT DOG', 'VIENNA BEEF', 'CHICAGO DOG',
  'INC', 'LLC', 'CORP', 'CORPORATION', 'ENTERPRISES', 'GROUP', 'BRANDS',
  'FRANCHISE', 'CHAIN', 'NATIONAL', 'INTERNATIONAL', 'AMERICA', 'USA'
];

// Build exclusion filter for SQL
const chainExclusions = chainKeywords.map(keyword => 
  `UPPER(json_extract(payload, '$.doing_business_as_name')) NOT LIKE '%${keyword}%'`
).join(' AND ');

// 1. Recent business license applications for independent restaurants
const independentRestaurantApplications = db.prepare(`
  SELECT 
    json_extract(payload, '$.doing_business_as_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.application_created_date') as application_date,
    json_extract(payload, '$.license_start_date') as license_start_date,
    json_extract(payload, '$.business_activity') as business_type,
    json_extract(payload, '$.license_status') as status,
    'Independent Restaurant Application' as signal_type,
    95 as base_score
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
    AND ${chainExclusions}
    AND LENGTH(json_extract(payload, '$.doing_business_as_name')) <= 50
  ORDER BY json_extract(payload, '$.application_created_date') DESC
  LIMIT 100
`).all(sixMonthsAgo.toISOString());

// 2. Food license inspections for independent restaurants
const independentFoodInspections = db.prepare(`
  SELECT 
    json_extract(payload, '$.dba_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.inspection_date') as inspection_date,
    json_extract(payload, '$.facility_type') as business_type,
    json_extract(payload, '$.inspection_type') as inspection_type,
    json_extract(payload, '$.risk') as risk_level,
    'Independent Food License Inspection' as signal_type,
    90 as base_score
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
    AND ${chainKeywords.map(keyword => 
      `UPPER(json_extract(payload, '$.dba_name')) NOT LIKE '%${keyword}%'`
    ).join(' AND ')}
    AND LENGTH(json_extract(payload, '$.dba_name')) <= 50
  ORDER BY json_extract(payload, '$.inspection_date') DESC
  LIMIT 50
`).all(sixMonthsAgo.toISOString());

// 3. Independent liquor license applications
const independentLiquorApplications = db.prepare(`
  SELECT 
    json_extract(payload, '$.doing_business_as_name') as business_name,
    json_extract(payload, '$.address') as address,
    json_extract(payload, '$.date_issued') as issue_date,
    json_extract(payload, '$.license_start_date') as start_date,
    'Independent Restaurant/Bar Liquor License' as business_type,
    'Independent Liquor License Application' as signal_type,
    100 as base_score
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
    AND ${chainKeywords.map(keyword => 
      `UPPER(json_extract(payload, '$.doing_business_as_name')) NOT LIKE '%${keyword}%'`
    ).join(' AND ')}
    AND LENGTH(json_extract(payload, '$.doing_business_as_name')) <= 50
  ORDER BY json_extract(payload, '$.date_issued') DESC
  LIMIT 30
`).all(sixMonthsAgo.toISOString());

// Helper function to check if a business name looks independent
function isIndependentBusiness(name) {
  if (!name) return false;
  
  const upperName = name.toUpperCase();
  
  // Additional checks for independence indicators
  const independentIndicators = [
    name.includes("'S "), // Possessive names like "Mario's Pizza"
    name.includes(" & "), // Partnership names like "Smith & Jones"
    /^[A-Z][a-z]+ [A-Z][a-z]+/.test(name), // First Last name pattern
    name.length < 25, // Shorter names tend to be independent
    !upperName.includes('#'), // No location numbers
    !upperName.includes('STORE'), // Avoid "Store #123" patterns
    !upperName.includes('LOCATION')
  ];
  
  return independentIndicators.some(indicator => indicator);
}

// Combine all leads
const allLeads = [
  ...independentRestaurantApplications.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.application_date,
    predicted_open_date: calculateOpenDate(lead.application_date, 50), // 50 days for restaurant setup
    signal_type: lead.signal_type,
    details: lead.business_type,
    contact: null,
    score: calculateScore(lead.base_score, lead.application_date, lead.business_name)
  })),
  ...independentFoodInspections.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.inspection_date,
    predicted_open_date: calculateOpenDate(lead.inspection_date, 30), // 30 days from inspection
    signal_type: lead.signal_type,
    details: `${lead.business_type} - Risk: ${lead.risk_level}`,
    contact: null,
    score: calculateScore(lead.base_score, lead.inspection_date, lead.business_name)
  })),
  ...independentLiquorApplications.map(lead => ({
    business_name: lead.business_name,
    address: lead.address,
    signal_date: lead.issue_date,
    predicted_open_date: calculateOpenDate(lead.issue_date, 35), // 35 days from license issue
    signal_type: lead.signal_type,
    details: lead.business_type,
    contact: null,
    score: calculateScore(lead.base_score, lead.issue_date, lead.business_name)
  }))
];

// Helper functions
function calculateOpenDate(signalDate, daysToAdd) {
  const date = new Date(signalDate);
  date.setDate(date.getDate() + daysToAdd);
  return date.toISOString().split('T')[0];
}

function calculateScore(baseScore, signalDate, businessName) {
  const daysSinceSignal = (now - new Date(signalDate)) / (1000 * 60 * 60 * 24);
  // Higher score for more recent signals
  const recencyBonus = Math.max(0, 20 - (daysSinceSignal / 7));
  
  // Bonus for independent business indicators
  const independentBonus = isIndependentBusiness(businessName) ? 10 : 0;
  
  return Math.min(100, baseScore + recencyBonus + independentBonus + (Math.random() * 5 - 2.5));
}

// Filter for 30-60 day window and remove duplicates
const filteredLeads = allLeads
  .filter(lead => {
    const openDate = new Date(lead.predicted_open_date);
    return openDate >= thirtyDaysFromNow && openDate <= sixtyDaysFromNow;
  })
  .sort((a, b) => b.score - a.score)
  .slice(0, 50);

// Remove duplicates by address and further filter for independence
const uniqueLeads = filteredLeads
  .filter((lead, index, arr) => 
    arr.findIndex(l => l.address === lead.address) === index
  )
  .filter(lead => isIndependentBusiness(lead.business_name));

console.log(`Found ${uniqueLeads.length} independent restaurant leads opening in 30-60 days`);

// Create CSV output
const csvHeader = 'Business Name,Address,Predicted Open Date,Signal Type,Signal Date,Details,Contact,Score\n';
const csvRows = uniqueLeads.map(lead => 
  `"${lead.business_name}","${lead.address}","${lead.predicted_open_date}","${lead.signal_type}","${lead.signal_date}","${lead.details || ''}","${lead.contact || ''}",${lead.score.toFixed(1)}`
).join('\n');

const csvContent = csvHeader + csvRows;

// Write to file
fs.writeFileSync('./out/chicago-independent-restaurant-leads.csv', csvContent);

console.log('✅ Independent restaurant leads exported to: out/chicago-independent-restaurant-leads.csv');
console.log(`📊 Total independent restaurant leads: ${uniqueLeads.length}`);

// Show sample
console.log('\n🔍 Top 5 independent restaurant openings:');
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

console.log('\n🚫 Filtered out chains/franchises including:');
console.log('   McDonald\'s, Starbucks, Subway, Pizza Hut, Domino\'s, etc.');
console.log('   Corporate entities (Inc, LLC, Corp)');
console.log('   Multi-location indicators (#, Store, Location)');

db.close();
