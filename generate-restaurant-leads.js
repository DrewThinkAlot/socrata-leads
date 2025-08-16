#!/usr/bin/env node

/**
 * Restaurant lead generation script for businesses opening in 30-60 days
 * Focuses on restaurants, bars, cafes, and food service establishments
 * Now uses the main pipeline with SpotOn intelligence
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import { leadsToCSV } from './src/util/csv.js';

const db = new Database('./data/pipeline.db');

// Calculate dates for 30-60 day prediction window
const now = new Date();
const thirtyDaysFromNow = new Date(now.getTime() + (30 * 24 * 60 * 60 * 1000));
const sixtyDaysFromNow = new Date(now.getTime() + (60 * 24 * 60 * 60 * 1000));
const sixMonthsAgo = new Date(now.getTime() - (180 * 24 * 60 * 60 * 1000));

console.log('🍽️  Loading restaurant leads from pipeline with SpotOn intelligence...');
console.log(`Prediction window: ${thirtyDaysFromNow.toISOString().split('T')[0]} to ${sixtyDaysFromNow.toISOString().split('T')[0]}`);

// Get leads from the main pipeline that have been scored and analyzed
const restaurantLeads = db.prepare(`
  SELECT * FROM leads 
  WHERE city = 'chicago' 
    AND created_at >= ?
    AND (
      -- Filter for restaurant-related businesses
      LOWER(name) LIKE '%restaurant%' OR
      LOWER(name) LIKE '%cafe%' OR
      LOWER(name) LIKE '%bar%' OR
      LOWER(name) LIKE '%grill%' OR
      LOWER(name) LIKE '%pizza%' OR
      LOWER(name) LIKE '%deli%' OR
      LOWER(name) LIKE '%bakery%' OR
      LOWER(name) LIKE '%bistro%' OR
      LOWER(name) LIKE '%tavern%' OR
      LOWER(name) LIKE '%kitchen%' OR
      LOWER(name) LIKE '%dining%' OR
      -- Also include leads with restaurant-type SpotOn service models
      (spoton_intelligence IS NOT NULL AND 
       json_extract(spoton_intelligence, '$.service_model') IN ('full-service', 'fast-casual'))
    )
    -- Filter for 30-60 day opening window based on days_remaining
    AND (
      (days_remaining IS NOT NULL AND days_remaining BETWEEN 30 AND 60) OR
      (days_remaining IS NULL AND score >= 70) -- High-scoring leads without days estimate
    )
    -- Exclude pop-up vendors
    AND (spoton_intelligence IS NULL OR 
         json_extract(spoton_intelligence, '$.is_pop_up_vendor') != 1)
  ORDER BY score DESC, 
           CASE WHEN spoton_intelligence IS NOT NULL 
                THEN json_extract(spoton_intelligence, '$.spoton_score') 
                ELSE 0 END DESC
  LIMIT 100
`).all(sixMonthsAgo.toISOString());

// Parse JSON fields and convert to Lead objects
const parsedLeads = restaurantLeads.map(lead => ({
  ...lead,
  spoton_intelligence: lead.spoton_intelligence ? JSON.parse(lead.spoton_intelligence) : null,
  evidence: lead.evidence ? JSON.parse(lead.evidence) : []
}));

console.log(`Found ${parsedLeads.length} restaurant leads from pipeline`);

// Filter for best quality leads (30-60 day window with good scores)
const qualifiedLeads = parsedLeads.filter(lead => {
  // Must have reasonable score
  if (lead.score < 50) return false;
  
  // Prefer leads with days_remaining in 30-60 range, but include high-scoring ones without estimate
  if (lead.days_remaining && (lead.days_remaining < 30 || lead.days_remaining > 60)) {
    return lead.score >= 80; // Only very high scoring leads outside window
  }
  
  return true;
}).slice(0, 50); // Limit to top 50

console.log(`Qualified ${qualifiedLeads.length} leads for 30-60 day restaurant openings`);

// Use the enhanced CSV export with SpotOn intelligence
async function exportLeads() {
  try {
    const csvContent = await leadsToCSV(qualifiedLeads);
    
    // Ensure output directory exists
    if (!fs.existsSync('./out')) {
      fs.mkdirSync('./out', { recursive: true });
    }
    
    // Write to file
    fs.writeFileSync('./out/chicago-restaurant-leads.csv', csvContent);
    
    console.log('✅ Restaurant leads exported to: out/chicago-restaurant-leads.csv');
    console.log(`📊 Total qualified leads: ${qualifiedLeads.length}`);
    
    // Show sample with SpotOn intelligence
    console.log('\n🔍 Top 5 restaurant openings:');
    qualifiedLeads.slice(0, 5).forEach((lead, i) => {
      const spoton = lead.spoton_intelligence;
      console.log(`${i+1}. ${lead.name} at ${lead.address}`);
      console.log(`   Score: ${lead.score} | Days Remaining: ${lead.days_remaining || 'N/A'} | Stage: ${lead.project_stage || 'Unknown'}`);
      if (spoton) {
        console.log(`   SpotOn Score: ${spoton.spoton_score} | Service: ${spoton.service_model} | Operator: ${spoton.operator_type}`);
        if (spoton.is_pop_up_vendor) console.log(`   ⚠️  Pop-up vendor detected`);
      }
    });
    
    // Show breakdown by SpotOn service model
    const serviceModelBreakdown = qualifiedLeads.reduce((acc, lead) => {
      const model = lead.spoton_intelligence?.service_model || 'unknown';
      acc[model] = (acc[model] || 0) + 1;
      return acc;
    }, {});
    
    console.log('\n📈 Service model breakdown:');
    Object.entries(serviceModelBreakdown).forEach(([model, count]) => {
      console.log(`   ${model}: ${count} leads`);
    });
    
    // Show SpotOn score distribution
    const highSpotOn = qualifiedLeads.filter(l => (l.spoton_intelligence?.spoton_score || 0) >= 70).length;
    const mediumSpotOn = qualifiedLeads.filter(l => {
      const score = l.spoton_intelligence?.spoton_score || 0;
      return score >= 40 && score < 70;
    }).length;
    const lowSpotOn = qualifiedLeads.length - highSpotOn - mediumSpotOn;
    
    console.log('\n🎯 SpotOn score distribution:');
    console.log(`   High (70+): ${highSpotOn} leads`);
    console.log(`   Medium (40-69): ${mediumSpotOn} leads`);
    console.log(`   Low (<40): ${lowSpotOn} leads`);
    
  } catch (error) {
    console.error('Error exporting leads:', error);
    process.exit(1);
  } finally {
    db.close();
  }
}

exportLeads();
