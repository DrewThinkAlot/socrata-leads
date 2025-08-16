#!/usr/bin/env node

/**
 * Test script to verify new CSV export format includes project stage fields
 */

import { leadsToCSV } from './src/util/csv.js';

// Sample test lead with new project stage fields
const testLead = {
  name: "Test Restaurant",
  address: "123 Main St, Chicago, IL",
  score: 95.5,
  city: "Chicago",
  phone: "(312) 555-0123",
  email: "info@testrestaurant.com",
  created_at: new Date().toISOString(),
  evidence: [
    {
      predicted_open_week: "2025-10-15",
      evidence: [
        {
          type: "Business License",
          description: "New restaurant license application",
          source_link: "https://example.com/license",
          event_date: "2025-08-15"
        }
      ]
    }
  ],
  // New project stage classification fields
  project_stage: "Pre-Opening",
  days_remaining: 45,
  stage_confidence: 85,
  days_confidence: 78
};

async function testCSVExport() {
  console.log("Testing CSV export with new project stage fields...");
  
  try {
    const csv = await leadsToCSV([testLead]);
    console.log("✅ CSV export successful!");
    console.log("\nCSV Headers and sample data:");
    console.log("=".repeat(60));
    console.log(csv);
  } catch (error) {
    console.error("❌ CSV export failed:", error);
  }
}

testCSVExport();
