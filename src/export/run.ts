#!/usr/bin/env node

/**
 * Lead export pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { writeFileSync, mkdirSync } from 'fs';
import { dirname, resolve } from 'path';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { leadsToCSV } from '../util/csv.js';
import { analyzeDescription } from '../util/llm.js';
import type { ExportArgs } from '../types.js';

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs(): ExportArgs {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: {
        type: 'string',
        short: 'c',
      },
      limit: {
        type: 'string',
        short: 'l',
      },
      out: {
        type: 'string',
        short: 'o',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run export -- --city <city> --limit <number> --out <file>

Options:
  -c, --city <city>    City name (required)
  -l, --limit <number> Number of leads to export (required)
  -o, --out <file>     Output CSV file path (required)
  -h, --help          Show this help message

Examples:
  npm run export -- --city chicago --limit 12 --out out/chicago-drop.csv
  npm run export -- --city seattle --limit 12 --out out/seattle-drop.csv
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  if (!values.limit) {
    console.error('Error: --limit is required');
    process.exit(1);
  }

  if (!values.out) {
    console.error('Error: --out is required');
    process.exit(1);
  }

  const limit = parseInt(values.limit, 10);
  if (isNaN(limit) || limit <= 0) {
    console.error('Error: --limit must be a positive number');
    process.exit(1);
  }

  return {
    city: values.city,
    limit,
    out: values.out,
  };
}

/**
 * Main export function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting lead export', args);
    
    // Create storage instance
    const storage = await createStorage();
    
    // Get top leads for export
    const leads = await storage.queryForExport(args.city, args.limit);
    
    if (leads.length === 0) {
      logger.info('No leads found to export', args);
      await storage.close();
      process.exit(0);
    }
    
    logger.info(`Found ${leads.length} leads to export`);
    
    // Convert leads to CSV
    const csvContent = await leadsToCSV(leads, {
      headers: true,
      delimiter: ',',
      quote: '"',
    });
    
    // Ensure output directory exists
    const outputPath = resolve(process.cwd(), args.out);
    const outputDir = dirname(outputPath);
    mkdirSync(outputDir, { recursive: true });
    
    // Write CSV file
    writeFileSync(outputPath, csvContent, 'utf-8');
    
    // Log export summary
    const scoreStats = {
      min: Math.min(...leads.map(l => l.score)),
      max: Math.max(...leads.map(l => l.score)),
      avg: Math.round(leads.reduce((sum, l) => sum + l.score, 0) / leads.length),
    };
    
    const contactStats = {
      withPhone: leads.filter(l => l.phone).length,
      withEmail: leads.filter(l => l.email).length,
      withBoth: leads.filter(l => l.phone && l.email).length,
    };
    
    logger.info('Lead export completed successfully', {
      city: args.city,
      leadsExported: leads.length,
      outputFile: outputPath,
      scoreStats,
      contactStats,
    });
    
    // Display top leads summary
    console.log('\n📊 Export Summary:');
    console.log(`City: ${args.city}`);
    console.log(`Leads exported: ${leads.length}`);
    console.log(`Output file: ${outputPath}`);
    console.log(`Score range: ${scoreStats.min}-${scoreStats.max} (avg: ${scoreStats.avg})`);
    console.log(`Contact info: ${contactStats.withPhone} phone, ${contactStats.withEmail} email, ${contactStats.withBoth} both`);
    
    console.log('\n🏆 Top 5 Leads:');
    for (let i = 0; i < Math.min(5, leads.length); i++) {
      const lead = leads[i];
      if (!lead) continue;
      const name = lead.name || 'Unknown Business';
      const contact = [lead.phone, lead.email].filter(Boolean).join(', ') || 'No contact';
      console.log(`${i + 1}. ${name} (Score: ${lead.score}) - ${lead.address}`);
      console.log(`   Contact: ${contact}`);
      console.log(`   Evidence: ${lead.evidence.length} signals`);
      
      // Add LLM-enhanced business insights if available
      try {
        const primaryEvent = lead.evidence[0];
        const primaryEvidence = primaryEvent?.evidence?.[0];
        if (primaryEvidence && (primaryEvidence.description || primaryEvidence.business_name)) {
          const analysis = await analyzeDescription(
            primaryEvidence.description || '',
            primaryEvidence.business_name
          );
          console.log(`   Business Type: ${analysis.businessType}`);
          console.log(`   Confidence: ${analysis.confidence}%`);
        }
      } catch (error) {
        // Silently continue if LLM analysis fails
      }
    }
    
    // Close storage
    await storage.close();
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Lead export failed', { error, args });
    process.exit(1);
  }
}

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', { promise, reason });
  process.exit(1);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', { error: error.message, stack: error.stack });
  process.exit(1);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
