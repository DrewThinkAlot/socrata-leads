#!/usr/bin/env node

/**
 * Enhanced LLM Analysis for Phase 1 Leads
 * Applies GPT-5-mini analysis to our 12 enhanced predictions
 */

import { config } from 'dotenv';
import { createStorage } from './storage/index.js';
import { logger } from './util/logger.js';
import { analyzeDescription, categorizeBusinessType, initializeLLM } from './util/llm.js';
import { parseDate, getAgeInDays } from './util/dates.js';
import { parseArgs } from 'util';
import type { Lead } from './types.js';

// Load environment variables
config();

/**
 * Enhanced LLM Analysis Results
 */
interface EnhancedAnalysis {
  lead: Lead;
  businessAnalysis: {
    category: string;
    confidence: number;
    keyFeatures: string[];
    source: 'llm' | 'fallback';
  };
  marketAnalysis: {
    locationScore: number;
    competitiveAdvantage: string[];
    marketOpportunity: string;
    riskFactors: string[];
  };
  timingAnalysis: {
    openingUrgency: 'HIGH' | 'MEDIUM' | 'LOW';
    keyMilestones: string[];
    predictedTimeframe: string;
    confidence: number;
  };
  salesRecommendations: {
    approachStrategy: string;
    keyValueProps: string[];
    decisionMakers: string[];
    bestContactTiming: string;
  };
}

/**
 * Parse command line arguments
 */
function parseCliArgs() {
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
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run analyze-leads -- --city <city> [--limit <number>]

Options:
  -c, --city <city>       City name (required)
  -l, --limit <number>    Number of leads to analyze (default: 12)
  -h, --help              Show this help message

Examples:
  npm run analyze-leads -- --city chicago
  npm run analyze-leads -- --city chicago --limit 5
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  return {
    city: values.city as string,
    limit: parseInt(values.limit as string) || 12,
  };
}

/**
 * Analyze business potential with enhanced GPT prompting
 */
async function analyzeBusinessPotential(lead: Lead): Promise<EnhancedAnalysis['businessAnalysis']> {
  // Combine all evidence descriptions
  const allDescriptions = lead.evidence
    .flatMap(e => e.evidence)
    .map(r => `${r.type}: ${r.description}`)
    .join('; ');

  const businessName = lead.name || 'Unknown Business';
  
  // Get basic business analysis
  const analysis = await analyzeDescription(allDescriptions, businessName);
  const categoryResult = await categorizeBusinessType(allDescriptions, businessName);
  
  return {
    category: categoryResult.category,
    confidence: analysis.confidence,
    keyFeatures: analysis.keyFeatures,
    source: analysis.source,
  };
}

/**
 * Analyze market opportunity and location with GPT
 */
async function analyzeMarketOpportunity(lead: Lead): Promise<EnhancedAnalysis['marketAnalysis']> {
  // Simple location scoring based on address patterns
  const locationScore = getLocationScore(lead.address);
  
  // Market analysis based on evidence
  const evidenceTypes = new Set(lead.evidence.flatMap(e => e.evidence).map(r => r.type));
  const hasMultipleSignals = evidenceTypes.size > 2;
  
  return {
    locationScore,
    competitiveAdvantage: hasMultipleSignals 
      ? ['Multi-signal validation', 'Strong regulatory compliance', 'Established location']
      : ['Basic compliance', 'Standard location'],
    marketOpportunity: locationScore >= 80 
      ? 'Prime location with high foot traffic potential'
      : locationScore >= 60 
      ? 'Good location with moderate opportunity'
      : 'Standard commercial location',
    riskFactors: lead.score < 90 
      ? ['Limited signal strength', 'Potential delays'] 
      : [],
  };
}

/**
 * Analyze opening timing with enhanced signals
 */
async function analyzeOpeningTiming(lead: Lead): Promise<EnhancedAnalysis['timingAnalysis']> {
  // Check for high-urgency signals
  const hasRecentInspection = lead.evidence.some(e => 
    e.evidence.some(r => 
      r.type?.toLowerCase().includes('food inspection') ||
      r.type?.toLowerCase().includes('building inspection')
    )
  );
  
  const hasRecentLicense = lead.evidence.some(e =>
    e.evidence.some(r => {
      const eventDate = parseDate(r.event_date);
      return eventDate && getAgeInDays(eventDate) <= 30;
    })
  );
  
  // Determine urgency
  let urgency: 'HIGH' | 'MEDIUM' | 'LOW';
  let confidence: number;
  
  if (hasRecentInspection && hasRecentLicense) {
    urgency = 'HIGH';
    confidence = 90;
  } else if (hasRecentInspection || hasRecentLicense) {
    urgency = 'MEDIUM';
    confidence = 75;
  } else {
    urgency = 'LOW';
    confidence = 60;
  }
  
  const keyMilestones: string[] = [];
  if (hasRecentInspection) keyMilestones.push('Recent inspection completed');
  if (hasRecentLicense) keyMilestones.push('License recently issued');
  keyMilestones.push('Permits in progress');
  
  return {
    openingUrgency: urgency,
    keyMilestones,
    predictedTimeframe: urgency === 'HIGH' 
      ? '2-4 weeks' 
      : urgency === 'MEDIUM' 
      ? '4-8 weeks' 
      : '8-12 weeks',
    confidence,
  };
}

/**
 * Generate sales recommendations
 */
async function generateSalesRecommendations(
  lead: Lead, 
  businessAnalysis: EnhancedAnalysis['businessAnalysis'],
  timingAnalysis: EnhancedAnalysis['timingAnalysis']
): Promise<EnhancedAnalysis['salesRecommendations']> {
  
  const isRestaurant = businessAnalysis.category.toLowerCase().includes('restaurant') ||
                      businessAnalysis.category.toLowerCase().includes('food');
  
  const approachStrategy = timingAnalysis.openingUrgency === 'HIGH'
    ? 'URGENT: Contact immediately - opening imminent'
    : timingAnalysis.openingUrgency === 'MEDIUM'
    ? 'PRIORITY: Contact within 48 hours - actively preparing'
    : 'STANDARD: Contact within 1 week - planning phase';
  
  const keyValueProps = isRestaurant
    ? [
        'Restaurant-optimized POS system',
        'Integrated payment processing',
        'Table management and reservations',
        'Kitchen display systems',
        'Inventory management'
      ]
    : [
        'Retail POS solution',
        'Inventory tracking',
        'Customer management',
        'Payment processing',
        'Analytics and reporting'
      ];
  
  const decisionMakers = [
    'Owner/Proprietor',
    'General Manager',
    'Operations Manager'
  ];
  
  const bestContactTiming = timingAnalysis.openingUrgency === 'HIGH'
    ? 'Mornings (9-11 AM) or early evening (4-6 PM) - avoid lunch rush prep'
    : 'Business hours (10 AM - 4 PM) for planning discussions';
  
  return {
    approachStrategy,
    keyValueProps,
    decisionMakers,
    bestContactTiming,
  };
}

/**
 * Simple location scoring based on address patterns
 */
function getLocationScore(address: string): number {
  const addr = address.toLowerCase();
  let score = 50; // Base score
  
  // High-traffic areas
  if (/(state st|michigan ave|clark st|broadway|milwaukee ave)/.test(addr)) {
    score += 30;
  }
  
  // Commercial indicators
  if (/(ave|avenue|blvd|boulevard)/.test(addr)) {
    score += 15;
  }
  
  // Street numbers (higher numbers often indicate main roads)
  const streetNumber = parseInt(address.match(/^\d+/)?.[0] || '0');
  if (streetNumber > 1000) {
    score += 10;
  }
  
  return Math.min(score, 100);
}

/**
 * Main analysis function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting enhanced LLM analysis for Phase 1 leads', args);
    
    // Initialize LLM
    initializeLLM();
    
    // Create storage instance
    const storage = await createStorage();
    
    // Get top leads
    const leads = await storage.getLeadsByCity(args.city);
    const topLeads = leads
      .sort((a, b) => b.score - a.score)
      .slice(0, args.limit);
    
    if (topLeads.length === 0) {
      logger.info('No leads found for analysis', args);
      await storage.close();
      process.exit(0);
    }
    
    logger.info(`Analyzing ${topLeads.length} leads with enhanced GPT analysis`);
    
    const analyses: EnhancedAnalysis[] = [];
    
    // Process each lead
    for (let i = 0; i < topLeads.length; i++) {
      const lead = topLeads[i];
      logger.info(`Processing lead ${i + 1}/${topLeads.length}: ${lead.name || 'Unknown'}`);
      
      try {
        // Run parallel analysis
        const [businessAnalysis, marketAnalysis, timingAnalysis] = await Promise.all([
          analyzeBusinessPotential(lead),
          analyzeMarketOpportunity(lead),
          analyzeOpeningTiming(lead),
        ]);
        
        const salesRecommendations = await generateSalesRecommendations(
          lead, 
          businessAnalysis, 
          timingAnalysis
        );
        
        analyses.push({
          lead,
          businessAnalysis,
          marketAnalysis,
          timingAnalysis,
          salesRecommendations,
        });
        
      } catch (error) {
        logger.error(`Failed to analyze lead: ${lead.name}`, { error });
      }
    }
    
    // Display results
    console.log('\n🚀 **ENHANCED GPT-5-mini LEAD ANALYSIS RESULTS**\n');
    console.log('=' * 80);
    
    analyses.forEach((analysis, index) => {
      const { lead, businessAnalysis, marketAnalysis, timingAnalysis, salesRecommendations } = analysis;
      
      console.log(`\n📊 **LEAD ${index + 1}: ${lead.name?.toUpperCase() || 'UNKNOWN BUSINESS'}**`);
      console.log(`📍 Address: ${lead.address}`);
      console.log(`⭐ Score: ${lead.score} | Predicted Opening: ${lead.evidence[0]?.predicted_open_week}`);
      
      console.log(`\n🏢 **BUSINESS ANALYSIS** (${businessAnalysis.source})`);
      console.log(`   Category: ${businessAnalysis.category}`);
      console.log(`   Confidence: ${businessAnalysis.confidence}%`);
      console.log(`   Key Features: ${businessAnalysis.keyFeatures.join(', ')}`);
      
      console.log(`\n📈 **MARKET ANALYSIS**`);
      console.log(`   Location Score: ${marketAnalysis.locationScore}/100`);
      console.log(`   Market Opportunity: ${marketAnalysis.marketOpportunity}`);
      console.log(`   Competitive Advantages: ${marketAnalysis.competitiveAdvantage.join(', ')}`);
      if (marketAnalysis.riskFactors.length > 0) {
        console.log(`   Risk Factors: ${marketAnalysis.riskFactors.join(', ')}`);
      }
      
      console.log(`\n⏰ **TIMING ANALYSIS**`);
      console.log(`   Opening Urgency: ${timingAnalysis.openingUrgency} (${timingAnalysis.confidence}% confidence)`);
      console.log(`   Predicted Timeframe: ${timingAnalysis.predictedTimeframe}`);
      console.log(`   Key Milestones: ${timingAnalysis.keyMilestones.join(', ')}`);
      
      console.log(`\n💼 **SALES RECOMMENDATIONS**`);
      console.log(`   Approach: ${salesRecommendations.approachStrategy}`);
      console.log(`   Key Value Props: ${salesRecommendations.keyValueProps.slice(0, 3).join(', ')}`);
      console.log(`   Decision Makers: ${salesRecommendations.decisionMakers.join(', ')}`);
      console.log(`   Best Contact Time: ${salesRecommendations.bestContactTiming}`);
      
      console.log('\n' + '-'.repeat(80));
    });
    
    // Summary statistics
    const highUrgency = analyses.filter(a => a.timingAnalysis.openingUrgency === 'HIGH').length;
    const mediumUrgency = analyses.filter(a => a.timingAnalysis.openingUrgency === 'MEDIUM').length;
    const restaurants = analyses.filter(a => 
      a.businessAnalysis.category.toLowerCase().includes('restaurant') ||
      a.businessAnalysis.category.toLowerCase().includes('food')
    ).length;
    
    console.log(`\n📋 **SUMMARY STATISTICS**`);
    console.log(`   Total Leads Analyzed: ${analyses.length}`);
    console.log(`   High Urgency (Contact Today): ${highUrgency}`);
    console.log(`   Medium Urgency (Contact This Week): ${mediumUrgency}`);
    console.log(`   Restaurant/Food Service: ${restaurants}`);
    console.log(`   Average Location Score: ${Math.round(analyses.reduce((sum, a) => sum + a.marketAnalysis.locationScore, 0) / analyses.length)}/100`);
    
    await storage.close();
    
    logger.info('Enhanced LLM analysis completed successfully', {
      totalAnalyzed: analyses.length,
      highUrgency,
      mediumUrgency,
      restaurants,
    });
    
  } catch (error) {
    logger.error('Enhanced LLM analysis failed', { error });
    process.exit(1);
  }
}

// Handle errors
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', { promise, reason });
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', { error: error.message, stack: error.stack });
  process.exit(1);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
