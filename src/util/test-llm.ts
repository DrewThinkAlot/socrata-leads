#!/usr/bin/env node

/**
 * Test script to verify LLM functionality
 */

import { categorizeBusinessType, processAddressWithLLM, analyzeDescription, initializeLLM } from './llm.js';
import { logger } from './logger.js';

async function testLLMFunctionality() {
  console.log('🧪 Testing LLM Functionality with GPT-5-mini');
  console.log('=' .repeat(50));
  
  // Initialize LLM
  initializeLLM();
  
  try {
    // Test 1: Business categorization
    console.log('\n📊 Test 1: Business Categorization');
    const businessDescription = "A family-owned Italian restaurant serving authentic pasta dishes and wood-fired pizzas with outdoor seating and delivery service.";
    const categoryResult = await categorizeBusinessType(businessDescription, "Mario's Pizzeria");
    console.log(`✅ Business Category: ${categoryResult.category} (${categoryResult.source})`);
    
    // Test 2: Address processing
    console.log('\n🏠 Test 2: Address Processing');
    const testAddress = "123 Main Street, Suite 4B, Seattle, WA 98101";
    const addressResult = await processAddressWithLLM(testAddress);
    console.log(`✅ Normalized Address: ${addressResult.normalized}`);
    console.log(`✅ Components:`, addressResult.components);
    
    // Test 3: Description analysis
    console.log('\n🔍 Test 3: Description Analysis');
    const analysisResult = await analyzeDescription(businessDescription, "Mario's Pizzeria");
    console.log(`✅ Business Type: ${analysisResult.businessType}`);
    console.log(`✅ Key Features: ${analysisResult.keyFeatures.join(', ')}`);
    console.log(`✅ Confidence: ${analysisResult.confidence}%`);
    
    console.log('\n🎉 All LLM tests completed successfully!');
    console.log('✅ GPT-5-mini is working correctly');
    
  } catch (error) {
    console.error('\n❌ LLM test failed:', error);
    process.exit(1);
  }
}

// Run the test
testLLMFunctionality().catch(console.error);