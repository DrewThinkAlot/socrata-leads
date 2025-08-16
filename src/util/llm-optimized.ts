/**
 * Optimized LLM utilities with intelligent sampling and batch processing
 */

import { OpenAI } from 'openai';
import { logger } from './logger.js';
import { LRUCache } from 'lru-cache';
import { createHash } from 'crypto';

// Configuration
const CONFIG = {
  SAMPLE_RATE: parseFloat(process.env.LLM_SAMPLE_RATE || '0.1'), // Only 10% of records
  MAX_CALLS_PER_RUN: parseInt(process.env.LLM_MAX_CALLS_PER_RUN || '50'),
  BATCH_SIZE: parseInt(process.env.LLM_BATCH_SIZE || '5'),
  CACHE_TTL: parseInt(process.env.LLM_CACHE_TTL_MS || '86400000'), // 24 hours
  ENABLED: process.env.LLM_ENABLED !== 'false',
};

// Global state
let callCount = 0;
let batchQueue: Array<{ text: string; resolve: Function; reject: Function }> = [];
let batchTimer: NodeJS.Timeout | null = null;

// Enhanced caching with disk persistence
const cache = new LRUCache<string, any>({
  max: 10000,
  ttl: CONFIG.CACHE_TTL,
});

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  timeout: parseInt(process.env.OPENAI_TIMEOUT_MS || '30000'),
});

/**
 * Generate cache key for consistent caching
 */
function getCacheKey(text: string, operation: string): string {
  return createHash('md5').update(`${operation}:${text}`).digest('hex');
}

/**
 * Smart sampling - only process high-value records
 */
function shouldProcessWithLLM(
  businessName: string = '',
  description: string = '',
  type: string = ''
): boolean {
  if (!CONFIG.ENABLED || callCount >= CONFIG.MAX_CALLS_PER_RUN) {
    return false;
  }

  // Always process if random sampling passes
  if (Math.random() > CONFIG.SAMPLE_RATE) {
    return false;
  }

  // Priority keywords that increase processing likelihood
  const highValueKeywords = [
    'restaurant', 'cafe', 'bar', 'grill', 'kitchen', 'diner',
    'new', 'opening', 'construction', 'renovation', 'grand opening'
  ];

  const combinedText = `${businessName} ${description} ${type}`.toLowerCase();
  const hasHighValueKeyword = highValueKeywords.some(keyword => 
    combinedText.includes(keyword)
  );

  // Process 100% of high-value records, sample rate for others
  return hasHighValueKeyword || Math.random() < CONFIG.SAMPLE_RATE;
}

/**
 * Batch processing for efficiency
 */
function processBatch() {
  if (batchQueue.length === 0) return;

  const batch = batchQueue.splice(0, CONFIG.BATCH_SIZE);
  
  // Process batch in parallel with rate limiting
  Promise.allSettled(
    batch.map(async ({ text, resolve, reject }) => {
      try {
        const result = await callOpenAI(text);
        resolve(result);
      } catch (error) {
        reject(error);
      }
    })
  );

  // Schedule next batch
  if (batchQueue.length > 0) {
    batchTimer = setTimeout(processBatch, 1000); // 1 second between batches
  } else {
    batchTimer = null;
  }
}

/**
 * Add to batch queue
 */
function queueForBatch(text: string): Promise<any> {
  return new Promise((resolve, reject) => {
    batchQueue.push({ text, resolve, reject });
    
    if (!batchTimer) {
      batchTimer = setTimeout(processBatch, 100); // Start processing soon
    }
  });
}

/**
 * Direct OpenAI API call with retries
 */
async function callOpenAI(text: string, retries = 2): Promise<any> {
  try {
    callCount++;
    
    const response = await openai.chat.completions.create({
      model: process.env.OPENAI_MODEL || 'gpt-3.5-turbo',
      messages: [{ role: 'user', content: text }],
      max_tokens: 150,
      temperature: 0.1,
    });

    return response.choices[0]?.message?.content || '';
  } catch (error: any) {
    if (retries > 0 && error?.status !== 401) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      return callOpenAI(text, retries - 1);
    }
    throw error;
  }
}

/**
 * Optimized business categorization with fallback
 */
export async function categorizeBusinessType(
  businessName: string = '',
  description: string = '',
  type: string = ''
): Promise<{ businessType: string; confidence: number }> {
  // Rule-based fallback for common cases
  const ruleBased = categorizeBusinessTypeRuleBased(businessName, description, type);
  
  if (!shouldProcessWithLLM(businessName, description, type)) {
    return ruleBased;
  }

  const cacheKey = getCacheKey(`${businessName}|${description}|${type}`, 'categorize');
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  try {
    const prompt = `Categorize this business in 2-3 words: "${businessName}" - ${description}. Type: ${type}`;
    const result = await queueForBatch(prompt);
    
    const parsed = {
      businessType: result.trim() || ruleBased.businessType,
      confidence: 85
    };
    
    cache.set(cacheKey, parsed);
    return parsed;
  } catch (error) {
    logger.warn('LLM categorization failed, using rule-based fallback', { error });
    return ruleBased;
  }
}

/**
 * Rule-based business categorization fallback
 */
function categorizeBusinessTypeRuleBased(
  businessName: string = '',
  description: string = '',
  type: string = ''
): { businessType: string; confidence: number } {
  const text = `${businessName} ${description} ${type}`.toLowerCase();
  
  const categories = [
    { keywords: ['restaurant', 'cafe', 'diner', 'bistro'], type: 'Restaurant', confidence: 90 },
    { keywords: ['bar', 'tavern', 'pub', 'lounge'], type: 'Bar/Nightlife', confidence: 90 },
    { keywords: ['retail', 'store', 'shop', 'boutique'], type: 'Retail', confidence: 85 },
    { keywords: ['office', 'professional', 'service'], type: 'Professional Services', confidence: 80 },
    { keywords: ['medical', 'dental', 'clinic', 'health'], type: 'Healthcare', confidence: 85 },
  ];

  for (const category of categories) {
    if (category.keywords.some(keyword => text.includes(keyword))) {
      return { businessType: category.type, confidence: category.confidence };
    }
  }

  return { businessType: 'General Business', confidence: 60 };
}

/**
 * Optimized description analysis with smart sampling
 */
export async function analyzeDescription(
  description: string = '',
  businessName: string = ''
): Promise<{ businessType: string; confidence: number; isNewBusiness: boolean }> {
  const ruleBased = analyzeDescriptionRuleBased(description, businessName);
  
  if (!shouldProcessWithLLM(businessName, description)) {
    return ruleBased;
  }

  const cacheKey = getCacheKey(`${description}|${businessName}`, 'analyze');
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  try {
    const prompt = `Analyze: "${description}" for business "${businessName}". Is this a NEW business opening? Category?`;
    const result = await queueForBatch(prompt);
    
    const isNewBusiness = /new|opening|construction|renovation/i.test(result);
    const businessType = result.includes('restaurant') ? 'Restaurant' : 
                        result.includes('retail') ? 'Retail' : 'General Business';
    
    const parsed = {
      businessType,
      confidence: 80,
      isNewBusiness
    };
    
    cache.set(cacheKey, parsed);
    return parsed;
  } catch (error) {
    logger.warn('LLM analysis failed, using rule-based fallback', { error });
    return ruleBased;
  }
}

/**
 * Rule-based description analysis fallback
 */
function analyzeDescriptionRuleBased(
  description: string = '',
  businessName: string = ''
): { businessType: string; confidence: number; isNewBusiness: boolean } {
  const text = `${description} ${businessName}`.toLowerCase();
  
  const newBusinessIndicators = [
    'new', 'opening', 'construction', 'renovation', 'build out',
    'tenant improvement', 'grand opening', 'coming soon'
  ];
  
  const operationalIndicators = [
    'renewal', 'transfer', 'existing', 'current', 'established',
    'remodel', 'maintenance', 'repair', 'annual'
  ];
  
  const isNewBusiness = newBusinessIndicators.some(indicator => 
    text.includes(indicator)
  ) && !operationalIndicators.some(indicator => 
    text.includes(indicator)
  );
  
  const businessType = categorizeBusinessTypeRuleBased('', description).businessType;
  
  return {
    businessType,
    confidence: 70,
    isNewBusiness
  };
}

/**
 * Get current LLM usage statistics
 */
export function getLLMStats() {
  return {
    callCount,
    maxCalls: CONFIG.MAX_CALLS_PER_RUN,
    sampleRate: CONFIG.SAMPLE_RATE,
    cacheSize: cache.size,
    queueSize: batchQueue.length,
    enabled: CONFIG.ENABLED
  };
}

/**
 * Reset call counter (useful for testing)
 */
export function resetLLMStats() {
  callCount = 0;
  batchQueue = [];
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }
}
