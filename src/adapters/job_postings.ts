/**
 * Job posting adapter for restaurant hiring signals
 * Phase 1: Simple implementation, can be enhanced with real APIs later
 */

import { logger } from '../util/logger.js';
import type { NormalizedRecord } from '../types.js';

export interface JobPostingConfig {
  enabled: boolean;
  sources: string[];
  keywords: string[];
  lookback_days: number;
}

/**
 * Mock job posting data for Phase 1 testing
 * In Phase 2, this would integrate with Indeed, LinkedIn, etc.
 */
const MOCK_JOB_POSTINGS = [
  {
    id: 'job-001',
    company: 'GIBSONS STEAK HOUSE',
    title: 'Head Chef',
    address: '1028 N RUSH ST, Chicago, IL',
    posted_date: '2025-07-15',
    description: 'Seeking experienced head chef for upscale steakhouse opening soon',
    lat: 41.9024,
    lon: -87.6267,
  },
  {
    id: 'job-002', 
    company: 'YARDBIRD SOUTHERN TABLE & BAR',
    title: 'Server Staff',
    address: '530 N WABASH AVE, Chicago, IL',
    posted_date: '2025-07-10',
    description: 'Multiple server positions for new Southern cuisine restaurant',
    lat: 41.8927,
    lon: -87.6261,
  },
  {
    id: 'job-003',
    company: 'New Chicago Restaurant',
    title: 'Kitchen Manager', 
    address: '2131 S ARCHER AVE, Chicago, IL',
    posted_date: '2025-08-01',
    description: 'Kitchen manager needed for Asian fusion restaurant opening',
    lat: 41.8515,
    lon: -87.6416,
  }
];

/**
 * Extract job posting data for a city
 */
export async function extractJobPostings(
  city: string,
  config: JobPostingConfig
): Promise<NormalizedRecord[]> {
  
  if (!config.enabled) {
    logger.debug('Job posting extraction disabled');
    return [];
  }

  logger.info('Starting job posting extraction', { city, sources: config.sources });

  try {
    // Phase 1: Use mock data
    // Phase 2: Integrate with real APIs (Indeed, LinkedIn, etc.)
    const records = await fetchMockJobPostings(city, config);
    
    logger.info('Job posting extraction completed', { 
      city, 
      recordCount: records.length 
    });
    
    return records;
    
  } catch (error) {
    logger.error('Job posting extraction failed', { error, city });
    throw error;
  }
}

/**
 * Fetch mock job postings (Phase 1 implementation)
 */
async function fetchMockJobPostings(
  city: string, 
  config: JobPostingConfig
): Promise<NormalizedRecord[]> {
  
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - config.lookback_days);
  
  const filteredPostings = MOCK_JOB_POSTINGS.filter(posting => {
    const postedDate = new Date(posting.posted_date);
    return postedDate >= cutoffDate &&
           config.keywords.some(keyword => 
             posting.title.toLowerCase().includes(keyword.toLowerCase()) ||
             posting.description.toLowerCase().includes(keyword.toLowerCase())
           );
  });

  return filteredPostings.map(posting => ({
    id: posting.id,
    business_name: posting.company,
    address: posting.address,
    lat: posting.lat,
    lon: posting.lon,
    status: 'HIRING',
    event_date: posting.posted_date,
    type: `Job Posting - ${posting.title}`,
    description: posting.description,
    phone: null,
    email: null,
    source_link: `https://indeed.com/job/${posting.id}`,
    payload: posting,
  }));
}

/**
 * Default job posting configuration for restaurants
 */
export const DEFAULT_JOB_CONFIG: JobPostingConfig = {
  enabled: true,
  sources: ['indeed', 'linkedin', 'ziprecruiter'],
  keywords: [
    'chef', 'cook', 'server', 'bartender', 'manager', 'host', 'hostess',
    'kitchen', 'restaurant', 'food service', 'dining', 'culinary'
  ],
  lookback_days: 30,
};

/**
 * Validate job posting configuration
 */
export function validateJobConfig(config: JobPostingConfig): boolean {
  return config.sources.length > 0 && 
         config.keywords.length > 0 && 
         config.lookback_days > 0;
}
