/**
 * Tests for SpotOn filter functionality
 */

import { describe, it, expect } from 'vitest';
import { analyzeSpotOnIntelligence } from '../../src/filters/spoton.js';
import type { Event, NormalizedRecord } from '../../src/types.js';

describe('SpotOn Intelligence Analysis', () => {
  it('should detect full-service restaurant with liquor license', async () => {
    const mockEvent: Event = {
      event_id: 'test-1',
      city: 'chicago',
      address: '123 Main St',
      name: 'Test Restaurant',
      predicted_open_week: '2024-01-15',
      signal_strength: 85,
      evidence: [
        {
          uid: 'record-1',
          city: 'chicago',
          dataset: 'building_permits',
          business_name: 'Test Restaurant',
          address: '123 Main St',
          type: 'Restaurant Permit',
          description: 'Full-service restaurant with bar and 50-seat capacity',
          event_date: '2024-01-01',
          status: 'ISSUED',
          source_link: 'https://example.com/record-1',
          raw_id: '1',
          created_at: '2024-01-01T00:00:00Z'
        }
      ],
      created_at: '2024-01-01T00:00:00Z'
    };

    const intelligence = await analyzeSpotOnIntelligence([mockEvent]);
    
    expect(intelligence.business_category).toBe('Restaurant');
    expect(intelligence.service_model).toBe('full-service');
    expect(intelligence.seat_capacity).toBe(50);
    expect(intelligence.spoton_score).toBeGreaterThan(0);
  });

  it('should detect fast-casual restaurant', async () => {
    const mockEvent: Event = {
      event_id: 'test-2',
      city: 'chicago',
      address: '456 Oak Ave',
      name: 'Fast Casual Eats',
      predicted_open_week: '2024-02-01',
      signal_strength: 75,
      evidence: [
        {
          uid: 'record-3',
          city: 'chicago',
          dataset: 'building_permits',
          business_name: 'Fast Casual Eats',
          address: '456 Oak Ave',
          type: 'Restaurant Permit',
          description: 'Fast-casual restaurant with 30-seat capacity',
          event_date: '2024-01-15',
          status: 'ISSUED',
          source_link: 'https://example.com/record-3',
          raw_id: '3',
          created_at: '2024-01-15T00:00:00Z'
        }
      ],
      created_at: '2024-01-15T00:00:00Z'
    };

    const intelligence = await analyzeSpotOnIntelligence([mockEvent]);
    
    expect(intelligence.business_category).toBe('Restaurant');
    expect(intelligence.service_model).toBe('fast-casual');
    expect(intelligence.seat_capacity).toBe(30);
  });

  it('should detect chain expansion', async () => {
    const mockEvent: Event = {
      event_id: 'test-3',
      city: 'chicago',
      address: '789 Pine St',
      name: 'Chain Restaurant #2',
      predicted_open_week: '2024-03-01',
      signal_strength: 90,
      evidence: [
        {
          uid: 'record-4',
          city: 'chicago',
          dataset: 'building_permits',
          business_name: 'Chain Restaurant #2',
          address: '789 Pine St',
          type: 'Restaurant Permit',
          description: 'Second location for established restaurant chain',
          event_date: '2024-02-01',
          status: 'ISSUED',
          source_link: 'https://example.com/record-4',
          raw_id: '4',
          created_at: '2024-02-01T00:00:00Z'
        }
      ],
      created_at: '2024-02-01T00:00:00Z'
    };

    const intelligence = await analyzeSpotOnIntelligence([mockEvent]);
    
    expect(intelligence.operator_type).toBe('chain-expansion');
  });
});
