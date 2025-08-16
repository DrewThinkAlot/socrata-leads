import { describe, it, expect, beforeEach } from 'vitest';
import { analyzeAddressForNewBusiness } from '../../src/score/run.js';
import type { Event, NormalizedRecord } from '../../src/types.js';

describe('Operational Filtering Tests', () => {
  const now = new Date('2024-08-13');
  
  // Helper to create test records
  const createRecord = (overrides: Partial<NormalizedRecord>): NormalizedRecord => ({
    uid: 'test-uid',
    city: 'chicago',
    dataset: 'test',
    raw_id: 'test',
    created_at: now.toISOString(),
    ...overrides
  });

  // Helper to create test events
  const createEvent = (address: string, records: NormalizedRecord[]): Event => ({
    event_id: 'test-event',
    city: 'chicago',
    address,
    name: 'Test Business',
    predicted_open_week: '2024-09-01',
    signal_strength: 75,
    evidence: records,
    created_at: now.toISOString()
  });

  describe('Should FILTER OUT operational restaurants', () => {
    it('should filter out restaurant with recent food safety inspection', async () => {
      const address = '123 Main St, Chicago, IL';
      
      const inspections = [
        createRecord({
          dataset: 'food_inspections',
          type: 'Food Safety Inspection',
          description: 'Routine food safety check',
          status: 'PASS',
          event_date: '2024-07-15', // 29 days ago
          address
        })
      ];
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-06-01',
          address
        })
      ];
      
      const businessLicenses = [
        createRecord({
          dataset: 'business_licenses',
          type: 'Business License',
          status: 'AAC',
          event_date: '2023-01-01', // Established business
          address
        })
      ];
      
      const events = [createEvent(address, [...inspections, ...licenses, ...businessLicenses])];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        businessLicenses,
        inspections,
        now
      );
      
      expect(result).toBe(false);
    });

    it('should filter out restaurant with active business license >90 days', async () => {
      const address = '456 Oak Ave, Chicago, IL';
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-05-01',
          address
        })
      ];
      
      const businessLicenses = [
        createRecord({
          dataset: 'business_licenses',
          type: 'Business License',
          status: 'AAC',
          event_date: '2023-06-01', // >90 days ago
          address
        })
      ];
      
      const events = [createEvent(address, [...licenses, ...businessLicenses])];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        businessLicenses,
        [],
        now
      );
      
      expect(result).toBe(false);
    });

    it('should filter out restaurant with long license history', async () => {
      const address = '789 Pine St, Chicago, IL';
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-08-01',
          address
        }),
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAC',
          event_date: '2022-01-01', // Old license
          address
        }),
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAC',
          event_date: '2021-01-01', // Older license
          address
        })
      ];
      
      const events = [createEvent(address, licenses)];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        [],
        [],
        now
      );
      
      expect(result).toBe(false);
    });
  });

  describe('Should ACCEPT new restaurant openings', () => {
    it('should accept restaurant with future liquor license and construction', async () => {
      const address = '321 New St, Chicago, IL';
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-09-01', // Future date
          address
        })
      ];
      
      const permits = [
        createRecord({
          dataset: 'building_permits',
          type: 'Building Permit',
          description: 'Restaurant build-out and tenant improvement',
          event_date: '2024-08-01', // Very recent construction
          address
        })
      ];
      
      const events = [createEvent(address, [...licenses, ...permits])];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        [],
        [],
        now
      );
      
      expect(result).toBe(true);
    });

    it('should accept restaurant with recent activity and opening signals', async () => {
      const address = '654 Startup Blvd, Chicago, IL';
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-08-10', // Very recent
          address
        })
      ];
      
      const permits = [
        createRecord({
          dataset: 'building_permits',
          type: 'Building Permit',
          description: 'New restaurant construction',
          event_date: '2024-08-05', // Very recent
          address
        })
      ];
      
      const events = [createEvent(address, [...licenses, ...permits])];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        [],
        [],
        now
      );
      
      expect(result).toBe(true);
    });

    it('should accept restaurant with only licensing inspections', async () => {
      const address = '987 Opening Way, Chicago, IL';
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-08-10',
          address
        })
      ];
      
      const inspections = [
        createRecord({
          dataset: 'food_inspections',
          type: 'Food License Inspection',
          description: 'Pre-opening licensing inspection',
          status: 'PASS',
          event_date: '2024-08-12',
          address
        })
      ];
      
      const events = [createEvent(address, [...licenses, ...inspections])];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        [],
        inspections,
        now
      );
      
      expect(result).toBe(true);
    });
  });

  describe('Edge cases', () => {
    it('should filter out old activity (>60 days)', async () => {
      const address = '111 Old St, Chicago, IL';
      
      const licenses = [
        createRecord({
          dataset: 'liquor_licenses',
          type: 'Liquor License',
          status: 'AAI',
          event_date: '2024-06-01', // >60 days ago
          address
        })
      ];
      
      const events = [createEvent(address, licenses)];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        licenses,
        [],
        [],
        now
      );
      
      expect(result).toBe(false);
    });

    it('should require liquor license', async () => {
      const address = '222 No License St, Chicago, IL';
      
      const permits = [
        createRecord({
          dataset: 'building_permits',
          type: 'Building Permit',
          description: 'Restaurant construction',
          event_date: '2024-08-01',
          address
        })
      ];
      
      const events = [createEvent(address, permits)];
      
      const result = await analyzeAddressForNewBusiness(
        address,
        events,
        [],
        [],
        [],
        now
      );
      
      expect(result).toBe(false);
    });
  });
});
