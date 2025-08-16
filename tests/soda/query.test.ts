import { describe, it, expect } from 'vitest';
import { formatDate, buildWatermarkCondition, buildIncrementalQuery } from '../../src/soda/query.js';

describe('SODA Query Utilities', () => {
  describe('formatDate', () => {
    it('should format dates in Socrata-compatible format', () => {
      const date = new Date('2024-01-15T14:30:45.123Z');
      const formatted = formatDate(date);
      expect(formatted).toBe("'2024-01-15T14:30:45'");
    });

    it('should handle edge cases like midnight', () => {
      const date = new Date('2024-01-01T00:00:00.000Z');
      const formatted = formatDate(date);
      expect(formatted).toBe("'2024-01-01T00:00:00'");
    });

    it('should handle end of year', () => {
      const date = new Date('2024-12-31T23:59:59.999Z');
      const formatted = formatDate(date);
      expect(formatted).toBe("'2024-12-31T23:59:59'");
    });
  });

  describe('buildWatermarkCondition', () => {
    it('should format date watermarks correctly', () => {
      const watermarkField = 'application_start_date';
      const lastWatermark = '2024-01-15T14:30:45.123Z';
      const condition = buildWatermarkCondition(watermarkField, lastWatermark);
      expect(condition).toBe("application_start_date > '2024-01-15T14:30:45'");
    });

    it('should handle string watermarks', () => {
      const watermarkField = 'permit_';
      const lastWatermark = 'PERMIT-12345';
      const condition = buildWatermarkCondition(watermarkField, lastWatermark);
      expect(condition).toBe("permit_ > 'PERMIT-12345'");
    });

    it('should return empty string for null watermark', () => {
      const condition = buildWatermarkCondition('field', null);
      expect(condition).toBe('');
    });
  });

  describe('buildIncrementalQuery', () => {
    it('should build complete query with date watermark', () => {
      const query = buildIncrementalQuery(
        ['permit_', 'permit_type', 'application_start_date'],
        "upper(permit_status) IN ('ISSUED','RELEASED','PERMIT ISSUED')",
        'application_start_date',
        'application_start_date',
        '2024-01-15T14:30:45.123Z',
        1000
      );

      expect(query.$select).toBe('permit_, permit_type, application_start_date');
      expect(query.$where).toContain("upper(permit_status) IN ('ISSUED','RELEASED','PERMIT ISSUED')");
      expect(query.$where).toContain("application_start_date > '2024-01-15T14:30:45'");
      expect(query.$order).toBe('application_start_date ASC');
      expect(query.$limit).toBe(1000);
    });
  });
});
