/**
 * Address normalization utilities
 */

/**
 * Normalize an address string for consistent matching
 */
export function normalizeAddress(address: string | null | undefined): string | undefined {
  if (!address || typeof address !== 'string') {
    return undefined;
  }

  return address
    .trim()
    .toUpperCase()
    // Remove extra whitespace
    .replace(/\s+/g, ' ')
    // Normalize common abbreviations
    .replace(/\bSTREET\b/g, 'ST')
    .replace(/\bAVENUE\b/g, 'AVE')
    .replace(/\bBOULEVARD\b/g, 'BLVD')
    .replace(/\bROAD\b/g, 'RD')
    .replace(/\bDRIVE\b/g, 'DR')
    .replace(/\bLANE\b/g, 'LN')
    .replace(/\bCOURT\b/g, 'CT')
    .replace(/\bPLACE\b/g, 'PL')
    .replace(/\bCIRCLE\b/g, 'CIR')
    .replace(/\bPARKWAY\b/g, 'PKWY')
    .replace(/\bNORTH\b/g, 'N')
    .replace(/\bSOUTH\b/g, 'S')
    .replace(/\bEAST\b/g, 'E')
    .replace(/\bWEST\b/g, 'W')
    .replace(/\bNORTHEAST\b/g, 'NE')
    .replace(/\bNORTHWEST\b/g, 'NW')
    .replace(/\bSOUTHEAST\b/g, 'SE')
    .replace(/\bSOUTHWEST\b/g, 'SW')
    // Remove common suffixes that might vary
    .replace(/\s+(UNIT|APT|SUITE|STE|#)\s*\w*$/i, '')
    .trim();
}

/**
 * Extract numeric coordinates from various input formats
 */
export function parseCoordinate(value: any): number | undefined {
  if (typeof value === 'number') {
    return isFinite(value) ? value : undefined;
  }
  
  if (typeof value === 'string') {
    const parsed = parseFloat(value);
    return isFinite(parsed) ? parsed : undefined;
  }
  
  return undefined;
}

/**
 * Validate that coordinates are within reasonable bounds
 */
export function validateCoordinates(lat?: number, lon?: number): { lat?: number; lon?: number } {
  const result: { lat?: number; lon?: number } = {};
  
  // Validate latitude (-90 to 90)
  if (lat !== undefined && lat >= -90 && lat <= 90) {
    result.lat = lat;
  }
  
  // Validate longitude (-180 to 180)
  if (lon !== undefined && lon >= -180 && lon <= 180) {
    result.lon = lon;
  }
  
  return result;
}

/**
 * Calculate distance between two points using Haversine formula
 * Returns distance in meters
 */
export function calculateDistance(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000; // Earth's radius in meters
  const φ1 = (lat1 * Math.PI) / 180;
  const φ2 = (lat2 * Math.PI) / 180;
  const Δφ = ((lat2 - lat1) * Math.PI) / 180;
  const Δλ = ((lon2 - lon1) * Math.PI) / 180;

  const a =
    Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}

/**
 * Check if two addresses are likely the same location
 * Uses both string similarity and coordinate distance if available
 */
export function addressesMatch(
  addr1: string | undefined,
  lat1: number | undefined,
  lon1: number | undefined,
  addr2: string | undefined,
  lat2: number | undefined,
  lon2: number | undefined,
  maxDistanceMeters: number = 100
): boolean {
  // If we have coordinates for both, use distance
  if (lat1 !== undefined && lon1 !== undefined && 
      lat2 !== undefined && lon2 !== undefined) {
    const distance = calculateDistance(lat1, lon1, lat2, lon2);
    return distance <= maxDistanceMeters;
  }
  
  // Fall back to string comparison
  if (!addr1 || !addr2) {
    return false;
  }
  
  const norm1 = normalizeAddress(addr1);
  const norm2 = normalizeAddress(addr2);
  
  if (!norm1 || !norm2) {
    return false;
  }
  
  return norm1 === norm2;
}

/**
 * Extract street number from address
 */
export function extractStreetNumber(address: string): string | undefined {
  const match = address.match(/^\s*(\d+[A-Z]?)\s/);
  return match ? match[1] : undefined;
}

/**
 * Extract street name from address (without number)
 */
export function extractStreetName(address: string): string | undefined {
  const normalized = normalizeAddress(address);
  if (!normalized) return undefined;
  
  // Remove leading street number
  const withoutNumber = normalized.replace(/^\s*\d+[A-Z]?\s+/, '');
  return withoutNumber || undefined;
}