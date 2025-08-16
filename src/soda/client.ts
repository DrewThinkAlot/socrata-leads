/**
 * Socrata Open Data API (SODA) client with retry logic
 */

import { withBackoff, handleRateLimit, parseRetryAfter } from '../util/backoff.js';
import { logger } from '../util/logger.js';
import { SocrataError, type SocrataClientOptions } from '../types.js';

/**
 * Default request timeout in milliseconds
 */
const DEFAULT_TIMEOUT = 30000;

/**
 * SODA API client class
 */
export class SodaClient {
  private baseUrl: string;
  private appToken: string | undefined;
  private timeout: number;

  constructor(baseUrl: string, appToken?: string, timeout: number = DEFAULT_TIMEOUT) {
    this.baseUrl = baseUrl.replace(/\/$/, ''); // Remove trailing slash
    this.appToken = appToken;
    this.timeout = timeout;
  }

  /**
   * Make a GET request to the SODA API with retry logic
   */
  async getJson(options: SocrataClientOptions): Promise<any[]> {
    const { path, params, appToken } = options;
    const url = this.buildUrl(path, params);
    const headers = this.buildHeaders(appToken || this.appToken);

    logger.debug('Making SODA API request', { url, headers: this.sanitizeHeaders(headers) });

    return withBackoff(async () => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);

      try {
        const response = await fetch(url, {
          method: 'GET',
          headers,
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        // Handle rate limiting (429)
        if (response.status === 429) {
          const retryAfter = response.headers.get('Retry-After');
          const retrySeconds = retryAfter ? parseRetryAfter(retryAfter) : undefined;
          
          await handleRateLimit(retrySeconds);
          
          throw new SocrataError(
            'Rate limited by Socrata API',
            429,
            retrySeconds
          );
        }

        // Handle other HTTP errors
        if (!response.ok) {
          const errorText = await response.text().catch(() => 'Unknown error');
          throw new SocrataError(
            `HTTP ${response.status}: ${errorText}`,
            response.status
          );
        }

        // Parse JSON response
        const data = await response.json();
        
        if (!Array.isArray(data)) {
          throw new SocrataError('Expected array response from Socrata API');
        }

        logger.debug('SODA API request successful', { 
          url, 
          recordCount: data.length,
          status: response.status 
        });

        return data;

      } catch (error) {
        clearTimeout(timeoutId);
        
        if (error instanceof SocrataError) {
          throw error;
        }
        
        if (error instanceof Error) {
          if (error.name === 'AbortError') {
            throw new SocrataError(`Request timeout after ${this.timeout}ms`);
          }
          
          throw new SocrataError(`Network error: ${error.message}`);
        }
        
        throw new SocrataError(`Unknown error: ${error}`);
      }
    }, {
      maxRetries: 6,
      initialDelayMs: 1000,
      maxDelayMs: 30000,
    });
  }

  /**
   * Build full URL with query parameters
   */
  private buildUrl(path: string, params?: Record<string, any>): string {
    const cleanPath = path.startsWith('/') ? path : `/${path}`;
    let url = `${this.baseUrl}${cleanPath}`;
    
    if (params && Object.keys(params).length > 0) {
      const searchParams = new URLSearchParams();
      
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null) {
          searchParams.append(key, String(value));
        }
      }
      
      if (searchParams.toString()) {
        url += `?${searchParams.toString()}`;
      }
    }
    
    return url;
  }

  /**
   * Build request headers
   */
  private buildHeaders(appToken?: string): Record<string, string> {
    const headers: Record<string, string> = {
      'Accept': 'application/json',
      'User-Agent': 'socrata-leads-pipeline/1.0',
    };

    if (appToken) {
      headers['X-App-Token'] = appToken;
    }

    return headers;
  }

  /**
   * Sanitize headers for logging (remove sensitive tokens)
   */
  private sanitizeHeaders(headers: Record<string, string>): Record<string, string> {
    const sanitized = { ...headers };
    if (sanitized['X-App-Token']) {
      sanitized['X-App-Token'] = '***';
    }
    return sanitized;
  }

  /**
   * Test connection to the SODA API
   */
  async testConnection(): Promise<boolean> {
    try {
      // Try to fetch a small amount of data from a common endpoint
      await this.getJson({
        baseUrl: this.baseUrl,
        path: '/api/views.json',
        params: { $limit: 1 },
      });
      return true;
    } catch (error) {
      logger.warn('SODA API connection test failed', { error });
      return false;
    }
  }

  /**
   * Get API metadata for debugging
   */
  async getApiInfo(): Promise<any> {
    return this.getJson({
      baseUrl: this.baseUrl,
      path: '/api/views.json',
      params: { $limit: 1 },
    });
  }
}

/**
 * Create a SODA client instance
 */
export function createSodaClient(baseUrl: string, appToken?: string): SodaClient {
  return new SodaClient(baseUrl, appToken);
}

/**
 * Convenience function for one-off requests
 */
export async function getJson(options: SocrataClientOptions): Promise<any[]> {
  const client = new SodaClient(options.baseUrl, options.appToken);
  return client.getJson(options);
}