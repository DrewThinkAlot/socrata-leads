/**
 * Exponential backoff utility with jitter for API rate limiting
 */

import { logger } from './logger.js';

export interface BackoffOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  jitterFactor?: number;
}

export class BackoffError extends Error {
  constructor(message: string, public attempts: number) {
    super(message);
    this.name = 'BackoffError';
  }
}

/**
 * Execute a function with exponential backoff retry logic
 */
export async function withBackoff<T>(
  fn: () => Promise<T>,
  options: BackoffOptions = {}
): Promise<T> {
  const {
    maxRetries = 6,
    initialDelayMs = 1000,
    maxDelayMs = 30000,
    jitterFactor = 0.1,
  } = options;

  let lastError: Error;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      
      if (attempt === maxRetries) {
        logger.error(`Max retries (${maxRetries}) exceeded`, {
          error: lastError.message,
          attempts: attempt + 1,
        });
        throw new BackoffError(
          `Failed after ${attempt + 1} attempts: ${lastError.message}`,
          attempt + 1
        );
      }

      // Calculate delay with exponential backoff and jitter
      const baseDelay = Math.min(initialDelayMs * Math.pow(2, attempt), maxDelayMs);
      const jitter = baseDelay * jitterFactor * Math.random();
      const delay = Math.floor(baseDelay + jitter);

      logger.warn(`Attempt ${attempt + 1} failed, retrying in ${delay}ms`, {
        error: lastError.message,
        attempt: attempt + 1,
        maxRetries,
        delay,
      });

      await sleep(delay);
    }
  }

  // This should never be reached, but TypeScript requires it
  throw lastError!;
}

/**
 * Handle HTTP 429 (Too Many Requests) with Retry-After header
 */
export async function handleRateLimit(
  retryAfterSeconds?: number,
  options: BackoffOptions = {}
): Promise<void> {
  if (retryAfterSeconds) {
    const delayMs = retryAfterSeconds * 1000;
    const maxDelayMs = options.maxDelayMs || 30000;
    
    // Respect Retry-After but cap at maxDelayMs
    const actualDelay = Math.min(delayMs, maxDelayMs);
    
    logger.warn(`Rate limited, waiting ${actualDelay}ms (Retry-After: ${retryAfterSeconds}s)`);
    await sleep(actualDelay);
  } else {
    // Fall back to exponential backoff
    const delay = options.initialDelayMs || 1000;
    logger.warn(`Rate limited, using default backoff delay: ${delay}ms`);
    await sleep(delay);
  }
}

/**
 * Sleep for the specified number of milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Parse Retry-After header value (seconds or HTTP date)
 */
export function parseRetryAfter(retryAfter: string): number | undefined {
  // Try parsing as seconds first
  const seconds = parseInt(retryAfter, 10);
  if (!isNaN(seconds)) {
    return seconds;
  }

  // Try parsing as HTTP date
  const date = new Date(retryAfter);
  if (!isNaN(date.getTime())) {
    const now = new Date();
    const diffMs = date.getTime() - now.getTime();
    return Math.max(0, Math.ceil(diffMs / 1000));
  }

  return undefined;
}