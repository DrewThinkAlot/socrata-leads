/**
 * Simple logger utility with levels and timing support
 */

import type { Logger, LogLevel } from '../types.js';

class SimpleLogger implements Logger {
  private level: LogLevel;
  private levelPriority: Record<LogLevel, number> = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
  };

  constructor(level: LogLevel = 'info') {
    this.level = level;
  }

  private shouldLog(level: LogLevel): boolean {
    return this.levelPriority[level] >= this.levelPriority[this.level];
  }

  private formatMessage(level: LogLevel, message: string, meta?: Record<string, any>): string {
    const timestamp = new Date().toISOString();
    const levelStr = level.toUpperCase().padEnd(5);
    const metaStr = meta ? ` ${JSON.stringify(meta)}` : '';
    return `${timestamp} [${levelStr}] ${message}${metaStr}`;
  }

  debug(message: string, meta?: Record<string, any>): void {
    if (this.shouldLog('debug')) {
      console.log(this.formatMessage('debug', message, meta));
    }
  }

  info(message: string, meta?: Record<string, any>): void {
    if (this.shouldLog('info')) {
      console.log(this.formatMessage('info', message, meta));
    }
  }

  warn(message: string, meta?: Record<string, any>): void {
    if (this.shouldLog('warn')) {
      console.warn(this.formatMessage('warn', message, meta));
    }
  }

  error(message: string, meta?: Record<string, any>): void {
    if (this.shouldLog('error')) {
      console.error(this.formatMessage('error', message, meta));
    }
  }

  /**
   * Create a timer function that logs elapsed time when called
   */
  timer(label: string): () => void {
    const start = Date.now();
    return () => {
      const elapsed = Date.now() - start;
      this.info(`${label} completed in ${elapsed}ms`);
    };
  }

  /**
   * Set the log level
   */
  setLevel(level: LogLevel): void {
    this.level = level;
  }
}

// Create singleton logger instance
const logLevel = (process.env.LOG_LEVEL as LogLevel) || 'info';
export const logger = new SimpleLogger(logLevel);