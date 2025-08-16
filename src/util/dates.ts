/**
 * Date utilities for ISO week calculations and date parsing
 */

import { format, parseISO, startOfWeek, addWeeks, isValid } from 'date-fns';

/**
 * Get ISO week string for a date (YYYY-WXX format)
 */
export function getISOWeek(date: Date): string {
  const year = date.getFullYear();
  const week = getWeekNumber(date);
  return `${year}-W${week.toString().padStart(2, '0')}`;
}

/**
 * Get ISO week number for a date (1-53)
 */
export function getWeekNumber(date: Date): number {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
}

/**
 * Parse various date formats into a Date object
 */
export function parseDate(dateStr: string | null | undefined): Date | null {
  if (!dateStr || typeof dateStr !== 'string') {
    return null;
  }

  // Try ISO format first
  try {
    const isoDate = parseISO(dateStr);
    if (isValid(isoDate)) {
      return isoDate;
    }
  } catch {
    // Continue to other formats
  }

  // Try common formats
  const formats = [
    // ISO variants
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/,
    /^\d{4}-\d{2}-\d{2}$/,
    // US formats
    /^\d{1,2}\/\d{1,2}\/\d{4}$/,
    /^\d{1,2}-\d{1,2}-\d{4}$/,
    // Other common formats
    /^\d{4}\/\d{2}\/\d{2}$/,
  ];

  for (const format of formats) {
    if (format.test(dateStr)) {
      const date = new Date(dateStr);
      if (isValid(date)) {
        return date;
      }
    }
  }

  return null;
}

/**
 * Format date as ISO string (YYYY-MM-DD)
 */
export function formatDateISO(date: Date): string {
  return format(date, 'yyyy-MM-dd');
}

/**
 * Format date as ISO datetime string
 */
export function formatDateTimeISO(date: Date): string {
  return date.toISOString();
}

/**
 * Get the start of the week for a given date (Monday)
 */
export function getWeekStart(date: Date): Date {
  return startOfWeek(date, { weekStartsOn: 1 }); // Monday
}

/**
 * Get predicted opening week based on event date and type
 */
export function getPredictedOpenWeek(eventDate: Date, eventType: string): string {
  let weeksToAdd = 0;

  // Adjust prediction based on event type
  const type = eventType.toLowerCase();
  if (type.includes('permit')) {
    // Building permits typically take 8-16 weeks
    weeksToAdd = 12;
  } else if (type.includes('license')) {
    // Licenses are usually closer to opening
    weeksToAdd = 4;
  } else {
    // Default prediction
    weeksToAdd = 8;
  }

  const predictedDate = addWeeks(eventDate, weeksToAdd);
  return getISOWeek(predictedDate);
}

/**
 * Check if a date is within the last N days
 */
export function isWithinDays(date: Date, days: number): boolean {
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffDays = diffMs / (1000 * 60 * 60 * 24);
  return diffDays <= days;
}

/**
 * Get date N days ago
 */
export function getDaysAgo(days: number): Date {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return date;
}

/**
 * Parse ISO week string back to date (start of week)
 */
export function parseISOWeek(weekStr: string): Date | null {
  const match = weekStr.match(/^(\d{4})-W(\d{2})$/);
  if (!match) {
    return null;
  }

  const year = parseInt(match[1]!, 10);
  const week = parseInt(match[2]!, 10);

  // January 4th is always in the first week of the year
  const jan4 = new Date(year, 0, 4);
  const startOfYear = getWeekStart(jan4);
  
  return addWeeks(startOfYear, week - 1);
}

/**
 * Get current ISO week string
 */
export function getCurrentWeek(): string {
  return getISOWeek(new Date());
}

/**
 * Check if two dates are in the same week
 */
export function isSameWeek(date1: Date, date2: Date): boolean {
  return getISOWeek(date1) === getISOWeek(date2);
}

/**
 * Calculate age of a date in days
 */
export function getAgeInDays(date: Date): number {
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  return Math.floor(diffMs / (1000 * 60 * 60 * 24));
}