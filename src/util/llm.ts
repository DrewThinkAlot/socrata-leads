/**
 * LLM utilities for the socrata-leads pipeline — refactored & trimmed
 * - Centralized config
 * - Unified cache (memory+disk) & in-flight de-dupe
 * - Single LLM exec with retries & JSON parsing
 * - Generic helper (llmTask) to remove repetition
 */

import { config as loadEnv } from 'dotenv';
import { logger } from './logger.js';
import fs from 'node:fs/promises';
import path from 'node:path';
loadEnv();

// -------------------------
// Config
// -------------------------
const CFG = {
  OPENAI_API_KEY: process.env.OPENAI_API_KEY ?? '',
  OPENAI_MODEL: process.env.OPENAI_MODEL || 'gpt-5-mini',
  OPENAI_API_URL: process.env.OPENAI_API_URL || 'https://api.openai.com/v1/chat/completions',
  OPENAI_TIMEOUT_MS: num(process.env.OPENAI_TIMEOUT_MS, 30_000),
  LLM_ENABLED: (process.env.LLM_ENABLED ?? 'true').toLowerCase() !== 'false',
  LLM_MAX_CALLS_PER_RUN: num(process.env.LLM_MAX_CALLS_PER_RUN, 0), // 0 = unlimited
  LLM_SAMPLE_RATE: clamp(parseFloat(process.env.LLM_SAMPLE_RATE ?? '1'), 0, 1),
  LLM_MAX_RETRIES: num(process.env.LLM_MAX_RETRIES, 3),
  LLM_DISK_CACHE_DIR: process.env.LLM_DISK_CACHE_DIR || '.cache/llm',
  LLM_CACHE_TTL_MS: num(process.env.LLM_CACHE_TTL_MS, 300_000), // 5m
};

function num(v: string | undefined, d: number) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function clamp(n: number, min: number, max: number) { return Math.max(min, Math.min(max, n)); }

// -------------------------
// Cache (memory + disk)
// -------------------------
const mem = new Map<string, { t: number; r: any }>();
const inFlight = new Map<string, Promise<any>>();
let callsThisRun = 0;

const TTL = CFG.LLM_CACHE_TTL_MS;
const MAX_MEM = 100;

const ensureDir = async () => fs.mkdir(CFG.LLM_DISK_CACHE_DIR, { recursive: true }).catch(() => {});
const diskPath = (k: string) => path.join(CFG.LLM_DISK_CACHE_DIR, `${k}.json`);

function hashKey(...parts: any[]) {
  const s = parts.map(p => JSON.stringify(p)).join('|');
  let h = 0; for (let i = 0; i < s.length; i++) { h = (h << 5) - h + s.charCodeAt(i); h |= 0; }
  return String(h >>> 0);
}

function fromMem(key: string) {
  const v = mem.get(key); if (!v) return null; if (Date.now() - v.t > TTL) { mem.delete(key); return null; } return v.r;
}
function toMem(key: string, r: any) {
  if (mem.size >= MAX_MEM) { const k = mem.keys().next().value; if (k) mem.delete(k); }
  mem.set(key, { t: Date.now(), r });
}

async function fromDisk(key: string) {
  try {
    const raw = await fs.readFile(diskPath(key), 'utf8');
    const obj = JSON.parse(raw);
    if (Date.now() - obj.t > TTL) { await fs.unlink(diskPath(key)).catch(() => {}); return null; }
    return obj.r;
  } catch { return null; }
}
async function toDisk(key: string, r: any) {
  try { await ensureDir(); await fs.writeFile(diskPath(key), JSON.stringify({ t: Date.now(), r }), 'utf8'); } catch {}
}

// -------------------------
// LLM core
// -------------------------
function shouldUseLLM(sampleKey: string) {
  // Always use the LLM when an API key is present. Ignore sampling and per-run call caps.
  return !!CFG.OPENAI_API_KEY;
}

export interface OpenAIRequestOptions {
  model?: string; temperature?: number; max_tokens?: number; timeout?: number; force_json?: boolean;
}
export interface OpenAIResponse {
  choices: Array<{ message: { role: string; content: string } }>; usage?: any; id?: string; model?: string;
}

async function callOpenAI(
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>,
  opts: OpenAIRequestOptions = {}
): Promise<OpenAIResponse> {
  const model = opts.model ?? CFG.OPENAI_MODEL;
  const temperature = opts.temperature ?? 0.2;
  const max_tokens = opts.max_tokens ?? 400;
  const timeout = opts.timeout ?? CFG.OPENAI_TIMEOUT_MS;

  const baseDelay = 2000; // Increased base delay for better handling of API issues
  // Prefer the newer parameter name for newer models that reject max_tokens
  let preferMaxCompletionTokens = /^(gpt-5|gpt-4\.1|o4|o3)/i.test(model);
  // Some newer models only support default temperature and will reject custom values
  let omitTemperature = /^(gpt-5|gpt-4\.1|o4|o3)/i.test(model);
  // Some models/endpoints gate json_object unless prompt contains 'json'; adapt by disabling response_format
  let omitResponseFormat = false;
  for (let attempt = 0; attempt <= CFG.LLM_MAX_RETRIES; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      callsThisRun++;
      // Build request body with adaptive params
      const body: any = { model, messages };
      if (opts.force_json && !omitResponseFormat) { body.response_format = { type: 'json_object' }; }
      if (!omitTemperature) { body.temperature = temperature; }
      const isResponsesApi = /\/v1\/responses$/.test(CFG.OPENAI_API_URL);
      if (isResponsesApi) {
        // Responses API expects max_output_tokens
        body.max_output_tokens = max_tokens;
      } else if (preferMaxCompletionTokens) {
        // Newer chat models expect max_completion_tokens
        body.max_completion_tokens = max_tokens;
      } else {
        // Legacy/chat-completions models expect max_tokens
        body.max_tokens = max_tokens;
      }

      const res = await fetch(CFG.OPENAI_API_URL, {
        method: 'POST',
        headers: { Authorization: `Bearer ${CFG.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (!res.ok) {
        const txt = await res.text();
        // If the server reports that max_tokens is unsupported, switch param name and retry immediately
        try {
          const err = JSON.parse(txt);
          const code = err?.error?.code || err?.error?.type;
          const param = err?.error?.param;
          const message: string = err?.error?.message || '';
          const suggestsCompletionParam = /max_completion_tokens/i.test(message);
          const complainsAboutMaxTokens = param === 'max_tokens' || /max_tokens/i.test(message);
          if (res.status === 400 && complainsAboutMaxTokens && suggestsCompletionParam) {
            if (!preferMaxCompletionTokens) {
              preferMaxCompletionTokens = true;
              // immediate retry without counting as a failed attempt beyond this loop iteration
              const delay = 100; await new Promise(r => setTimeout(r, delay));
              attempt--; // do not advance the attempt counter for this param-switch retry
              continue;
            }
          }
          // If the server reports temperature unsupported, omit it and retry
          const complainsAboutTemperature = param === 'temperature' || /temperature/i.test(message);
          const unsupportedValue = code === 'unsupported_value' || /unsupported/i.test(message);
          if (res.status === 400 && complainsAboutTemperature && unsupportedValue) {
            if (!omitTemperature) {
              omitTemperature = true;
              const delay = 100; await new Promise(r => setTimeout(r, delay));
              attempt--;
              continue;
            }
          }
          // If the server gates json_object without 'json' mention, drop response_format
          const mentionsJsonWordRequirement = /response_format[\s\S]*json_object/i.test(message) || /must contain the word 'json'/i.test(message);
          if (res.status === 400 && mentionsJsonWordRequirement && !omitResponseFormat) {
            omitResponseFormat = true;
            const delay = 100; await new Promise(r => setTimeout(r, delay));
            attempt--;
            continue;
          }
        } catch {}
        const retriable = res.status === 429 || res.status >= 500;
        if (retriable && attempt < CFG.LLM_MAX_RETRIES) {
          const delay = baseDelay * 2 ** attempt + Math.random() * 1000; logger.warn('OpenAI retry', { attempt: attempt + 1, status: res.status });
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        throw new Error(`OpenAI ${res.status}: ${txt}`);
      }
      return await res.json();
    } catch (e: any) {
      clearTimeout(timer);
      if (attempt < CFG.LLM_MAX_RETRIES) {
        const delay = baseDelay * 2 ** attempt + Math.random() * 1000; logger.warn('OpenAI error, retrying', { attempt: attempt + 1, error: String(e?.message ?? e) });
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw e;
    }
  }
  throw new Error('Max retries exceeded');
}

function parseFirstJson(text: string): any | null {
  if (!text) return null;
  try { return JSON.parse(text); } catch {}
  const start = text.indexOf('{'); const end = text.lastIndexOf('}');
  if (start >= 0 && end > start) { try { return JSON.parse(text.slice(start, end + 1)); } catch {} }
  const m = /```(?:json)?\s*([\s\S]*?)\s*```/i.exec(text); if (m?.[1]) { try { return JSON.parse(m[1]); } catch {} }
  return null;
}

// -------------------------
// Generic LLM task with cache + fallback
// -------------------------
async function llmTask<T>({
  name, keyParts, messages, parse, fallback, sampleKey = '', opts,
}: {
  name: string;
  keyParts: any[];
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>;
  parse: (raw: string) => T | null;
  fallback: () => T;
  sampleKey?: string;
  opts?: OpenAIRequestOptions;
}): Promise<T> {
  const key = hashKey(name, ...keyParts);

  // memory cache
  const m = fromMem(key); if (m) return m;
  // disk cache
  const d = await fromDisk(key); if (d) { toMem(key, d); return d; }

  if (!shouldUseLLM(sampleKey || key)) { const res = fallback(); toMem(key, res); await toDisk(key, res); return res; }
  if (!CFG.OPENAI_API_KEY) { logger.warn('OPENAI_API_KEY missing, using fallback'); const res = fallback(); toMem(key, res); await toDisk(key, res); return res; }

  if (inFlight.has(key)) return inFlight.get(key)!;

      const p = (async () => {
    try {
      const resp = await callOpenAI(messages, opts);
      const raw = resp.choices?.[0]?.message?.content?.trim() || '';
      const parsed = parse(raw);
          if (parsed) { toMem(key, parsed); await toDisk(key, parsed); return parsed; }
          logger.debug(`${name}: invalid LLM response, using fallback`);
      const res = fallback(); toMem(key, res); await toDisk(key, res); return res;
    } catch (e) {
      logger.error(`${name}: LLM call failed, using fallback`, { error: String((e as Error).message) });
      const res = fallback(); toMem(key, res); await toDisk(key, res); return res;
    } finally { inFlight.delete(key); }
  })();

  inFlight.set(key, p);
  return p;
}

// -------------------------
// Business categorization
// -------------------------
const CATEGORY_LIST = [
  'Restaurant/Food Service', 'Retail/Store', 'Professional Services', 'Healthcare/Medical',
  'Construction/Contractor', 'Entertainment/Recreation', 'Automotive', 'Technology/IT', 'Education/Training', 'Other',
] as const;
export type BusinessCategory = typeof CATEGORY_LIST[number];

function normalizeCategory(s: string): BusinessCategory | string {
  const m: Record<string, BusinessCategory> = {
    restaurant: 'Restaurant/Food Service', 'food service': 'Restaurant/Food Service', food: 'Restaurant/Food Service', cafe: 'Restaurant/Food Service', bar: 'Restaurant/Food Service', catering: 'Restaurant/Food Service',
    retail: 'Retail/Store', store: 'Retail/Store', shop: 'Retail/Store', boutique: 'Retail/Store', clothing: 'Retail/Store', electronics: 'Retail/Store',
    professional: 'Professional Services', consulting: 'Professional Services', consultant: 'Professional Services', law: 'Professional Services', accounting: 'Professional Services', marketing: 'Professional Services', design: 'Professional Services', advisory: 'Professional Services',
    healthcare: 'Healthcare/Medical', medical: 'Healthcare/Medical', clinic: 'Healthcare/Medical', doctor: 'Healthcare/Medical', dentist: 'Healthcare/Medical', therapy: 'Healthcare/Medical',
    construction: 'Construction/Contractor', contractor: 'Construction/Contractor', builder: 'Construction/Contractor', renovation: 'Construction/Contractor', roofing: 'Construction/Contractor', plumbing: 'Construction/Contractor',
    entertainment: 'Entertainment/Recreation', recreation: 'Entertainment/Recreation', fitness: 'Entertainment/Recreation', gym: 'Entertainment/Recreation', spa: 'Entertainment/Recreation', salon: 'Entertainment/Recreation',
    automotive: 'Automotive', auto: 'Automotive', car: 'Automotive', vehicle: 'Automotive', mechanic: 'Automotive', dealership: 'Automotive',
    technology: 'Technology/IT', it: 'Technology/IT', software: 'Technology/IT', computer: 'Technology/IT', web: 'Technology/IT', digital: 'Technology/IT',
    education: 'Education/Training', training: 'Education/Training', school: 'Education/Training', learning: 'Education/Training', academy: 'Education/Training', university: 'Education/Training',
  };
  const k = s?.trim().toLowerCase();
  if (!k) return s;
  if (m[k]) return m[k];
  for (const [kk, vv] of Object.entries(m)) if (k.includes(kk)) return vv;
  return s;
}

function catFallback(desc: string): BusinessCategory {
  const L = desc.toLowerCase();
  const has = (...xs: string[]) => xs.some(x => L.includes(x));
  if (has('restaurant', 'food', 'kitchen', 'cafe', 'bar', 'catering')) return 'Restaurant/Food Service';
  if (has('store', 'retail', 'shop', 'boutique', 'clothing', 'electronics')) return 'Retail/Store';
  if (has('consult', 'law', 'account', 'marketing', 'design', 'advis')) return 'Professional Services';
  if (has('health', 'medical', 'clinic', 'doctor', 'dentist', 'therapy')) return 'Healthcare/Medical';
  if (has('construction', 'contractor', 'build', 'renovat', 'roof', 'plumb')) return 'Construction/Contractor';
  if (has('entertain', 'recreat', 'fitness', 'gym', 'spa', 'salon')) return 'Entertainment/Recreation';
  if (has('automotive', 'auto', 'car', 'vehicle', 'mechanic', 'dealership')) return 'Automotive';
  if (has('tech', 'software', ' it ', 'computer', 'web', 'digital')) return 'Technology/IT';
  if (has('education', 'school', 'training', 'learn', 'academy', 'university')) return 'Education/Training';
  return 'Other';
}

export async function categorizeBusinessType(description: string, businessName?: string): Promise<{ category: BusinessCategory | string; source: 'llm' | 'fallback' }> {
  return llmTask<{ category: BusinessCategory | string; source: 'llm' | 'fallback' }>({
    name: 'categorizeBusinessType',
    keyParts: [description, businessName],
    sampleKey: hashKey(description),
    messages: [
      { role: 'system', content: 'You are an expert business analyst. Return JSON.' },
      { role: 'user', content: `Return JSON only. key is category. value must be one of: ${CATEGORY_LIST.join(', ')}. Business Name: ${businessName ?? ''}. Description: ${description}` },
    ],
    parse: (raw) => {
      // Try strict JSON first
      const j = parseFirstJson(raw);
      let cat: unknown;
      if (j && typeof j === 'object') {
        // Accept several common keys
        cat = (j as any).category ?? (j as any).Category ?? (j as any).businessType ?? (j as any).type;
      }
      if (typeof cat !== 'string') {
        // Fallback to plain-text content
        const s = String(raw || '').trim().replace(/^"|"$/g, '');
        cat = s;
      }
      let normalized = normalizeCategory(String(cat));
      const isMember = (CATEGORY_LIST as readonly string[]).includes(String(normalized));
      // If not mapped to a known category, coerce to Other to avoid parse failures
      if (!isMember) normalized = 'Other';
      return { category: normalized as BusinessCategory | string, source: 'llm' as const };
    },
    fallback: () => ({ category: catFallback(description), source: 'fallback' }),
    opts: { temperature: 0.1, max_tokens: 40, force_json: true },
  });
}

// -------------------------
// Address parsing
// -------------------------
function processAddressFallback(address: string) {
  const normalized = address.trim().replace(/\s+/g, ' ')
    .replace(/\bSTREET\b/g, 'ST').replace(/\bAVENUE\b/g, 'AVE').replace(/\bBOULEVARD\b/g, 'BLVD')
    .replace(/\bROAD\b/g, 'RD').replace(/\bDRIVE\b/g, 'DR').replace(/\bLANE\b/g, 'LN').replace(/\bCOURT\b/g, 'CT')
    .replace(/\bPLACE\b/g, 'PL').replace(/\bCIRCLE\b/g, 'CIR').replace(/\bPARKWAY\b/g, 'PKWY')
    .replace(/\bNORTH\b/g, 'N').replace(/\bSOUTH\b/g, 'S').replace(/\bEAST\b/g, 'E').replace(/\bWEST\b/g, 'W')
    .replace(/\bNORTHEAST\b/g, 'NE').replace(/\bNORTHWEST\b/g, 'NW').replace(/\bSOUTHEAST\b/g, 'SE').replace(/\bSOUTHWEST\b/g, 'SW');

  const components: Record<string, string> = {};
  const numM = normalized.match(/^(\d+[A-Z]?)/); if (numM?.[1]) components.street_number = numM[1];
  const unitM = normalized.match(/\s+(UNIT|APT|SUITE|STE|#)\s*([A-Z0-9-]+)/i); if (unitM?.[2]) components.unit = unitM[2];
  const csz = /,\s*([A-Za-z .'-]+),?\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)\b/; const m = normalized.match(csz);
  if (m) { if (m[1]) components.city = m[1]; if (m[2]) components.state = m[2]; if (m[3]) components.zip = m[3]; }
  return { normalized, components };
}

export async function processAddressWithLLM(address: string): Promise<{ normalized: string; components: Record<string, string>; source: 'llm' | 'fallback' }> {
  return llmTask<{ normalized: string; components: Record<string, string>; source: 'llm' | 'fallback' }>({
    name: 'processAddressWithLLM',
    keyParts: [address],
    sampleKey: hashKey(address),
    messages: [
      { role: 'system', content: 'You are an expert US address parser. Return JSON only.' },
      { role: 'user', content: `Return JSON only. keys normalized and components {street_number, street_name, unit, city, state, zip}. Address: ${address}` },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      if (j && typeof j === 'object') {
        // Collect components if present
        const components: Record<string, string> = {};
        if (j.components && typeof j.components === 'object') {
          for (const [k, v] of Object.entries(j.components as Record<string, unknown>)) {
            if (typeof v === 'string' && v.trim()) components[k] = v.trim();
          }
        }

        // Derive normalized string
        let normalized = '';
        if (typeof j.normalized === 'string' && j.normalized.trim()) {
          normalized = j.normalized.trim();
        } else if (typeof j.address === 'string' && j.address.trim()) {
          normalized = j.address.trim();
        } else if (typeof j.line1 === 'string') {
          const line2 = typeof j.line2 === 'string' ? ` ${j.line2}` : '';
          normalized = `${j.line1}${line2}`.trim();
        } else if (Object.keys(components).length > 0) {
          const parts = [
            [components.street_number, components.street_name].filter(Boolean).join(' '),
            components.unit ? `Unit ${components.unit}` : '',
            components.city ? `${components.city},` : '',
            components.state,
            components.zip,
          ].filter(Boolean);
          normalized = parts.join(' ').replace(/\s+,/g, ',').replace(/\s{2,}/g, ' ').trim();
        }

        // If nothing parsable, fall back to the original input to avoid hard failure
        if (!normalized) normalized = String(address).trim();

        // If we have either a normalized string or any component, treat as valid parse
        if (normalized || Object.keys(components).length > 0) {
          return { normalized, components, source: 'llm' as const };
        }
      }

      // As a last resort, accept a plain-text address-looking response
      const text = String(raw || '').trim();
      if (/,\s*[A-Za-z .'-]+,?\s*[A-Z]{2}\s*\d{5}/.test(text)) {
        return { normalized: text, components: {}, source: 'llm' as const };
      }
      return null;
    },
    fallback: () => ({ ...processAddressFallback(address), source: 'fallback' }),
    opts: { temperature: 0.1, max_tokens: 220, force_json: true },
  });
}

// -------------------------
// Description analysis
// -------------------------
function analyzeDescriptionFallback(description: string) {
  const businessType = catFallback(description);
  const L = description.toLowerCase(); const feats: string[] = [];
  if (/(restaurant|food)/.test(L)) feats.push('Food Service');
  if (/delivery/.test(L)) feats.push('Delivery Service');
  if (/online/.test(L)) feats.push('Online Presence');
  if (/(24\/7|24 hours)/.test(L)) feats.push('24/7 Operation');
  if (/family/.test(L)) feats.push('Family-Friendly');
  return { businessType, keyFeatures: feats.slice(0, 3), confidence: 70 };
}

export async function analyzeDescription(description: string, businessName?: string): Promise<{ businessType: string; keyFeatures: string[]; confidence: number; source: 'llm' | 'fallback' }> {
  return llmTask<{ businessType: string; keyFeatures: string[]; confidence: number; source: 'llm' | 'fallback' }>({
    name: 'analyzeDescription',
    keyParts: [description, businessName],
    sampleKey: hashKey(description, businessName ?? ''),
    messages: [
      { role: 'system', content: 'You are an expert business analyst. Return JSON only.' },
      { role: 'user', content: `Return JSON only with fields businessType, keyFeatures, confidence. Business Name: ${businessName ?? ''}. Description: ${description}` },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      let businessType: string | undefined;
      let keyFeatures: string[] | undefined;
      let confidence: number | undefined;
      if (j && typeof j === 'object') {
        businessType = j.businessType || j.category || j.type;
        const features = j.keyFeatures || j.features || j.tags || j.attributes || j.traits;
        if (Array.isArray(features)) keyFeatures = features.filter((x: any) => typeof x === 'string');
        if (typeof j.confidence === 'number') confidence = j.confidence;
        if (typeof j.confidence === 'string') {
          const n = Number(j.confidence.replace(/[^0-9.]+/g, ''));
          if (Number.isFinite(n)) confidence = n;
        }
      }
      if (!businessType) {
        // Try to extract from plain text
        const m = String(raw || '').match(/businessType\s*[:=]\s*([^\n\r]+)/i);
        if (m?.[1]) businessType = m[1].trim().replace(/^"|"$/g, '');
      }
      if (!keyFeatures) {
        const m = String(raw || '').match(/keyFeatures\s*[:=]\s*\[([^\]]+)\]/i);
        if (m?.[1]) keyFeatures = m[1].split(',').map(s => s.replace(/^[\s\"]+|[\s\"]+$/g, '')).filter(Boolean);
      }
      if (!Number.isFinite(confidence as number)) {
        const m = String(raw || '').match(/confidence\s*[:=]\s*([0-9]{1,3})/i);
        if (m?.[1]) confidence = Number(m[1]);
      }
      if (!businessType) return null;
      const bt = String(businessType);
      const feats = (keyFeatures || []).slice(0, 3);
      const conf = clamp(Number(confidence ?? 70), 0, 100);
      return { businessType: bt, keyFeatures: feats, confidence: conf, source: 'llm' as const };
    },
    fallback: () => ({ ...analyzeDescriptionFallback(description), source: 'fallback' }),
    opts: { temperature: 0.2, max_tokens: 320, force_json: true },
  });
}

// -------------------------
// Additional caching for business analysis
// -------------------------
const businessCache = new Map<string, { businessType: string; confidence: number }>();

export function getBusinessAnalysisCache(description: string, businessName?: string): { businessType: string; confidence: number } | null {
  const cacheKey = `${description}:${businessName || ''}`;
  return businessCache.get(cacheKey) || null;
}

export function setBusinessAnalysisCache(description: string, businessName: string | undefined, result: { businessType: string; confidence: number }): void {
  const cacheKey = `${description}:${businessName || ''}`;
  businessCache.set(cacheKey, result);
  
  // Limit cache size
  if (businessCache.size > 1000) {
    const firstKey = businessCache.keys().next().value;
    if (firstKey) businessCache.delete(firstKey);
  }
}

// -------------------------
// Init
// -------------------------
export function initializeLLM() {
  if (!CFG.OPENAI_API_KEY) logger.warn('OPENAI_API_KEY not set. Falling back to heuristics.');
  else logger.info('LLM utilities ready');
}

// removed duplicate export to avoid conflicts with interface exports above
