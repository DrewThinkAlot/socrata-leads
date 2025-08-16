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
  // Enhanced LLM Features
  LLM_ENHANCED_FILTERING: (process.env.LLM_ENHANCED_FILTERING ?? 'false').toLowerCase() === 'true',
  LLM_DUPLICATE_DETECTION: (process.env.LLM_DUPLICATE_DETECTION ?? 'false').toLowerCase() === 'true',
  LLM_DYNAMIC_SCORING: (process.env.LLM_DYNAMIC_SCORING ?? 'false').toLowerCase() === 'true',
  LLM_CONTACT_EXTRACTION: (process.env.LLM_CONTACT_EXTRACTION ?? 'false').toLowerCase() === 'true',
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
function sampleAccept(sampleKey: string): boolean {
  // Deterministic sampling based on hashed key so repeated runs are stable
  try {
    const h = Number(hashKey(sampleKey)) % 1000; // 0..999
    const threshold = Math.round(CFG.LLM_SAMPLE_RATE * 1000); // 0..1000
    return h < threshold;
  } catch {
    // Fallback to random if hashing fails
    return Math.random() < CFG.LLM_SAMPLE_RATE;
  }
}

function shouldUseLLM(sampleKey: string) {
  if (!CFG.LLM_ENABLED) return false;
  if (!CFG.OPENAI_API_KEY) return false;
  if (CFG.LLM_MAX_CALLS_PER_RUN > 0 && callsThisRun >= CFG.LLM_MAX_CALLS_PER_RUN) return false;
  if (CFG.LLM_SAMPLE_RATE < 1) return sampleAccept(sampleKey);
  return true;
}

export interface OpenAIRequestOptions {
  model?: string;
  temperature?: number;
  max_tokens?: number;
  timeout?: number;
  force_json?: boolean;
}
export interface OpenAIResponse {
  choices: Array<{ message: { role: string; content: string } }>;
  usage?: any;
  id?: string;
  model?: string;
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
// Project stage classification
// -------------------------
const PROJECT_STAGES = [
  'Planning',
  'Pre-Opening', 
  'Soft Opening',
  'Grand Opening',
  'Operational'
] as const;
export type ProjectStage = typeof PROJECT_STAGES[number];

function stageFallback(description: string, permitTypes: string[]): ProjectStage {
  const desc = description.toLowerCase();
  const permits = permitTypes.join(' ').toLowerCase();
  
  // Restaurant-specific early stage indicators
  if (desc.includes('plan') || desc.includes('proposal') || desc.includes('concept') || 
      permits.includes('plan') || permits.includes('zoning') || permits.includes('variance') ||
      permits.includes('conditional use') || permits.includes('master use')) {
    return 'Planning';
  }
  
  // Construction/renovation phase with restaurant-specific signals
  if (desc.includes('build') || desc.includes('construct') || desc.includes('renov') ||
      permits.includes('build') || permits.includes('construct') || permits.includes('alter') ||
      permits.includes('tenant improvement') || permits.includes('mechanical') || 
      permits.includes('electrical') || permits.includes('plumbing') ||
      desc.includes('kitchen') || desc.includes('hood') || desc.includes('ventilation')) {
    return 'Pre-Opening';
  }
  
  // Near opening indicators with restaurant-specific equipment and utility signals
  if (desc.includes('soft') || desc.includes('trial') || desc.includes('preview') ||
      permits.includes('health') || permits.includes('food') ||
      desc.includes('equipment') || desc.includes('installation') || desc.includes('final') ||
      desc.includes('inspection') || desc.includes('training') || desc.includes('staff') ||
      desc.includes('water service') || desc.includes('gas connection') || desc.includes('utility hookup') ||
      permits.includes('water service') || permits.includes('electrical')) {
    return 'Soft Opening';
  }
  
  // Grand opening indicators
  if (desc.includes('grand') || desc.includes('launch') || desc.includes('open') ||
      desc.includes('ready to open')) {
    return 'Grand Opening';
  }
  
  return 'Operational';
}

export async function classifyProjectStage(
  description: string, 
  businessName?: string,
  permitTypes?: string[],
  issueDate?: string
): Promise<{ stage: ProjectStage; confidence: number; source: 'llm' | 'fallback' }> {
  const permitContext = permitTypes?.length ? `Permit Types: ${permitTypes.join(', ')}` : '';
  const dateContext = issueDate ? `Issue Date: ${issueDate}` : '';
  
  return llmTask<{ stage: ProjectStage; confidence: number; source: 'llm' | 'fallback' }>({
    name: 'classifyProjectStage',
    keyParts: [description, businessName, permitTypes?.join(','), issueDate],
    sampleKey: hashKey(description, businessName ?? ''),
    messages: [
      { role: 'system', content: 'You are an expert business analyst specializing in restaurant and retail development timelines. Return JSON only.' },
      { role: 'user', content: `Return JSON only with fields stage and confidence (0-100). Stage must be one of: ${PROJECT_STAGES.join(', ')}. 
Business Name: ${businessName ?? ''}
Description: ${description}
${permitContext}
${dateContext}

Classify the current project stage based on business description, permit types, and timeline context.` },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      let stage: string | undefined;
      let confidence: number | undefined;
      
      if (j && typeof j === 'object') {
        stage = j.stage || j.projectStage || j.phase || j.status;
        confidence = j.confidence || j.confidenceScore || j.score;
      }
      
      if (!stage) {
        const m = String(raw || '').match(/stage\s*[:=]\s*([^\n]+)/i);
        if (m?.[1]) stage = m[1].trim().replace(/^"|"$/g, '');
      }
      
      if (!stage) return null;
      
      const normalizedStage = PROJECT_STAGES.includes(stage as ProjectStage) 
        ? stage as ProjectStage 
        : stageFallback(description, permitTypes || []);
      
      const conf = clamp(Number(confidence ?? 75), 0, 100);
      return { stage: normalizedStage, confidence: conf, source: 'llm' as const };
    },
    fallback: () => ({ 
      stage: stageFallback(description, permitTypes || []), 
      confidence: 60, 
      source: 'fallback' 
    }),
    opts: { temperature: 0.15, max_tokens: 80, force_json: true },
  });
}

// -------------------------
// Days remaining estimation
// -------------------------
// Restaurant-specific stage duration estimates based on industry research
const RESTAURANT_STAGE_DAYS_REMAINING = {
  'fast-food': {
    Planning: { min: 60, max: 180, avg: 90 },
    'Pre-Opening': { min: 21, max: 84, avg: 56 },  // 8-12 weeks construction
    'Soft Opening': { min: 3, max: 14, avg: 7 },
    'Grand Opening': { min: 0, max: 5, avg: 2 },
    Operational: { min: 0, max: 0, avg: 0 }
  },
  'fast-casual': {
    Planning: { min: 90, max: 270, avg: 120 },
    'Pre-Opening': { min: 42, max: 112, avg: 77 },  // 10-16 weeks construction
    'Soft Opening': { min: 7, max: 21, avg: 14 },
    'Grand Opening': { min: 0, max: 7, avg: 3 },
    Operational: { min: 0, max: 0, avg: 0 }
  },
  'full-service': {
    Planning: { min: 120, max: 365, avg: 210 },
    'Pre-Opening': { min: 84, max: 168, avg: 140 }, // 20-24 weeks construction
    'Soft Opening': { min: 14, max: 42, avg: 21 },
    'Grand Opening': { min: 0, max: 14, avg: 7 },
    Operational: { min: 0, max: 0, avg: 0 }
  },
  'unknown': {
    Planning: { min: 90, max: 365, avg: 180 },
    'Pre-Opening': { min: 30, max: 120, avg: 60 },
    'Soft Opening': { min: 7, max: 30, avg: 14 },
    'Grand Opening': { min: 0, max: 7, avg: 3 },
    Operational: { min: 0, max: 0, avg: 0 }
  }
};

function estimateDaysRemainingFallback(stage: ProjectStage, permitTypes: string[], issueDate?: string, restaurantType: string = 'unknown'): number {
  // Determine restaurant type from permit types if not provided
  let detectedType = restaurantType;
  if (detectedType === 'unknown') {
    const permits = permitTypes.join(' ').toLowerCase();
    if (permits.includes('drive') || permits.includes('quick') || permits.includes('fast food')) {
      detectedType = 'fast-food';
    } else if (permits.includes('full bar') || permits.includes('liquor') || permits.includes('wine')) {
      detectedType = 'full-service';
    } else if (permits.includes('casual') || permits.includes('counter')) {
      detectedType = 'fast-casual';
    }
  }
  
  const stageData = RESTAURANT_STAGE_DAYS_REMAINING[detectedType as keyof typeof RESTAURANT_STAGE_DAYS_REMAINING] || 
                   RESTAURANT_STAGE_DAYS_REMAINING.unknown;
  const currentStageData = stageData[stage];
  
  // Adjust based on permit types and complexity
  let multiplier = 1.0;
  const permits = permitTypes.join(' ').toLowerCase();
  
  // Construction complexity adjustments
  if (permits.includes('build') || permits.includes('construct')) {
    multiplier = detectedType === 'full-service' ? 1.3 : 1.2;
  }
  if (permits.includes('tenant improvement') || permits.includes('renovation')) {
    multiplier *= 0.8; // Renovations are typically faster
  }
  
  // Late-stage signals reduce timeline
  if (permits.includes('health') || permits.includes('final')) {
    multiplier *= 0.6;
  }
  if (permits.includes('equipment') || permits.includes('installation')) {
    multiplier *= 0.4; // Very close to opening
  }
  if (permits.includes('water service') || permits.includes('utility') || permits.includes('gas') || permits.includes('electrical')) {
    multiplier *= 0.3; // Utilities being connected - imminent opening
  }
  
  // Early-stage signals extend timeline
  if (permits.includes('plan') || permits.includes('zoning') || permits.includes('variance')) {
    multiplier *= 1.8;
  }
  
  // Seasonal adjustment
  const currentMonth = new Date().getMonth() + 1;
  if (currentMonth >= 1 && currentMonth <= 3) multiplier *= 1.2; // Winter delays
  if (currentMonth >= 10 && currentMonth <= 12) multiplier *= 1.3; // Holiday delays
  
  // Adjust based on issue date recency
  if (issueDate) {
    try {
      const issue = new Date(issueDate);
      const daysSince = Math.floor((Date.now() - issue.getTime()) / (1000 * 60 * 60 * 24));
      
      // Reduce estimates for older permits (progress likely made)
      if (stage === 'Planning' && daysSince > 120) multiplier *= 0.7;
      if (stage === 'Pre-Opening' && daysSince > 60) multiplier *= 0.6;
      if (stage === 'Soft Opening' && daysSince > 21) multiplier *= 0.4;
    } catch {
      // Invalid date, use default
    }
  }
  
  return Math.round(currentStageData.avg * multiplier);
}

export async function estimateDaysRemaining(
  stage: ProjectStage,
  description: string,
  permitTypes?: string[],
  issueDate?: string,
  businessName?: string
): Promise<{ daysRemaining: number; confidence: number; source: 'llm' | 'fallback' }> {
  const permitContext = permitTypes?.length ? `Permit Types: ${permitTypes.join(', ')}` : '';
  const dateContext = issueDate ? `Issue Date: ${issueDate}` : '';
  
  // Detect restaurant type for enhanced context
  const combinedText = `${businessName || ''} ${description}`.toLowerCase();
  let restaurantType = 'unknown';
  if (combinedText.includes('fast food') || combinedText.includes('drive') || combinedText.includes('quick')) {
    restaurantType = 'fast-food';
  } else if (combinedText.includes('fine dining') || combinedText.includes('full service') || combinedText.includes('wine')) {
    restaurantType = 'full-service';
  } else if (combinedText.includes('casual') || combinedText.includes('counter') || combinedText.includes('fresh')) {
    restaurantType = 'fast-casual';
  }
  
  return llmTask<{ daysRemaining: number; confidence: number; source: 'llm' | 'fallback' }>({
    name: 'estimateDaysRemaining',
    keyParts: [stage, description, businessName, permitTypes?.join(','), issueDate, restaurantType],
    sampleKey: hashKey(stage, description),
    messages: [
      { role: 'system', content: 'You are an expert restaurant development timeline analyst with knowledge of industry-specific construction and permitting phases. Return JSON only.' },
      { role: 'user', content: `Return JSON only with fields daysRemaining and confidence (0-100).
Project Stage: ${stage}
Restaurant Type: ${restaurantType}
Business Name: ${businessName ?? ''}
Description: ${description}
${permitContext}
${dateContext}

Estimate days until grand opening based on restaurant-specific timelines:
- Fast-food: 8-12 weeks construction
- Fast-casual: 10-16 weeks construction  
- Full-service: 20-24 weeks construction

Consider permit sequence: Planning → Construction → Equipment → Final Inspections → Opening` },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      let daysRemaining: number | undefined;
      let confidence: number | undefined;
      
      if (j && typeof j === 'object') {
        if (typeof j.daysRemaining === 'number') daysRemaining = j.daysRemaining;
        if (typeof j.days_remaining === 'number') daysRemaining = j.days_remaining;
        if (typeof j.estimatedDays === 'number') daysRemaining = j.estimatedDays;
        
        if (typeof j.confidence === 'number') confidence = j.confidence;
        if (typeof j.confidence === 'string') {
          const n = Number(j.confidence.replace(/[^0-9.]+/g, ''));
          if (Number.isFinite(n)) confidence = n;
        }
      }
      
      if (daysRemaining === undefined) {
        const m = String(raw || '').match(/days(?:\s*remaining)?\s*[:=]\s*(\d+)/i);
        if (m?.[1]) daysRemaining = Number(m[1]);
      }
      
      if (daysRemaining === undefined) return null;
      
      const days = Math.max(0, Math.round(daysRemaining));
      const conf = clamp(Number(confidence ?? 75), 0, 100);
      return { daysRemaining: days, confidence: conf, source: 'llm' as const };
    },
    fallback: () => ({ 
      daysRemaining: estimateDaysRemainingFallback(stage, permitTypes || [], issueDate, restaurantType), 
      confidence: 70, 
      source: 'fallback' 
    }),
    opts: { temperature: 0.15, max_tokens: 80, force_json: true },
  });
}

// -------------------------
// Enhanced LLM Functions for Efficiency
// -------------------------

/**
 * Detect if a business is currently operational or still pre-opening
 */
export async function detectOperationalStatus(
  description: string,
  permitTypes: string[],
  businessName?: string,
  issueDate?: string
): Promise<{ isOperational: boolean; confidence: number; source: 'llm' | 'fallback' }> {
  const permitContext = permitTypes.length ? `Permit Types: ${permitTypes.join(', ')}` : '';
  const dateContext = issueDate ? `Issue Date: ${issueDate}` : '';
  
  return llmTask<{ isOperational: boolean; confidence: number; source: 'llm' | 'fallback' }>({
    name: 'detectOperationalStatus',
    keyParts: [description, permitTypes.join(','), businessName, issueDate],
    sampleKey: hashKey(description, businessName ?? ''),
    messages: [
      { 
        role: 'system', 
        content: 'You are a business analyst expert at detecting whether a restaurant is currently operational or still in pre-opening phase based on permit descriptions, business context, and timeline indicators. Return JSON only.' 
      },
      { 
        role: 'user', 
        content: `Analyze if this business is currently operational or still pre-opening. Return JSON: {"isOperational": boolean, "confidence": 0-100}.

Business Name: ${businessName ?? ''}
Description: ${description}
${permitContext}
${dateContext}

Consider:
- Language patterns (renewal, transfer, re-inspection = operational)
- New construction, buildout, grand opening = pre-opening
- Permit sequence and timing
- Business name patterns (established vs new)` 
      },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      if (j && typeof j === 'object') {
        const isOperational = Boolean(j.isOperational ?? j.operational ?? j.is_operational);
        const confidence = clamp(Number(j.confidence ?? 75), 0, 100);
        return { isOperational, confidence, source: 'llm' as const };
      }
      return null;
    },
    fallback: () => {
      // Fallback logic based on patterns
      const desc = description.toLowerCase();
      const permits = permitTypes.join(' ').toLowerCase();
      
      const operationalPatterns = ['renewal', 'transfer', 'change of ownership', 're-inspection', 'maintenance', 'repair', 'annual', 'routine', 'existing', 'current', 'established'];
      const preOpeningPatterns = ['grand opening', 'opening soon', 'new location', 'build-out', 'tenant improvement', 'new restaurant', 'coming soon', 'under construction', 'now hiring'];
      
      const operationalScore = operationalPatterns.filter(p => desc.includes(p) || permits.includes(p)).length;
      const preOpeningScore = preOpeningPatterns.filter(p => desc.includes(p) || permits.includes(p)).length;
      
      return { 
        isOperational: operationalScore > preOpeningScore, 
        confidence: Math.min(80, Math.max(40, (Math.abs(operationalScore - preOpeningScore) + 1) * 20)), 
        source: 'fallback' as const 
      };
    },
    opts: { temperature: 0.1, max_tokens: 60, force_json: true },
  });
}

/**
 * Resolve if two business records represent the same entity
 */
export async function resolveBusinessEntity(
  address1: string,
  businessName1: string,
  address2: string,
  businessName2: string
): Promise<{ isSameBusiness: boolean; confidence: number; source: 'llm' | 'fallback' }> {
  return llmTask<{ isSameBusiness: boolean; confidence: number; source: 'llm' | 'fallback' }>({
    name: 'resolveBusinessEntity',
    keyParts: [address1, businessName1, address2, businessName2],
    sampleKey: hashKey(address1, businessName1),
    messages: [
      { 
        role: 'system', 
        content: 'You are an expert at determining if two business records refer to the same physical restaurant location, accounting for address variations, business name changes, and franchise relationships. Return JSON only.' 
      },
      { 
        role: 'user', 
        content: `Compare these two business records and determine if they represent the same restaurant location. Return JSON: {"isSameBusiness": boolean, "confidence": 0-100}.

Record 1:
Address: ${address1}
Business Name: ${businessName1}

Record 2:
Address: ${address2}
Business Name: ${businessName2}

Consider:
- Address variations (abbreviations, suite numbers, formatting)
- Business name changes, DBA relationships
- Franchise vs corporate naming
- Same physical location indicators` 
      },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      if (j && typeof j === 'object') {
        const isSameBusiness = Boolean(j.isSameBusiness ?? j.same_business ?? j.is_same);
        const confidence = clamp(Number(j.confidence ?? 50), 0, 100);
        return { isSameBusiness, confidence, source: 'llm' as const };
      }
      return null;
    },
    fallback: () => {
      // Simple fallback: exact address match or very similar names
      const addr1Clean = address1.toLowerCase().replace(/[^a-z0-9]/g, '');
      const addr2Clean = address2.toLowerCase().replace(/[^a-z0-9]/g, '');
      const name1Clean = businessName1.toLowerCase().replace(/[^a-z0-9]/g, '');
      const name2Clean = businessName2.toLowerCase().replace(/[^a-z0-9]/g, '');
      
      const addressMatch = addr1Clean === addr2Clean;
      const nameMatch = name1Clean === name2Clean;
      const nameSimilar = name1Clean.includes(name2Clean) || name2Clean.includes(name1Clean);
      
      if (addressMatch && (nameMatch || nameSimilar)) {
        return { isSameBusiness: true, confidence: 90, source: 'fallback' as const };
      }
      if (addressMatch) {
        return { isSameBusiness: true, confidence: 70, source: 'fallback' as const };
      }
      return { isSameBusiness: false, confidence: 60, source: 'fallback' as const };
    },
    opts: { temperature: 0.1, max_tokens: 80, force_json: true },
  });
}

/**
 * Extract contact information from unstructured text
 */
export async function extractContactInfoLLM(
  description: string,
  businessName?: string
): Promise<{
  phone?: string;
  email?: string;
  website?: string;
  contactPerson?: string;
  source: 'llm' | 'fallback';
}> {
  return llmTask<{
    phone?: string;
    email?: string;
    website?: string;
    contactPerson?: string;
    source: 'llm' | 'fallback';
  }>({
    name: 'extractContactInfoLLM',
    keyParts: [description, businessName],
    sampleKey: hashKey(description),
    messages: [
      { 
        role: 'system', 
        content: 'You are an expert at extracting contact information from business descriptions and permit text. Return JSON only with properly formatted contact details.' 
      },
      { 
        role: 'user', 
        content: `Extract all contact information from this text. Return JSON: {"phone": "string", "email": "string", "website": "string", "contactPerson": "string"}. Include only valid, properly formatted contact details. Omit fields if not found.

Business Name: ${businessName ?? ''}
Description: ${description}

Format phone numbers as (XXX) XXX-XXXX. Validate email addresses. Extract full names for contact persons.` 
      },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      if (j && typeof j === 'object') {
        const result: any = { source: 'llm' as const };
        
        if (typeof j.phone === 'string' && j.phone.trim()) result.phone = j.phone.trim();
        if (typeof j.email === 'string' && j.email.trim() && j.email.includes('@')) result.email = j.email.trim();
        if (typeof j.website === 'string' && j.website.trim()) result.website = j.website.trim();
        if (typeof j.contactPerson === 'string' && j.contactPerson.trim()) result.contactPerson = j.contactPerson.trim();
        
        return result;
      }
      return null;
    },
    fallback: () => {
      // Basic regex extraction
      const result: any = { source: 'fallback' as const };
      const text = `${businessName ?? ''} ${description}`;
      
      const phoneMatch = text.match(/\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
      if (phoneMatch) result.phone = phoneMatch[0];
      
      const emailMatch = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
      if (emailMatch) result.email = emailMatch[0];
      
      const websiteMatch = text.match(/https?:\/\/[^\s]+|www\.[^\s]+/);
      if (websiteMatch) result.website = websiteMatch[0];
      
      return result;
    },
    opts: { temperature: 0.1, max_tokens: 150, force_json: true },
  });
}

/**
 * Calculate dynamic lead score based on business context
 */
export async function calculateDynamicLeadScore(
  events: any[],
  staticScore: number
): Promise<{ 
  score: number; 
  factors: Record<string, number>; 
  adjustments: string[];
  source: 'llm' | 'fallback';
}> {
  const combinedContext = events.map(e => 
    `${e.description || ''} ${e.name || ''} ${e.type || ''}`
  ).join(' ').substring(0, 1000); // Limit context length
  
  return llmTask<{ 
    score: number; 
    factors: Record<string, number>; 
    adjustments: string[];
    source: 'llm' | 'fallback';
  }>({
    name: 'calculateDynamicLeadScore',
    keyParts: [combinedContext, staticScore],
    sampleKey: hashKey(combinedContext.substring(0, 200)),
    messages: [
      { 
        role: 'system', 
        content: 'You are an expert lead qualification analyst for restaurant sales. Analyze lead quality and provide scoring adjustments based on business context. Return JSON only.' 
      },
      { 
        role: 'user', 
        content: `Analyze this restaurant lead and provide quality scoring. Return JSON: {"score": 0-100, "factors": {"recency": weight, "intent": weight, "contact": weight, "timeline": weight, "complexity": weight}, "adjustments": ["reason1", "reason2"]}.

Current Static Score: ${staticScore}
Business Context: ${combinedContext}

Consider:
- Business type and complexity (full-service > fast-casual > fast-food)
- Opening timeline and stage indicators
- Contact information availability
- Market opportunity signals
- Permit complexity and sequence` 
      },
    ],
    parse: (raw) => {
      const j = parseFirstJson(raw) as any;
      if (j && typeof j === 'object') {
        const score = clamp(Number(j.score ?? staticScore), 0, 100);
        const factors = (j.factors && typeof j.factors === 'object') ? j.factors : {};
        const adjustments = Array.isArray(j.adjustments) ? j.adjustments.filter((a: any) => typeof a === 'string') : [];
        
        return { score, factors, adjustments, source: 'llm' as const };
      }
      return null;
    },
    fallback: () => ({
      score: staticScore,
      factors: { recency: 0.3, intent: 0.25, contact: 0.2, timeline: 0.15, complexity: 0.1 },
      adjustments: ['Using static scoring fallback'],
      source: 'fallback' as const
    }),
    opts: { temperature: 0.2, max_tokens: 200, force_json: true },
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
