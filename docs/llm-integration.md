# LLM Integration Documentation

## Overview

This document describes the integration of Large Language Models (LLMs) into the socrata-leads pipeline. The integration enhances the pipeline's capabilities in business categorization, description analysis, and lead scoring.

## Features

### 1. Enhanced Business Categorization
- Uses LLMs to classify business types from permit/license descriptions
- Provides more accurate categorization than keyword-based approaches
- Supports custom business categories and emerging market trends

### 2. Intelligent Description Analysis
- Extracts key business features and services from descriptions
- Provides confidence scores for business type classifications
- Identifies high-value business opportunities

### 3. LLM-Enhanced Lead Scoring
- Adds LLM-based business potential scoring to lead evaluation
- Provides additional insights for lead prioritization
- Enhances traditional scoring factors with AI-driven analysis

## Implementation Details

### LLM Utility Module
The core LLM functionality is implemented in `src/util/llm.ts` and includes:

1. **Business Categorization** - Classifies businesses into predefined categories
2. **Description Analysis** - Extracts key features and provides confidence scores
3. **Address Processing** - Normalizes and parses addresses with enhanced accuracy

### Integration Points

1. **Normalization Process** (`src/normalize/run.ts`)
   - Enhanced business categorization during data normalization
   - Additional business insights added to normalized records

2. **Fusion Process** (`src/fuse/run.ts`)
   - LLM analysis of business evidence for enhanced event creation

3. **Scoring Process** (`src/score/run.ts`)
   - LLM-based business potential scoring (0-20 additional points)
   - Bonus scoring for high-value business categories

4. **Export Process** (`src/export/run.ts`)
   - LLM-enhanced business insights in export summaries

## Configuration

To enable LLM features, add your OpenAI API key and optional model to the `.env` file:

```bash
OPENAI_API_KEY=sk-proj-...
# Optional override (defaults to gpt-5-mini)
OPENAI_MODEL=gpt-5-mini
# Optional timeout in ms
OPENAI_TIMEOUT_MS=30000
```

## Usage

### Testing LLM Integration
Run the LLM integration tests:

```bash
npm run test-llm
```

### Running the Pipeline
The LLM features are automatically used when the pipeline runs:

```bash
npm run daily
```

Or for individual cities:

```bash
npm run extract -- --city chicago
npm run normalize -- --city chicago
npm run fuse -- --city chicago
npm run score -- --city chicago
npm run export -- --city chicago --limit 12 --out out/chicago-drop.csv
```

## Benefits

1. **Improved Accuracy** - Better business classification and lead scoring
2. **Enhanced Insights** - Deeper understanding of business opportunities
3. **Reduced Manual Configuration** - Less need for manual rule creation
4. **Scalability** - Handles diverse data sources with consistent quality

## Fallback Mechanisms

When LLM features are not configured or fail:
- The pipeline falls back to traditional rule-based approaches
- All core functionality continues to work without LLM integration
- Detailed logging helps identify when fallbacks occur

## Performance Considerations

- LLM calls are cached to reduce costs and improve performance
- Timeout handling prevents pipeline delays from LLM service issues
- Asynchronous processing maintains pipeline throughput

## Cost Controls

Environment variables:

```bash
# Enable or disable all LLM usage
LLM_ENABLED=true

## Note Sampling and per-run call caps are not used. The LLM is invoked whenever an API key is present.

# Override retries for transient errors
LLM_MAX_RETRIES=2

# In-memory and disk cache TTL in ms
LLM_CACHE_TTL_MS=300000

# Directory for persistent LLM cache shared across pipeline stages
LLM_DISK_CACHE_DIR=.cache/llm

# Optional OpenAI overrides
OPENAI_MODEL=gpt-5-mini
OPENAI_API_URL=https://api.openai.com/v1/chat/completions
OPENAI_TIMEOUT_MS=30000

# Token budget (0 means unlimited)
LLM_MAX_TOKENS_PER_RUN=0
```

Behavior:

- Results are cached in memory and on disk using deterministic keys
- Concurrency is de-duplicated so identical in-flight requests share one call
- Sampling and per-run budget gate requests and fall back to rules when gated
- Disk cache enables reuse across separate CLI stages (normalize, fuse, score, export)
- Optional token budget caps approximate total tokens per run using API usage metadata