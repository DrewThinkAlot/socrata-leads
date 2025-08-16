# Socrata Leads Pipeline

A city-agnostic pipeline that ingests datasets from Socrata portals, normalizes them to a canonical schema, fuses signals (permits + licenses), scores openings, and exports 12-lead "drops" per market.

## Features

- **City-agnostic design**: Add new cities with just configuration files
- **Incremental processing**: Efficient watermark-based data extraction
- **Signal fusion**: Combine permits and licenses to identify business openings
- **Lead scoring**: Weighted algorithm to rank business opportunities
- **LLM-enhanced processing**: AI-powered business categorization and analysis
- **Robust error handling**: Exponential backoff for API rate limits
- **Pluggable storage**: SQLite (default) with PostgreSQL support
- **Type-safe configuration**: Zod validation for city configs

## Quick Start

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Set up environment**:
   ```bash
   cp .env.example .env
   # Edit .env and add your Socrata app tokens
   ```

3. **Initialize database**:
   ```bash
   npm run migrate
   ```

4. **Run backfill for a city**:
   ```bash
   npm run backfill -- --city chicago --days 120
   ```

5. **Run daily pipeline**:
   ```bash
   npm run daily
   ```

6. **Export leads**:
   ```bash
   npm run export -- --city chicago --limit 12 --out out/chicago-drop.csv
   ```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Socrata APIs  │    │   City Configs   │    │   Storage Layer │
│                 │    │                  │    │                 │
│ • Chicago       │    │ • chicago.yaml   │    │ • SQLite (def.) │
│ • Seattle       │    │ • seattle.yaml   │    │ • PostgreSQL    │
│ • (Miami/ArcGIS)│    │ • Zod validation │    │ • Migrations    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                  │
                    ┌─────────────────────────┐
                    │    Processing Pipeline   │
                    │                         │
                    │ 1. Extract → Raw        │
                    │ 2. Normalize → Schema   │
                    │ 3. Fuse → Events        │
                    │ 4. Score → Leads        │
                    │ 5. Export → CSV         │
                    └─────────────────────────┘
```

## LLM Integration

The pipeline now includes optional LLM-enhanced features for improved business intelligence:

### Features
- **Enhanced Business Categorization**: AI-powered classification of business types
- **Intelligent Description Analysis**: Extract key business features from permit/license descriptions
- **LLM-Enhanced Lead Scoring**: Additional scoring factors based on business potential
- **Smart Address Processing**: Improved address normalization and parsing

### Configuration
To enable LLM features, add your OpenAI API key to `.env`:

```bash
OPENAI_API_KEY=sk-proj-...
```

### Benefits
- Improved accuracy in business classification and lead scoring
- Enhanced insights for business development teams
- Reduced manual configuration and rule creation
- Better handling of diverse data sources

See [docs/llm-integration.md](docs/llm-integration.md) for detailed documentation.

## Commands

### Data Pipeline

- `npm run extract -- --city <city> [--dataset <name>] [--since <date>]`
- `npm run normalize -- --city <city>`
- `npm run fuse -- --city <city>`
- `npm run score -- --city <city>`
- `npm run export -- --city <city> --limit 12 --out <file>`

### Orchestration

- `npm run daily` - Run full pipeline for all cities
- `npm run backfill -- --city <city> --days <n>` - Backfill historical data

### Development

- `npm run dev` - Start development mode
- `npm run test` - Run tests
- `npm run lint` - Lint code
- `npm run typecheck` - Type checking

## Configuration

### Environment Variables

```bash
# Socrata API tokens (optional, for higher rate limits)
CHICAGO_APP_TOKEN=your_token_here
SEATTLE_APP_TOKEN=your_token_here

# Database (SQLite default)
DATABASE_URL=sqlite://./data/pipeline.db
# DATABASE_URL=postgres://user:pass@host/db

# Logging
LOG_LEVEL=info
```

### City Configuration

Create `configs/{city}.yaml` files with dataset mappings:

```yaml
city: chicago
base_url: https://data.cityofchicago.org
app_token: ${CHICAGO_APP_TOKEN}
datasets:
  building_permits:
    id: ydr8-5enu
    select: [permit_, permit_type, application_start_date, ...]
    where: "upper(status) IN ('ISSUED','RELEASED')"
    order_by: application_start_date ASC
    watermark_field: application_start_date
    map:
      business_name: permit_type
      address: "CONCAT(street_number,' ',street_name)"
      # ... canonical field mappings
```

## Signal Fusion Rules

The pipeline identifies business opening signals by combining:

1. **Rule A**: Building permit + liquor license within 120 days, same address → 80 points
2. **Rule B**: Large commercial permit alone within 60 days → 60 points  
3. **Rule C**: License status AAI/ACT + future start date → 70 points

## Lead Scoring

Leads are scored (0-100) based on:

- **Recency** (0-30): How recent the signals are
- **Permit/License Type** (0-30): Weight based on business type
- **Contact Information** (0-20): Presence of phone/email
- **Multi-signal Bonus** (0-20): Multiple signals for same location

## Database Schema

- `raw` - Original API responses with watermarks
- `normalized` - Canonical schema across all cities
- `events` - Fused signals with strength scores
- `leads` - Final scored business opportunities
- `checkpoints` - Incremental processing state

## Adding New Cities

1. Create `configs/{city}.yaml` with dataset mappings
2. Ensure required canonical fields are mapped:
   - `business_name`, `address`, `event_date`, `type`
3. Run validation: `npm run test`
4. Test extraction: `npm run extract -- --city {city} --limit 10`

## Adding ArcGIS Support (Miami)

The pipeline includes a stub for ArcGIS integration:

```typescript
// src/adapters/arcgis_stub.ts
export class ArcGISAdapter {
  // TODO: Implement for Miami and other ArcGIS cities
}
```

## Development

### Project Structure

```
src/
├── index.ts              # Main entry point
├── types.ts              # Shared type definitions
├── util/                 # Utilities (logger, backoff, etc.)
├── soda/                 # Socrata API client
├── config/               # Configuration loading & validation
├── adapters/             # Data source adapters
├── storage/              # Database abstraction
├── extract/              # Data extraction pipeline
├── normalize/            # Data normalization
├── fuse/                 # Signal fusion rules
├── score/                # Lead scoring algorithm
├── export/               # CSV export
└── orchestrate/          # Daily/backfill runners
```

### Testing

```bash
npm run test              # Run all tests
npm run test:watch        # Watch mode
npm run test:coverage     # Coverage report
```

### Code Style

- **Functional core, imperative shell**: Pure functions with side effects at edges
- **Config-driven**: No hardcoded field names
- **Strong typing**: No `any` types, exhaustive switches
- **Small functions**: Testable, composable units

## Troubleshooting

### Rate Limiting

The pipeline handles Socrata rate limits automatically:
- Exponential backoff with jitter
- Respects `Retry-After` headers
- Configurable retry attempts

### Database Issues

```bash
# Reset database
rm -f data/pipeline.db
npm run migrate

# Check database stats
sqlite3 data/pipeline.db "SELECT name, COUNT(*) FROM sqlite_master s, pragma_table_info(s.name) GROUP BY s.name;"
```

### Configuration Errors

```bash
# Validate all city configs
npm run test -- --grep "config"

# Test specific city
npm run extract -- --city chicago --limit 1
```

## License

MIT