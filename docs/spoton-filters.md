# SpotOn Filters Implementation Guide

## Overview
This document describes the SpotOn-specific business intelligence filters implemented in the socrata-leads pipeline. These filters are designed to identify high-value prospects for SpotOn sales representatives based on 8 priority signals.

## 8 Priority Signals Implemented

### 1. Service Model Detection
- **Full-service restaurants** (highest priority)
- **Fast-casual with table service** (high priority)
- **Takeout-only** (medium priority)
- **Delivery-first/ghost kitchens** (low priority)

### 2. Capacity Requirements
- **Minimum seating capacity**: 25 seats (configurable)
- **Minimum square footage**: 1,500 sq ft (configurable)

### 3. Liquor License Analysis
- **Full bar license** (highest priority)
- **Restaurant license** (high priority)
- **Beer & wine license** (medium priority)
- **Tavern license** (medium priority)

### 4. Reservation Systems
- **OpenTable** detection
- **Resy** detection
- **SevenRooms** detection
- **Yelp Waitlist** detection
- **Tock** detection

### 5. Kitchen Complexity Indicators
- **Type I hood** presence
- **Multiple cook lines**
- **Hot/cold stations**
- **Multiple printers** (KDS indicators)

### 6. Operator Experience
- **Chain expansion** (highest priority)
- **Existing operator** (high priority)
- **New operator** (medium priority)

### 7. Timeline Validation
- **30-60 day opening window** (configurable)
- **Future-dated permits/licenses**

### 8. Business Category
- **Restaurants** (highest priority)
- **Bars** (high priority)
- **Breweries** (medium priority)
- **Wineries** (medium priority)

## Configuration

### City-Level Configuration
Add SpotOn filter configuration to your city YAML files:

```yaml
spoton_filters:
  min_seat_capacity: 25
  min_square_footage: 1500
  preferred_business_types:
    - Restaurant
    - Bar
    - Fast Casual
    - Full Service
  reservation_platforms:
    - OpenTable
    - Resy
    - SevenRooms
    - Yelp Waitlist
  liquor_license_priority:
    - Full Bar License
    - Restaurant License
    - Beer and Wine License
  timeline_window_days: [30, 60]
  service_model_weights:
    full-service: 1.5
    fast-casual: 1.3
    takeout-only: 0.5
    delivery-first: 0.3
  operator_type_weights:
    chain-expansion: 1.4
    existing-operator: 1.2
    new-operator: 1.0
```

## CSV Export Format

The enhanced CSV export includes SpotOn intelligence fields:

| Field | Description |
|-------|-------------|
| `spoton_score` | SpotOn-specific score (0-100) |
| `business_category` | Detected business type |
| `service_model` | Service model classification |
| `seat_capacity` | Estimated seating capacity |
| `square_footage` | Estimated square footage |
| `liquor_license_type` | Type of liquor license |
| `reservation_systems` | Detected reservation platforms |
| `kitchen_complexity` | Kitchen complexity level |
| `operator_type` | Operator experience classification |
| `opening_timeline_days` | Days until opening |
| `has_type_i_hood` | Type I hood indicator |
| `has_multiple_cook_lines` | Multiple cook lines indicator |
| `has_hot_cold_stations` | Hot/cold stations indicator |
| `has_multiple_printers` | Multiple printers indicator |

## Usage Examples

### Running the Pipeline
```bash
# Extract data for a city
npm run extract -- --city chicago

# Score leads with SpotOn intelligence
npm run score -- --city chicago

# Export top 12 leads with SpotOn filters
npm run export -- --city chicago --limit 12 --out out/chicago-spoton-drop.csv
```

### Filtering High-Value Leads
```bash
# Filter for full-service restaurants with liquor licenses
grep "full-service" out/chicago-spoton-drop.csv | grep -v "delivery-first"

# Filter for chain expansions
grep "chain-expansion" out/chicago-spoton-drop.csv

# Filter for high-capacity restaurants
awk -F',' '$7 >= 50' out/chicago-spoton-drop.csv
```

## Integration with Sales Workflow

### Lead Prioritization
1. **Tier 1**: SpotOn score > 80 + full-service + liquor license + chain expansion
2. **Tier 2**: SpotOn score > 70 + full-service + liquor license
3. **Tier 3**: SpotOn score > 60 + fast-casual + liquor license

### Contact Strategy
- **Chain expansions**: Focus on operational efficiency and standardization
- **Existing operators**: Emphasize ROI and proven solutions
- **New operators**: Highlight ease of use and comprehensive support

## Technical Architecture

### Core Components
- `src/filters/spoton.ts`: Main SpotOn intelligence engine
- `src/types.ts`: Enhanced Lead type with SpotOn intelligence
- `src/util/csv.ts`: CSV export with SpotOn fields
- `configs/*.yaml`: City-specific SpotOn configurations

### Data Sources
- Building permits (construction signals)
- Liquor licenses (business type + timeline)
- Food inspections (operational status)
- Business licenses (operator experience)

## Testing

Run the SpotOn filter tests:
```bash
npm test tests/filters/spoton.test.ts
```

## Future Enhancements

- **Social media integration** (Instagram, Facebook presence)
- **Menu complexity analysis** (multi-menu operations)
- **Competition analysis** (nearby restaurants)
- **Real estate data** (lease terms, property values)
- **Financial indicators** (investment levels, funding rounds)

## Support

For questions or issues with SpotOn filters, please refer to:
- Technical documentation in `/docs`
- Configuration examples in `/configs`
- Test cases in `/tests/filters`
