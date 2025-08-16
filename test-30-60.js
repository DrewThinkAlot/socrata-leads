// Standalone test for 30-60 day timeline detection
function parseDate(dateStr) {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return isNaN(date.getTime()) ? null : date;
}

async function estimateTimeline(records) {
  const futureDates = records
    .map(r => parseDate(r.future_date))
    .filter(d => d !== null)
    .filter(d => d > new Date())
    .sort((a, b) => a.getTime() - b.getTime());
  
  if (futureDates.length === 0) return undefined;
  
  const earliestFuture = futureDates[0];
  const now = new Date();
  const daysUntilOpening = Math.ceil((earliestFuture.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
  
  return daysUntilOpening;
}

// Mock record with a future license start date 45 days out
const mockRecords = [
  {
    business_name: 'Test Restaurant',
    address: '123 Test St, Chicago, IL',
    type: 'Business License',
    event_date: '2025-08-01',
    future_date: '2025-09-29', // 45 days from today (2025-08-15)
  },
];

const today = new Date('2025-08-15');
const daysUntil = await estimateTimeline(mockRecords);
console.log(`Days until opening: ${daysUntil}`);
console.log(`Expected 30-60 range: ${daysUntil >= 30 && daysUntil <= 60 ? '✅ PASS' : '❌ FAIL'}`);
