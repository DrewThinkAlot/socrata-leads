// Test pop-up vendor detection
import { detectPopUpVendor } from './src/filters/spoton.js';

// Test cases
const testCases = [
  {
    name: 'Pop-up taco stand',
    evidence: [{
      business_name: "Sarah's Pop-up Taco Stand",
      description: "Temporary weekend food vendor at farmers market",
      type: "Temporary Food Vendor Permit"
    }],
    expected: true
  },
  {
    name: 'Regular restaurant',
    evidence: [{
      business_name: "Downtown Bistro",
      description: "Full-service restaurant with bar and dining area",
      type: "Restaurant License"
    }],
    expected: false
  },
  {
    name: 'Food truck',
    evidence: [{
      business_name: "Mobile Kitchen Express",
      description: "Food truck serving gourmet sandwiches",
      type: "Mobile Food Vendor Permit"
    }],
    expected: true
  },
  {
    name: 'Market vendor',
    evidence: [{
      business_name: "Weekend Market Stall",
      description: "Seasonal outdoor vendor at craft fair",
      type: "Market Vendor License"
    }],
    expected: true
  }
];

async function runTests() {
  console.log('Testing pop-up vendor detection...\n');
  
  for (const testCase of testCases) {
    const result = await detectPopUpVendor(testCase.evidence);
    const passed = result === testCase.expected;
    console.log(`${testCase.name}: ${result} (${passed ? 'PASS' : 'FAIL'})`);
  }
}

runTests().catch(console.error);
