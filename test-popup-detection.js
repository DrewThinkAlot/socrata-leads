// Simple test for pop-up vendor detection
import { analyzeSpotOnIntelligence } from './src/filters/spoton.js';

// Test data with pop-up vendor characteristics
const popUpEvents = [
  {
    evidence: [
      {
        business_name: "Sarah's Pop-up Taco Stand",
        description: "Temporary weekend food vendor at farmers market",
        type: "Temporary Food Vendor Permit"
      }
    ]
  }
];

// Test data with regular restaurant
const regularEvents = [
  {
    evidence: [
      {
        business_name: "Downtown Bistro",
        description: "Full-service restaurant with bar and dining area",
        type: "Restaurant License"
      }
    ]
  }
];

// Test data with food truck
const foodTruckEvents = [
  {
    evidence: [
      {
        business_name: "Mobile Kitchen Express",
        description: "Food truck serving gourmet sandwiches",
        type: "Mobile Food Vendor Permit"
      }
    ]
  }
];

async function testPopUpDetection() {
  console.log('Testing pop-up vendor detection...\n');
  
  // Test 1: Pop-up vendor
  console.log('Test 1: Pop-up vendor');
  const popUpResult = await analyzeSpotOnIntelligence(popUpEvents, { exclude_pop_up_vendors: true });
  console.log('Is pop-up vendor:', popUpResult.is_pop_up_vendor);
  console.log('Service model:', popUpResult.service_model);
  console.log('SpotOn score:', popUpResult.spoton_score);
  
  // Test 2: Regular restaurant
  console.log('\nTest 2: Regular restaurant');
  const regularResult = await analyzeSpotOnIntelligence(regularEvents, { exclude_pop_up_vendors: true });
  console.log('Is pop-up vendor:', regularResult.is_pop_up_vendor);
  console.log('Service model:', regularResult.service_model);
  console.log('SpotOn score:', regularResult.spoton_score);
  
  // Test 3: Food truck
  console.log('\nTest 3: Food truck');
  const truckResult = await analyzeSpotOnIntelligence(foodTruckEvents, { exclude_pop_up_vendors: true });
  console.log('Is pop-up vendor:', truckResult.is_pop_up_vendor);
  console.log('Service model:', truckResult.service_model);
  console.log('SpotOn score:', truckResult.spoton_score);
}

testPopUpDetection().catch(console.error);
