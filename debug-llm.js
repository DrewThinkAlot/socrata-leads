#!/usr/bin/env node

/**
 * Debug script to see raw LLM responses and parsing issues
 */

import { config as loadEnv } from 'dotenv';
loadEnv();

// Mock the LLM utilities to see raw responses
const CFG = {
  OPENAI_API_KEY: process.env.OPENAI_API_KEY ?? '',
  OPENAI_MODEL: process.env.OPENAI_MODEL || 'gpt-5-mini',
  OPENAI_API_URL: process.env.OPENAI_API_URL || 'https://api.openai.com/v1/chat/completions',
};

async function debugLLMResponses() {
  console.log('🔍 Debugging LLM Responses');
  console.log('=' .repeat(50));
  
  const businessDescription = "A family-owned Italian restaurant serving authentic pasta dishes and wood-fired pizzas with outdoor seating and delivery service.";
  
  try {
    // Test the actual OpenAI call with debug output
    const messages = [
      { role: 'system', content: 'You are an expert business analyst. Return JSON.' },
      { role: 'user', content: `Return JSON only. key is category. value must be one of: Restaurant/Food Service, Retail/Store, Professional Services, Healthcare/Medical, Construction/Contractor, Entertainment/Recreation, Automotive, Technology/IT, Education/Training, Other. Business Name: Mario's Pizzeria. Description: ${businessDescription}` },
    ];
    
    console.log('📤 Sending to OpenAI:');
    console.log('Messages:', JSON.stringify(messages, null, 2));
    
    const res = await fetch(CFG.OPENAI_API_URL, {
      method: 'POST',
      headers: { 
        Authorization: `Bearer ${CFG.OPENAI_API_KEY}`, 
        'Content-Type': 'application/json' 
      },
      body: JSON.stringify({
        model: CFG.OPENAI_MODEL,
        messages,
        response_format: { type: 'json_object' },
        temperature: 0.1,
        max_tokens: 40
      }),
    });
    
    const data = await res.json();
    console.log('📥 Raw response:', JSON.stringify(data, null, 2));
    
    if (data.choices?.[0]?.message?.content) {
      const content = data.choices[0].message.content;
      console.log('📝 Raw content:', content);
      
      // Test parsing
      try {
        const parsed = JSON.parse(content);
        console.log('✅ Parsed JSON:', parsed);
        
        // Check if category is valid
        const CATEGORY_LIST = [
          'Restaurant/Food Service', 'Retail/Store', 'Professional Services', 'Healthcare/Medical',
          'Construction/Contractor', 'Entertainment/Recreation', 'Automotive', 'Technology/IT', 'Education/Training', 'Other',
        ];
        
        const category = parsed.category || parsed.Category || parsed.businessType || parsed.type;
        console.log('🏷️ Extracted category:', category);
        console.log('✅ Is valid category?', CATEGORY_LIST.includes(category));
        
      } catch (e) {
        console.log('❌ JSON parsing failed:', e.message);
      }
    }
    
  } catch (error) {
    console.error('❌ Error:', error);
  }
}

debugLLMResponses();
