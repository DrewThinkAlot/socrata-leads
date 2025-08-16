#!/usr/bin/env node

/**
 * Optimized pipeline orchestration with dependency management
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { spawn } from 'child_process';
import { performance } from 'perf_hooks';

config();

interface PipelineStep {
  name: string;
  command: string;
  args: string[];
  dependencies: string[];
  timeout: number;
  retries: number;
  optional: boolean;
}

interface StepResult {
  name: string;
  success: boolean;
  duration: number;
  error?: string;
  output?: string;
}

function parseCliArgs() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: { type: 'string', short: 'c' },
      steps: { type: 'string', short: 's' },
      parallel: { type: 'boolean', short: 'p' },
      help: { type: 'boolean', short: 'h' },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run pipeline:optimized -- --city <city>

Options:
  -c, --city <city>     City name (required)
  -s, --steps <steps>   Comma-separated steps to run (default: all)
  -p, --parallel        Run independent steps in parallel
  -h, --help           Show this help message

Available steps: extract, normalize, score, export
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  return {
    city: values.city as string,
    steps: values.steps ? (values.steps as string).split(',') : ['extract', 'normalize', 'score', 'export'],
    parallel: values.parallel as boolean || false,
  };
}

/**
 * Define optimized pipeline steps
 */
function getPipelineSteps(city: string): PipelineStep[] {
  return [
    {
      name: 'schema-migration',
      command: 'npx',
      args: ['tsx', 'src/storage/migrations/run.ts'],
      dependencies: [],
      timeout: 30000,
      retries: 1,
      optional: false,
    },
    {
      name: 'extract',
      command: 'npx',
      args: ['tsx', 'src/extract/run.ts', '--city', city],
      dependencies: ['schema-migration'],
      timeout: 300000, // 5 minutes
      retries: 2,
      optional: false,
    },
    {
      name: 'normalize',
      command: 'npx',
      args: ['tsx', 'src/normalize/run.ts', '--city', city, '--fast', '--workers', '4', '--batchSize', '5000'],
      dependencies: ['extract'],
      timeout: 1200000, // 20 minutes
      retries: 1,
      optional: false,
    },
    {
      name: 'score',
      command: 'npx',
      args: ['tsx', 'src/score/run-optimized.ts', '--city', city, '--chunkSize', '10000', '--workers', '4'],
      dependencies: ['normalize'],
      timeout: 600000, // 10 minutes
      retries: 1,
      optional: false,
    },
    {
      name: 'export',
      command: 'npx',
      args: ['tsx', 'src/export/run.ts', '--city', city],
      dependencies: ['score'],
      timeout: 120000, // 2 minutes
      retries: 1,
      optional: true,
    }
  ];
}

/**
 * Execute a single pipeline step
 */
async function executeStep(step: PipelineStep): Promise<StepResult> {
  const startTime = performance.now();
  
  logger.info(`Starting step: ${step.name}`, {
    command: step.command,
    args: step.args,
    timeout: step.timeout
  });

  return new Promise((resolve) => {
    const process = spawn(step.command, step.args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: step.timeout
    });

    let output = '';
    let error = '';

    process.stdout?.on('data', (data) => {
      const chunk = data.toString();
      output += chunk;
      
      // Log progress in real-time
      if (chunk.includes('[INFO]')) {
        logger.info(`${step.name}: ${chunk.trim()}`);
      }
    });

    process.stderr?.on('data', (data) => {
      const chunk = data.toString();
      error += chunk;
      logger.warn(`${step.name} stderr: ${chunk.trim()}`);
    });

    process.on('close', (code) => {
      const duration = performance.now() - startTime;
      const success = code === 0;
      
      const result: StepResult = {
        name: step.name,
        success,
        duration,
        output: output.substring(0, 1000), // Limit output size
        error: success ? undefined : error.substring(0, 500)
      };

      logger.info(`Step ${step.name} ${success ? 'completed' : 'failed'}`, {
        duration: Math.round(duration),
        exitCode: code
      });

      resolve(result);
    });

    process.on('error', (err) => {
      const duration = performance.now() - startTime;
      resolve({
        name: step.name,
        success: false,
        duration,
        error: err.message
      });
    });
  });
}

/**
 * Execute step with retries
 */
async function executeStepWithRetries(step: PipelineStep): Promise<StepResult> {
  let lastResult: StepResult | null = null;
  
  for (let attempt = 1; attempt <= step.retries + 1; attempt++) {
    logger.info(`Executing ${step.name} (attempt ${attempt}/${step.retries + 1})`);
    
    lastResult = await executeStep(step);
    
    if (lastResult.success) {
      return lastResult;
    }
    
    if (attempt <= step.retries) {
      logger.warn(`Step ${step.name} failed, retrying in 5 seconds...`, {
        attempt,
        error: lastResult.error
      });
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  return lastResult!;
}

/**
 * Check if step dependencies are satisfied
 */
function canExecuteStep(step: PipelineStep, completedSteps: Set<string>): boolean {
  return step.dependencies.every(dep => completedSteps.has(dep));
}

/**
 * Get next executable steps
 */
function getNextSteps(
  allSteps: PipelineStep[],
  completedSteps: Set<string>,
  runningSteps: Set<string>,
  targetSteps: string[]
): PipelineStep[] {
  return allSteps.filter(step => 
    targetSteps.includes(step.name) &&
    !completedSteps.has(step.name) &&
    !runningSteps.has(step.name) &&
    canExecuteStep(step, completedSteps)
  );
}

/**
 * Main orchestration function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting optimized pipeline orchestration', args);
    
    const storage = await createStorage();
    const allSteps = getPipelineSteps(args.city);
    const targetSteps = args.steps;
    
    // Validate target steps
    const validSteps = allSteps.map(s => s.name);
    const invalidSteps = targetSteps.filter(s => !validSteps.includes(s));
    if (invalidSteps.length > 0) {
      throw new Error(`Invalid steps: ${invalidSteps.join(', ')}. Valid steps: ${validSteps.join(', ')}`);
    }
    
    const completedSteps = new Set<string>();
    const runningSteps = new Set<string>();
    const results: StepResult[] = [];
    const startTime = performance.now();
    
    logger.info(`Pipeline will execute steps: ${targetSteps.join(' → ')}`);
    
    // Execute steps with dependency management
    while (completedSteps.size < targetSteps.length) {
      const nextSteps = getNextSteps(allSteps, completedSteps, runningSteps, targetSteps);
      
      if (nextSteps.length === 0) {
        // Check if we're waiting for running steps
        if (runningSteps.size > 0) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }
        
        // No more steps can be executed
        const remainingSteps = targetSteps.filter(s => !completedSteps.has(s));
        throw new Error(`Cannot execute remaining steps due to failed dependencies: ${remainingSteps.join(', ')}`);
      }
      
      // Execute steps (parallel if enabled, otherwise sequential)
      const stepsToExecute = args.parallel ? nextSteps : [nextSteps[0]];
      
      const stepPromises = stepsToExecute.map(async (step) => {
        runningSteps.add(step.name);
        
        try {
          const result = await executeStepWithRetries(step);
          results.push(result);
          
          if (result.success) {
            completedSteps.add(step.name);
            logger.info(`✅ Step ${step.name} completed successfully`);
          } else {
            if (!step.optional) {
              throw new Error(`Required step ${step.name} failed: ${result.error}`);
            }
            logger.warn(`⚠️ Optional step ${step.name} failed but continuing`, { error: result.error });
            completedSteps.add(step.name); // Mark as completed to unblock dependencies
          }
        } finally {
          runningSteps.delete(step.name);
        }
      });
      
      await Promise.all(stepPromises);
    }
    
    const totalDuration = performance.now() - startTime;
    const successfulSteps = results.filter(r => r.success).length;
    
    logger.info('Pipeline orchestration completed', {
      totalSteps: results.length,
      successfulSteps,
      failedSteps: results.length - successfulSteps,
      totalDurationMs: Math.round(totalDuration),
      totalDurationMinutes: Math.round(totalDuration / 60000),
      results: results.map(r => ({
        step: r.name,
        success: r.success,
        durationMs: Math.round(r.duration)
      }))
    });
    
    await storage.close();
    
    // Exit with error if any required steps failed
    const failedRequiredSteps = results.filter(r => !r.success && !allSteps.find(s => s.name === r.name)?.optional);
    if (failedRequiredSteps.length > 0) {
      logger.error('Pipeline failed due to required step failures', {
        failedSteps: failedRequiredSteps.map(r => r.name)
      });
      process.exit(1);
    }
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Pipeline orchestration failed', { error, args });
    process.exit(1);
  }
}

main().catch(console.error);
