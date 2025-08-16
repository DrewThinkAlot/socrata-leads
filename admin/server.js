#!/usr/bin/env node

/**
 * Simple Express server for restaurant lead pipeline admin interface
 */

const express = require('express');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');
const Database = require('better-sqlite3');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const PROJECT_ROOT = path.join(__dirname, '..');

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Store active jobs
const activeJobs = new Map();

// WebSocket connection for real-time updates
wss.on('connection', (ws) => {
  console.log('Client connected');
  
  ws.on('close', () => {
    console.log('Client disconnected');
  });
});

// Broadcast to all connected clients
function broadcast(data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// API Routes

// Get pipeline status
app.get('/api/status', (req, res) => {
  const jobs = Array.from(activeJobs.entries()).map(([id, job]) => ({
    id,
    type: job.type,
    status: job.status,
    startTime: job.startTime,
    progress: job.progress || 0
  }));
  
  res.json({ jobs });
});

// Start data extraction
app.post('/api/extract', (req, res) => {
  const { city, since, maxRecords, optimized } = req.body;
  
  if (!city) {
    return res.status(400).json({ error: 'City is required' });
  }
  
  const jobId = `extract-${Date.now()}`;
  const args = ['run', 'extract', '--', '--city', city];
  
  if (since) args.push('--since', since);
  if (maxRecords) args.push('--limit', maxRecords.toString());
  if (optimized) args.push('--optimized');
  
  const job = {
    id: jobId,
    type: 'extract',
    status: 'running',
    startTime: new Date().toISOString(),
    city,
    progress: 0
  };
  
  activeJobs.set(jobId, job);
  
  const process = spawn('npm', args, {
    cwd: PROJECT_ROOT,
    stdio: ['pipe', 'pipe', 'pipe']
  });
  
  job.process = process;
  
  process.stdout.on('data', (data) => {
    const output = data.toString();
    console.log(`[${jobId}] ${output}`);
    
    // Parse progress if available
    if (output.includes('completed')) {
      job.progress = 100;
    } else if (output.includes('processing')) {
      job.progress = Math.min(job.progress + 10, 90);
    }
    
    broadcast({
      type: 'job-update',
      jobId,
      status: job.status,
      progress: job.progress,
      output: output.trim()
    });
  });
  
  process.stderr.on('data', (data) => {
    console.error(`[${jobId}] Error: ${data}`);
    broadcast({
      type: 'job-error',
      jobId,
      error: data.toString()
    });
  });
  
  process.on('close', (code) => {
    job.status = code === 0 ? 'completed' : 'failed';
    job.progress = code === 0 ? 100 : job.progress;
    job.endTime = new Date().toISOString();
    
    broadcast({
      type: 'job-complete',
      jobId,
      status: job.status,
      code
    });
    
    // Clean up after 5 minutes
    setTimeout(() => {
      activeJobs.delete(jobId);
    }, 5 * 60 * 1000);
  });
  
  res.json({ jobId, status: 'started' });
});

// Generate leads
app.post('/api/generate-leads', (req, res) => {
  const { type, city, limit } = req.body;
  
  if (!type || !city) {
    return res.status(400).json({ error: 'Type and city are required' });
  }
  
  const jobId = `generate-${type}-${Date.now()}`;
  let scriptName;
  
  switch (type) {
    case '30-60-day':
      scriptName = 'generate-30-60-day-leads.js';
      break;
    case 'restaurant':
      scriptName = 'generate-restaurant-leads.js';
      break;
    case 'independent-restaurant':
      scriptName = 'generate-independent-restaurant-leads.js';
      break;
    default:
      return res.status(400).json({ error: 'Invalid lead type' });
  }
  
  const args = [scriptName];
  if (limit) args.push('--limit', limit.toString());
  
  const job = {
    id: jobId,
    type: `generate-${type}`,
    status: 'running',
    startTime: new Date().toISOString(),
    city,
    progress: 0
  };
  
  activeJobs.set(jobId, job);
  
  const process = spawn('node', args, {
    cwd: PROJECT_ROOT,
    stdio: ['pipe', 'pipe', 'pipe']
  });
  
  job.process = process;
  
  process.stdout.on('data', (data) => {
    const output = data.toString();
    console.log(`[${jobId}] ${output}`);
    
    // Parse progress and results
    if (output.includes('exported to:')) {
      job.progress = 90;
      const match = output.match(/exported to: (.+\.csv)/);
      if (match) {
        job.outputFile = match[1];
      }
    } else if (output.includes('Total qualified leads:')) {
      job.progress = 100;
      const match = output.match(/Total qualified leads: (\d+)/);
      if (match) {
        job.leadCount = parseInt(match[1]);
      }
    }
    
    broadcast({
      type: 'job-update',
      jobId,
      status: job.status,
      progress: job.progress,
      output: output.trim(),
      leadCount: job.leadCount,
      outputFile: job.outputFile
    });
  });
  
  process.stderr.on('data', (data) => {
    console.error(`[${jobId}] Error: ${data}`);
    broadcast({
      type: 'job-error',
      jobId,
      error: data.toString()
    });
  });
  
  process.on('close', (code) => {
    job.status = code === 0 ? 'completed' : 'failed';
    job.progress = code === 0 ? 100 : job.progress;
    job.endTime = new Date().toISOString();
    
    broadcast({
      type: 'job-complete',
      jobId,
      status: job.status,
      code,
      leadCount: job.leadCount,
      outputFile: job.outputFile
    });
    
    // Clean up after 5 minutes
    setTimeout(() => {
      activeJobs.delete(jobId);
    }, 5 * 60 * 1000);
  });
  
  res.json({ jobId, status: 'started' });
});

// Download CSV files
app.get('/api/download/:filename', (req, res) => {
  const filename = req.params.filename;
  // Only allow .csv files
  if (!/^[A-Za-z0-9._-]+\.csv$/i.test(filename)) {
    return res.status(400).json({ error: 'Invalid filename' });
  }

  const outDir = path.join(PROJECT_ROOT, 'out');
  const filePath = path.resolve(outDir, filename);
  // Ensure resolved path stays within outDir
  if (!filePath.startsWith(path.resolve(outDir) + path.sep)) {
    return res.status(400).json({ error: 'Invalid path' });
  }

  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: 'File not found' });
  }

  res.download(filePath);
});

// Get leads from database
app.get('/api/leads', (req, res) => {
  const { city, limit = 50 } = req.query;
  
  if (!city) {
    return res.status(400).json({ error: 'City parameter is required' });
  }
  
  try {
    const dbPath = path.join(PROJECT_ROOT, 'data', 'pipeline.db');
    
    if (!fs.existsSync(dbPath)) {
      return res.json({ leads: [], message: 'No database found. Run extraction first.' });
    }
    
    const db = new Database(dbPath, { readonly: true });
    
    // Get leads from the leads table
    const query = `
      SELECT * FROM leads 
      WHERE city = ? 
      ORDER BY created_at DESC 
      LIMIT ?
    `;
    
    const leads = db.prepare(query).all(city, parseInt(limit));
    
    // Parse JSON fields
    const parsedLeads = leads.map(lead => ({
      ...lead,
      spoton_intelligence: lead.spoton_intelligence ? JSON.parse(lead.spoton_intelligence) : null,
      evidence: lead.evidence ? JSON.parse(lead.evidence) : []
    }));
    
    db.close();
    
    res.json({ 
      leads: parsedLeads,
      count: parsedLeads.length,
      city 
    });
    
  } catch (error) {
    console.error('Error fetching leads:', error);
    res.status(500).json({ 
      error: 'Failed to fetch leads', 
      message: error.message 
    });
  }
});

// List available CSV files
app.get('/api/files', (req, res) => {
  const outDir = path.join(PROJECT_ROOT, 'out');
  
  if (!fs.existsSync(outDir)) {
    return res.json({ files: [] });
  }
  
  const files = fs.readdirSync(outDir)
    .filter(file => file.endsWith('.csv'))
    .map(file => {
      const filePath = path.join(outDir, file);
      const stats = fs.statSync(filePath);
      return {
        name: file,
        size: stats.size,
        modified: stats.mtime.toISOString()
      };
    })
    .sort((a, b) => new Date(b.modified) - new Date(a.modified));
  
  res.json({ files });
});

// Stop a running job
app.post('/api/stop/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  const job = activeJobs.get(jobId);
  
  if (!job || !job.process) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  job.process.kill('SIGTERM');
  job.status = 'stopped';
  
  broadcast({
    type: 'job-complete',
    jobId,
    status: 'stopped'
  });
  
  res.json({ status: 'stopped' });
});

// Evaluation pipeline endpoints

// Run evaluation pipeline
app.post('/api/evaluate', (req, res) => {
  const { mode, city, daysBack, monthsBack, specificDate, restaurantOnly } = req.body;
  
  const jobId = `evaluate-${Date.now()}`;
  const args = ['run', 'evaluate', '--', '--mode', mode || 'full'];
  
  if (city) args.push('--city', city);
  if (daysBack) args.push('--days-back', daysBack.toString());
  if (monthsBack) args.push('--months-back', monthsBack.toString());
  if (specificDate) args.push('--date', specificDate);
  if (restaurantOnly) args.push('--restaurant-only');
  
  const job = {
    id: jobId,
    type: 'evaluate',
    status: 'running',
    startTime: new Date().toISOString(),
    mode,
    city,
    progress: 0
  };
  
  activeJobs.set(jobId, job);
  
  const process = spawn('npm', args, {
    cwd: PROJECT_ROOT,
    stdio: ['pipe', 'pipe', 'pipe']
  });
  
  job.process = process;
  
  process.stdout.on('data', (data) => {
    const output = data.toString();
    console.log(`[${jobId}] ${output}`);
    
    // Parse evaluation progress
    if (output.includes('Ground truth collection complete')) {
      job.progress = 25;
    } else if (output.includes('Evaluation metrics calculated')) {
      job.progress = 75;
    } else if (output.includes('Evaluation complete')) {
      job.progress = 100;
    }
    
    broadcast({
      type: 'job-update',
      jobId,
      status: job.status,
      progress: job.progress,
      output: output.trim()
    });
  });
  
  process.stderr.on('data', (data) => {
    console.error(`[${jobId}] Error: ${data}`);
    broadcast({
      type: 'job-error',
      jobId,
      error: data.toString()
    });
  });
  
  process.on('close', (code) => {
    job.status = code === 0 ? 'completed' : 'failed';
    job.progress = code === 0 ? 100 : job.progress;
    job.endTime = new Date().toISOString();
    
    broadcast({
      type: 'job-complete',
      jobId,
      status: job.status,
      code
    });
    
    setTimeout(() => {
      activeJobs.delete(jobId);
    }, 5 * 60 * 1000);
  });
  
  res.json({ jobId, status: 'started' });
});

// Get evaluation results
app.get('/api/evaluation-results', (req, res) => {
  const { city, limit = 10 } = req.query;
  
  try {
    const dbPath = path.join(PROJECT_ROOT, 'data', 'pipeline.db');
    
    if (!fs.existsSync(dbPath)) {
      return res.json({ results: [], message: 'No database found. Run evaluation first.' });
    }
    
    const db = new Database(dbPath, { readonly: true });
    
    // Check if evaluation_results table exists
    const tableExists = db.prepare(`
      SELECT name FROM sqlite_master 
      WHERE type='table' AND name='evaluation_results'
    `).get();
    
    if (!tableExists) {
      return res.json({ results: [], message: 'No evaluation results found. Run evaluation first.' });
    }
    
    let query = 'SELECT * FROM evaluation_results ORDER BY created_at DESC LIMIT ?';
    let params = [parseInt(limit)];
    
    if (city) {
      query = 'SELECT * FROM evaluation_results WHERE city = ? ORDER BY created_at DESC LIMIT ?';
      params = [city, parseInt(limit)];
    }
    
    const results = db.prepare(query).all(...params);
    
    // Parse JSON fields
    const parsedResults = results.map(result => ({
      ...result,
      metrics: result.metrics ? JSON.parse(result.metrics) : null,
      coverage: result.coverage ? JSON.parse(result.coverage) : null,
      ablation_results: result.ablation_results ? JSON.parse(result.ablation_results) : null
    }));
    
    db.close();
    
    res.json({ 
      results: parsedResults,
      count: parsedResults.length,
      city: city || 'all'
    });
    
  } catch (error) {
    console.error('Error fetching evaluation results:', error);
    res.status(500).json({ 
      error: 'Failed to fetch evaluation results', 
      message: error.message 
    });
  }
});

// Run full pipeline (extract -> normalize -> fuse -> score -> export)
app.post('/api/run-pipeline', (req, res) => {
  const { city, since, maxRecords } = req.body;
  
  if (!city) {
    return res.status(400).json({ error: 'City is required' });
  }
  
  const jobId = `pipeline-${Date.now()}`;
  const args = ['run', 'daily'];
  
  if (city !== 'all') {
    args.push('--city', city);
  }
  if (since) args.push('--since', since);
  if (maxRecords) args.push('--limit', maxRecords.toString());
  
  const job = {
    id: jobId,
    type: 'full-pipeline',
    status: 'running',
    startTime: new Date().toISOString(),
    city,
    progress: 0,
    stage: 'extracting'
  };
  
  activeJobs.set(jobId, job);
  
  const process = spawn('npm', args, {
    cwd: PROJECT_ROOT,
    stdio: ['pipe', 'pipe', 'pipe']
  });
  
  job.process = process;
  
  process.stdout.on('data', (data) => {
    const output = data.toString();
    console.log(`[${jobId}] ${output}`);
    
    // Parse pipeline stages
    if (output.includes('Extraction complete')) {
      job.stage = 'normalizing';
      job.progress = 20;
    } else if (output.includes('Normalization complete')) {
      job.stage = 'fusing';
      job.progress = 40;
    } else if (output.includes('Fusion complete')) {
      job.stage = 'scoring';
      job.progress = 60;
    } else if (output.includes('Scoring complete')) {
      job.stage = 'exporting';
      job.progress = 80;
    } else if (output.includes('Export complete')) {
      job.stage = 'completed';
      job.progress = 100;
    }
    
    broadcast({
      type: 'job-update',
      jobId,
      status: job.status,
      progress: job.progress,
      stage: job.stage,
      output: output.trim()
    });
  });
  
  process.stderr.on('data', (data) => {
    console.error(`[${jobId}] Error: ${data}`);
    broadcast({
      type: 'job-error',
      jobId,
      error: data.toString()
    });
  });
  
  process.on('close', (code) => {
    job.status = code === 0 ? 'completed' : 'failed';
    job.progress = code === 0 ? 100 : job.progress;
    job.endTime = new Date().toISOString();
    
    broadcast({
      type: 'job-complete',
      jobId,
      status: job.status,
      code,
      stage: job.stage
    });
    
    setTimeout(() => {
      activeJobs.delete(jobId);
    }, 5 * 60 * 1000);
  });
  
  res.json({ jobId, status: 'started' });
});

// Get database statistics
app.get('/api/stats', (req, res) => {
  try {
    const dbPath = path.join(PROJECT_ROOT, 'data', 'pipeline.db');
    
    if (!fs.existsSync(dbPath)) {
      return res.json({ 
        stats: { 
          totalRecords: 0, 
          totalLeads: 0, 
          cities: [],
          lastUpdate: null 
        } 
      });
    }
    
    const db = new Database(dbPath, { readonly: true });
    
    const totalRecords = db.prepare('SELECT COUNT(*) as count FROM records').get().count;
    const totalLeads = db.prepare('SELECT COUNT(*) as count FROM leads').get().count;
    
    const cities = db.prepare(`
      SELECT city, COUNT(*) as count 
      FROM records 
      WHERE city IS NOT NULL 
      GROUP BY city 
      ORDER BY count DESC
    `).all();
    
    const lastUpdate = db.prepare('SELECT MAX(updated_at) as last_update FROM records').get().last_update;
    
    db.close();
    
    res.json({
      stats: {
        totalRecords,
        totalLeads,
        cities,
        lastUpdate
      }
    });
    
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ 
      error: 'Failed to fetch statistics', 
      message: error.message 
    });
  }
});

// Serve the main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
server.listen(PORT, () => {
  console.log(`🚀 Restaurant Lead Pipeline Admin running at http://localhost:${PORT}`);
  console.log(`📊 Dashboard: http://localhost:${PORT}`);
});
