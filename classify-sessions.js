#!/usr/bin/env node

/*
  Session-Based Speech Classification Script
  
  Classifies speeches from specific sessions and updates the web interface
  to show the new AI-classified topics in the sitting overview.
  
  Usage: node classify-sessions.js [number_of_sessions]
*/

// Load environment variables from .env file
require('dotenv').config();

const sqlite3 = require('sqlite3').verbose();
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');
const cliProgress = require('cli-progress');

// Configuration
const DB_FILE = path.join(__dirname, 'ep_data.db');
const PROMPT_FILE = path.join(__dirname, 'gpt-topic-classification-prompt.md');
const MODEL = 'gpt-5-nano-2025-08-07';

// Tier 2 Rate Limits
const MAX_RPM = 5000;
const MAX_TPM = 2000000;
const BATCH_SIZE = 10;
const REQUEST_DELAY = 60 / MAX_RPM * 1000;

// GPT-5-nano Pricing
const INPUT_COST_PER_1M = 0.05;
const OUTPUT_COST_PER_1M = 0.40;

class SessionClassifier {
  constructor() {
    this.db = new sqlite3.Database(DB_FILE);
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    this.systemPrompt = '';
    this.results = [];
    this.progressBar = null;
    
    // Rate limiting tracking
    this.requestCount = 0;
    this.tokenCount = 0;
    this.lastMinute = Math.floor(Date.now() / 60000);
    
    // Cost tracking
    this.totalInputTokens = 0;
    this.totalOutputTokens = 0;
    this.totalCost = 0;
    this.totalRequests = 0;
  }

  // Load the classification prompt
  loadPrompt() {
    try {
      this.systemPrompt = fs.readFileSync(PROMPT_FILE, 'utf8');
      console.log('✅ Loaded classification prompt');
    } catch (error) {
      console.error('❌ Error loading prompt file:', error.message);
      process.exit(1);
    }
  }

  // Get recent sessions
  async getRecentSessions(limit = 5) {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT 
          s.id,
          s.label,
          s.activity_date,
          s.type,
          COUNT(i.id) as speech_count
        FROM sittings s
        LEFT JOIN individual_speeches i ON s.id = i.sitting_id
        WHERE s.activity_date IS NOT NULL
        GROUP BY s.id, s.label, s.activity_date, s.type
        ORDER BY s.activity_date DESC
        LIMIT ?
      `;

      this.db.all(query, [limit], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Get speeches from a specific session
  async getSpeechesFromSession(sittingId) {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT 
          i.id,
          i.speaker_name,
          i.political_group,
          i.language,
          i.speech_content,
          i.topic as existing_topic,
          s.activity_date,
          s.label as sitting_label
        FROM individual_speeches i
        JOIN sittings s ON i.sitting_id = s.id
        WHERE i.sitting_id = ?
        AND i.speech_content IS NOT NULL 
        AND length(i.speech_content) > 50
        ORDER BY i.speech_order, i.id
      `;

      this.db.all(query, [sittingId], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Rate limiting
  async rateLimit() {
    const currentMinute = Math.floor(Date.now() / 60000);
    
    if (currentMinute !== this.lastMinute) {
      this.requestCount = 0;
      this.tokenCount = 0;
      this.lastMinute = currentMinute;
    }
    
    if (this.requestCount >= MAX_RPM * 0.9) {
      const waitTime = 60000 - (Date.now() % 60000);
      console.log(`\n⏳ Rate limit approaching, waiting ${Math.round(waitTime/1000)}s...`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
      this.requestCount = 0;
      this.tokenCount = 0;
    }
    
    await new Promise(resolve => setTimeout(resolve, REQUEST_DELAY));
  }

  // Format speech for API input
  formatSpeechInput(speech) {
    return [
      `Speaker: ${speech.speaker_name || 'Unknown'}`,
      `Political Group: ${speech.political_group || 'Unknown'}`,
      `Language: ${speech.language || 'EN'}`,
      `Speech:`,
      '```',
      speech.speech_content,
      '```'
    ].join('\n');
  }

  // Classify a single speech
  async classifySpeech(speech, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        await this.rateLimit();
        
        const userInput = this.formatSpeechInput(speech);
        
        const response = await this.openai.chat.completions.create({
          model: MODEL,
          messages: [
            { role: 'system', content: this.systemPrompt },
            { role: 'user', content: userInput }
          ]
        });

        const classification = response.choices[0]?.message?.content?.trim() || 'Unknown';
        
        // Track usage
        this.requestCount++;
        this.totalRequests++;
        
        const inputTokens = response.usage?.prompt_tokens || 0;
        const outputTokens = response.usage?.completion_tokens || 0;
        const reasoningTokens = response.usage?.completion_tokens_details?.reasoning_tokens || 0;
        
        this.tokenCount += inputTokens + outputTokens;
        this.totalInputTokens += inputTokens;
        this.totalOutputTokens += outputTokens;
        
        const inputCost = (inputTokens / 1000000) * INPUT_COST_PER_1M;
        const outputCost = (outputTokens / 1000000) * OUTPUT_COST_PER_1M;
        this.totalCost += inputCost + outputCost;

        return {
          id: speech.id,
          speaker: speech.speaker_name,
          group: speech.political_group,
          language: speech.language,
          existingTopic: speech.existing_topic,
          classifiedTopic: classification,
          inputTokens,
          outputTokens,
          reasoningTokens,
          cost: inputCost + outputCost,
          speechLength: speech.speech_content.length,
          attempt
        };

      } catch (error) {
        console.error(`❌ Attempt ${attempt} failed for speech ${speech.id}:`, error.message);
        
        if (attempt === retries) {
          return {
            id: speech.id,
            speaker: speech.speaker_name,
            group: speech.political_group,
            language: speech.language,
            existingTopic: speech.existing_topic,
            classifiedTopic: 'ERROR',
            inputTokens: 0,
            outputTokens: 0,
            reasoningTokens: 0,
            cost: 0,
            speechLength: speech.speech_content.length,
            error: error.message,
            attempt
          };
        }
        
        await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
      }
    }
  }

  // Update progress bar
  updateProgress(current, total, currentTopic, currentSpeaker, sessionName) {
    if (!this.progressBar) {
      this.progressBar = new cliProgress.SingleBar({
        format: '🚀 {sessionName} |{bar}| {percentage}% | {value}/{total} | {currentTopic} | {currentSpeaker} | ${cost}',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true
      });
      this.progressBar.start(total, 0, {
        sessionName: sessionName || 'Starting...',
        currentTopic: 'Starting...',
        currentSpeaker: '',
        cost: '0.00'
      });
    }
    
    this.progressBar.update(current, {
      sessionName: sessionName || 'Processing...',
      currentTopic: currentTopic || 'Processing...',
      currentSpeaker: currentSpeaker || '',
      cost: this.totalCost.toFixed(4)
    });
  }

  // Process speeches in batches
  async processBatch(speeches, startIndex, sessionName) {
    const batch = speeches.slice(startIndex, startIndex + BATCH_SIZE);
    const promises = batch.map(speech => this.classifySpeech(speech));
    
    const results = await Promise.all(promises);
    
    results.forEach((result, index) => {
      const globalIndex = startIndex + index;
      this.updateProgress(
        globalIndex + 1, 
        speeches.length, 
        result.classifiedTopic,
        result.speaker,
        sessionName
      );
    });
    
    return results;
  }

  // Save results to database
  async saveResultsToDatabase() {
    console.log('\n💾 Saving classified topics to database...');
    
    return new Promise((resolve, reject) => {
      // Ensure columns exist
      this.db.exec(`
        ALTER TABLE individual_speeches ADD COLUMN classified_topic TEXT;
        ALTER TABLE individual_speeches ADD COLUMN topic_classified_by TEXT;
        ALTER TABLE individual_speeches ADD COLUMN topic_classified_at INTEGER;
        ALTER TABLE individual_speeches ADD COLUMN topic_classification_cost REAL;
      `, (err) => {
        // Ignore errors (columns might already exist)
      });
      
      const stmt = this.db.prepare(`
        UPDATE individual_speeches 
        SET classified_topic = ?, 
            topic_classified_by = 'gpt-5-nano-2025-08-07',
            topic_classified_at = strftime('%s', 'now'),
            topic_classification_cost = ?
        WHERE id = ?
      `);
      
      let completed = 0;
      let errors = 0;
      
      this.results.forEach(result => {
        stmt.run([result.classifiedTopic, result.cost, result.id], (err) => {
          if (err) {
            console.error(`❌ Error saving speech ${result.id}:`, err.message);
            errors++;
          }
          completed++;
          
          if (completed === this.results.length) {
            stmt.finalize();
            console.log(`✅ Saved ${completed - errors} topics to database`);
            if (errors > 0) {
              console.log(`⚠️  ${errors} errors occurred during save`);
            }
            resolve();
          }
        });
      });
    });
  }

  // Print session results
  printSessionResults(session, speeches) {
    this.progressBar?.stop();
    
    console.log('\n' + '='.repeat(80));
    console.log(`📊 SESSION RESULTS: ${session.label}`);
    console.log(`📅 Date: ${session.activity_date}`);
    console.log('='.repeat(80));

    // Basic stats
    console.log(`\n📈 Processing Statistics:`);
    console.log(`   Total speeches processed: ${this.results.length}`);
    console.log(`   Successful classifications: ${this.results.filter(r => r.classifiedTopic !== 'ERROR').length}`);
    console.log(`   Failed classifications: ${this.results.filter(r => r.classifiedTopic === 'ERROR').length}`);

    // Topic distribution
    console.log(`\n🏷️  Topic Distribution:`);
    const topicCounts = {};
    this.results.forEach(r => {
      if (r.classifiedTopic !== 'ERROR') {
        topicCounts[r.classifiedTopic] = (topicCounts[r.classifiedTopic] || 0) + 1;
      }
    });

    Object.entries(topicCounts)
      .sort((a, b) => b[1] - a[1])
      .forEach(([topic, count]) => {
        const percentage = ((count / this.results.length) * 100).toFixed(1);
        console.log(`   ${topic}: ${count} speeches (${percentage}%)`);
      });

    // Sample results
    console.log(`\n📝 Sample Classifications:`);
    console.log('ID | Speaker | Original Topic | Classified Topic | Length');
    console.log('-'.repeat(80));
    
    this.results.slice(0, 10).forEach(r => {
      const id = r.id.toString().padEnd(3);
      const speaker = (r.speaker || 'Unknown').substring(0, 15).padEnd(16);
      const original = (r.existingTopic || 'None').substring(0, 20).padEnd(21);
      const classified = (r.classifiedTopic || 'ERROR').substring(0, 20).padEnd(21);
      const length = r.speechLength.toString().padEnd(6);
      console.log(`${id} | ${speaker} | ${original} | ${classified} | ${length}`);
    });

    if (this.results.length > 10) {
      console.log(`... and ${this.results.length - 10} more speeches`);
    }
  }

  // Process a single session
  async processSession(session) {
    console.log(`\n🎯 Processing Session: ${session.label}`);
    console.log(`📅 Date: ${session.activity_date}`);
    console.log(`📊 Expected speeches: ${session.speech_count}`);
    
    // Get speeches for this session
    const speeches = await this.getSpeechesFromSession(session.id);
    console.log(`✅ Found ${speeches.length} speeches to classify`);
    
    if (speeches.length === 0) {
      console.log('⚠️  No speeches found for this session');
      return;
    }

    // Reset progress bar for new session
    this.progressBar = null;
    this.results = [];

    // Process speeches in batches
    for (let i = 0; i < speeches.length; i += BATCH_SIZE) {
      const batchResults = await this.processBatch(speeches, i, session.label);
      this.results.push(...batchResults);
    }

    // Save results to database
    await this.saveResultsToDatabase();

    // Print results
    this.printSessionResults(session, speeches);

    return this.results;
  }

  // Main execution
  async run(numberOfSessions = 5) {
    this.startTime = Date.now();
    
    console.log('🚀 EUROWATCH Session-Based Speech Classification');
    console.log('===============================================\n');

    // Check API key
    if (!process.env.OPENAI_API_KEY) {
      console.error('❌ Error: OPENAI_API_KEY environment variable not set');
      process.exit(1);
    }

    try {
      // Load prompt
      this.loadPrompt();

      // Get recent sessions
      console.log(`📥 Fetching ${numberOfSessions} most recent sessions...`);
      const sessions = await this.getRecentSessions(numberOfSessions);
      console.log(`✅ Found ${sessions.length} sessions to process`);

      if (sessions.length === 0) {
        console.log('❌ No sessions found. Exiting.');
        return;
      }

      // Process each session
      let totalSpeeches = 0;
      let totalCost = 0;

      for (let i = 0; i < sessions.length; i++) {
        const session = sessions[i];
        console.log(`\n📋 Session ${i + 1}/${sessions.length}: ${session.label}`);
        
        const sessionResults = await this.processSession(session);
        if (sessionResults) {
          totalSpeeches += sessionResults.length;
          totalCost += sessionResults.reduce((sum, r) => sum + r.cost, 0);
        }
      }

      // Final summary
      console.log('\n' + '='.repeat(80));
      console.log('🎉 ALL SESSIONS COMPLETED');
      console.log('='.repeat(80));
      console.log(`📊 Total speeches processed: ${totalSpeeches}`);
      console.log(`💰 Total cost: $${totalCost.toFixed(4)}`);
      console.log(`⏱️  Total time: ${((Date.now() - this.startTime) / 1000 / 60).toFixed(1)} minutes`);
      console.log(`\n🌐 Check your web interface to see the new classified topics!`);

    } catch (error) {
      console.error('❌ Classification failed:', error);
    } finally {
      this.db.close();
    }
  }
}

// Parse command line arguments
const numberOfSessions = process.argv[2] ? parseInt(process.argv[2]) : 5;

// Run the classifier
const classifier = new SessionClassifier();
classifier.run(numberOfSessions).catch(console.error);
