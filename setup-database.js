// Setup script to load slice data into SQLite database
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');

const db = new sqlite3.Database('validation.db');

// Initialize database tables first
db.serialize(() => {
  // Create tables if they don't exist
  db.run(`CREATE TABLE IF NOT EXISTS slices (
    id TEXT PRIMARY KEY,
    conversation_id TEXT,
    context TEXT,
    focus_turns TEXT,
    hybrid_predictions TEXT
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS assignments (
    participant_id TEXT,
    slice_id TEXT,
    assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (participant_id, slice_id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS annotations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    participant_id TEXT,
    slice_id TEXT,
    interaction_types TEXT,
    curiosity_types TEXT,
    routing_validation TEXT,
    annotation_time_seconds INTEGER,
    submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

// Load your slice data from JSON file
function loadSlicesFromFile() {
  try {
    // First try to load the new rebuilt conversation slices
    if (fs.existsSync('Conv_slices_rebuilt_updated.json')) {
      console.log('Loading conversation slices from Conv_slices_rebuilt_updated.json...');
      const data = JSON.parse(fs.readFileSync('Conv_slices_rebuilt_updated.json', 'utf8'));
      
      if (data.slices) {
        console.log(`Loaded ${data.slices.length} rebuilt conversation slices`);
        return data.slices;
      }
    }

    // Fallback to original rebuilt file
    if (fs.existsSync('Conv_slices_rebuilt.json')) {
      console.log('Loading conversation slices from Conv_slices_rebuilt.json...');
      const data = JSON.parse(fs.readFileSync('Conv_slices_rebuilt.json', 'utf8'));
      
      if (data.slices) {
        console.log(`Loaded ${data.slices.length} rebuilt conversation slices`);
        return data.slices;
      }
    }

    // Fallback to validation slices
    if (fs.existsSync('validation_slices_content.json')) {
      console.log('Loading validation slices from validation_slices_content.json...');
      const contentData = JSON.parse(fs.readFileSync('validation_slices_content.json', 'utf8'));
      const assessmentData = JSON.parse(fs.readFileSync('validation_slices_assessments.json', 'utf8'));
      
      if (contentData.slices && assessmentData.assessments) {
        // Merge content and assessments by ID
        const mergedSlices = contentData.slices.map(slice => {
          const assessment = assessmentData.assessments.find(a => a.id === slice.id);
          return {
            ...slice,
            hybrid_predictions: assessment ? assessment.hybrid_predictions : {},
            validation_priority: assessment ? assessment.validation_priority : 0.5
          };
        });
        
        console.log(`Loaded ${mergedSlices.length} 1-turn validation slices with hybrid predictions`);
        return mergedSlices;
      }
    }

    // Fallback to old format
    if (fs.existsSync('validation_slices.json')) {
      console.log('Loading validation slices from validation_slices.json...');
      const data = JSON.parse(fs.readFileSync('validation_slices.json', 'utf8'));
      if (data.slices) {
        console.log(`Loaded ${data.slices.length} validation slices with hybrid predictions`);
        return data.slices;
      }
    }

    // Fallback to original slice files
    const sliceFiles = [
      'conv_slices_03.json',
      'conversation_slices_2party.json'
    ];

    let allSlices = [];

    sliceFiles.forEach(filename => {
      if (fs.existsSync(filename)) {
        console.log(`Loading slices from ${filename}...`);
        const data = JSON.parse(fs.readFileSync(filename, 'utf8'));
        
        if (data.slices) {
          allSlices = allSlices.concat(data.slices);
        } else if (Array.isArray(data)) {
          allSlices = allSlices.concat(data);
        }
      }
    });

    if (allSlices.length === 0) {
      console.log('No slice files found. Creating sample data...');
      allSlices = createSampleSlices();
    }

    return allSlices;
  } catch (error) {
    console.error('Error loading slice files:', error);
    console.log('Creating sample data instead...');
    return createSampleSlices();
  }
}

// Create sample slices if no data files exist
function createSampleSlices() {
  const sampleSlices = [];
  
  for (let i = 1; i <= 36; i++) {
    sampleSlices.push({
      id: `slice_${i.toString().padStart(2, '0')}`,
      conversation_id: `conv_${Math.floor(i/3) + 1}`,
      context: i === 1 ? "None (start of recorded conversation)" : `Context for slice ${i}...`,
      text: `Sample conversation text for slice ${i}.\n\nSpeaker A: This is sample text for testing.\n\nSpeaker B: This is a response that shows different interaction patterns.`,
      focus_turns: [
        {
          speaker: "A",
          text: `Sample turn from speaker A in slice ${i}`
        },
        {
          speaker: "B", 
          text: `Sample response from speaker B in slice ${i}`
        }
      ],
      hybrid_predictions: {
        interaction_types: [
          {
            type: "questioning",
            confidence: 0.7 + Math.random() * 0.3,
            source: "pattern"
          }
        ],
        routing_reason: i % 5 === 0 ? "ambiguous_patterns" : "high_confidence_pattern"
      }
    });
  }

  return sampleSlices;
}

// Insert slices into database
function insertSlices(slices) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // Clear existing data
      db.run('DELETE FROM annotations');
      db.run('DELETE FROM assignments'); 
      db.run('DELETE FROM slices');

      // Insert new slices
      const stmt = db.prepare(`
        INSERT INTO slices (id, conversation_id, context, focus_turns, hybrid_predictions)
        VALUES (?, ?, ?, ?, ?)
      `);

      slices.forEach((slice, index) => {
        // Ensure unique slice IDs by using validation prefix
        const sliceId = `validation_${slice.id || (index + 1).toString().padStart(2, '0')}`;
        const conversationId = slice.conversation_id || `conv_${Math.floor(index/3) + 1}`;
        
        // Handle context - validation slices may have null context
        let context = slice.context;
        if (!context && slice.text) {
          context = slice.text;
        }
        if (context === "None (start of recorded conversation)") {
          context = null;
        }
        
        // Handle focus_turns - new format has single focus_turn, old format has array
        let focusTurns;
        if (slice.focus_turn) {
          // New 1-turn format
          focusTurns = [slice.focus_turn];
        } else if (slice.focus_turns && Array.isArray(slice.focus_turns)) {
          // Old 3-turn format
          focusTurns = slice.focus_turns;
        } else if (slice.text) {
          // Parse text into turns for older format
          focusTurns = parseTextIntoTurns(slice.text);
        } else {
          focusTurns = [];
        }

        // Handle hybrid predictions or interaction types - support both formats
        let hybridPredictions = slice.hybrid_predictions || {};
        
        // Convert new format (interaction_types array) to hybrid predictions format for database
        if (slice.interaction_types && Array.isArray(slice.interaction_types)) {
          hybridPredictions = {
            interaction_types: slice.interaction_types.map(type => ({
              type: type,
              confidence: 0.8, // Default confidence for curated slices
              source: 'curated'
            })),
            curiosity_types: [],
            routing_reason: 'curated_conversation',
            llm_routed: false,
            pattern_confidence: 0.8
          };
        }

        stmt.run(
          sliceId,
          conversationId,
          context || '',
          JSON.stringify(focusTurns),
          JSON.stringify(hybridPredictions)
        );
      });

      stmt.finalize((err) => {
        if (err) {
          reject(err);
        } else {
          resolve(slices.length);
        }
      });
    });
  });
}

// Helper function to parse text format into structured turns
function parseTextIntoTurns(text) {
  const lines = text.split('\n\n');
  const turns = [];
  
  lines.forEach(line => {
    const colonIndex = line.indexOf(':');
    if (colonIndex > 0) {
      const speaker = line.substring(0, colonIndex).trim();
      const utterance = line.substring(colonIndex + 1).trim();
      turns.push({
        speaker: speaker,
        text: utterance
      });
    }
  });
  
  return turns;
}

// Main setup function
async function setupDatabase() {
  try {
    console.log('Setting up validation database...');
    
    const slices = loadSlicesFromFile();
    console.log(`Loaded ${slices.length} slices`);
    
    const insertedCount = await insertSlices(slices);
    console.log(`Inserted ${insertedCount} slices into database`);
    
    // Verify setup
    db.get('SELECT COUNT(*) as count FROM slices', (err, row) => {
      if (err) {
        console.error('Error verifying setup:', err);
      } else {
        console.log(`Database setup complete. Total slices: ${row.count}`);
      }
      
      db.close();
    });
    
  } catch (error) {
    console.error('Setup failed:', error);
    db.close();
  }
}

// Run setup if called directly
if (require.main === module) {
  setupDatabase();
}

module.exports = { setupDatabase, loadSlicesFromFile };