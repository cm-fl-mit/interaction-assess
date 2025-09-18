// Setup script to load slice data into database
const Database = require('./database');
const fs = require('fs');

const db = new Database();

// Initialize database tables first
async function initializeDatabase() {
  try {
    await db.initialize();
    console.log('Database tables initialized');
  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  }
}

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
  console.log('Creating 40 sample slices for validation...');
  
  const sampleSlices = [];
  
  for (let i = 1; i <= 40; i++) {
    sampleSlices.push({
      id: i,
      conversation_id: `sample_conversation_${Math.ceil(i/3)}`,
      context: `Context for slice ${i}...`,
      focus_turn: {
        speaker: i % 2 === 0 ? "A" : "B",
        text: `Sample turn from speaker ${i % 2 === 0 ? "A" : "B"} in slice ${i}`,
        turn: i
      },
      focus_turns: [
        {
          speaker: i % 2 === 0 ? "A" : "B", 
          text: `Sample turn from speaker ${i % 2 === 0 ? "A" : "B"} in slice ${i}`,
          turn: i
        },
        {
          speaker: i % 2 === 0 ? "B" : "A",
          text: `Sample response from speaker ${i % 2 === 0 ? "B" : "A"} in slice ${i}`,
          turn: i + 1
        }
      ],
      interaction_types: [
        i % 3 === 0 ? "agreeing" : i % 3 === 1 ? "disagreeing" : "explaining"
      ],
      hybrid_predictions: {
        interaction_types: [
          {
            type: i % 3 === 0 ? "agreeing" : i % 3 === 1 ? "disagreeing" : "explaining",
            confidence: 0.7 + Math.random() * 0.3,
            source: "pattern"
          }
        ],
        curiosity_types: [
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
async function insertSlices(slices) {
  try {
    // Clear existing data
    await db.run('DELETE FROM annotations');
    await db.run('DELETE FROM assignments'); 
    await db.run('DELETE FROM slices');

    console.log('Inserting slices into database...');

    // Insert new slices
    for (let i = 0; i < slices.length; i++) {
      const slice = slices[i];
      
      // Ensure unique slice IDs by using validation prefix
      const sliceId = `validation_${slice.id || (i + 1).toString().padStart(2, '0')}`;
      const conversationId = slice.conversation_id || `conv_${Math.floor(i/3) + 1}`;
      
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
      } else if (slice.turns && Array.isArray(slice.turns)) {
        // Another old format
        focusTurns = slice.turns;
      } else {
        // Text format - parse it
        focusTurns = parseTextIntoTurns(slice.text || '');
      }
      
      // Handle hybrid predictions
      let hybridPredictions = slice.hybrid_predictions || {};
      if (slice.model_predictions) {
        hybridPredictions = slice.model_predictions;
      }
      
      await db.run(`
        INSERT INTO slices (id, conversation_id, context, focus_turns, hybrid_predictions)
        VALUES (?, ?, ?, ?, ?)
      `, [
        sliceId,
        conversationId,
        context,
        JSON.stringify(focusTurns),
        JSON.stringify(hybridPredictions)
      ]);
    }

    console.log(`Inserted ${slices.length} slices into database`);
    return slices.length;
  } catch (error) {
    console.error('Error inserting slices:', error);
    throw error;
  }
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
    
    await initializeDatabase();
    
    const slices = loadSlicesFromFile();
    console.log(`Loaded ${slices.length} slices`);
    
    const insertedCount = await insertSlices(slices);
    console.log(`Inserted ${insertedCount} slices into database`);
    
    // Verify setup
    const result = await db.get('SELECT COUNT(*) as count FROM slices');
    console.log(`Database now contains ${result.count} slices`);
    
    console.log('Database setup complete!');
    
  } catch (error) {
    console.error('Setup failed:', error);
    process.exit(1);
  }
}

// Run setup if called directly
if (require.main === module) {
  setupDatabase();
}

module.exports = { setupDatabase, loadSlicesFromFile };