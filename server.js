// Simple Express server for validation platform
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const Database = require('./database');
const GoogleSheetsService = require('./google-sheets');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(express.static('public'));

// Database setup (for slice loading and assignments)
const db = new Database();

// Google Sheets setup (for annotation saving)
const sheetsService = new GoogleSheetsService();

// Initialize database tables and load data
(async () => {
  try {
    console.log('Starting database initialization...');
    await db.initialize();
    console.log('Database initialization completed successfully');
    
    // Check if we need to load slice data
    const result = await db.get('SELECT COUNT(*) as count FROM slices');
    console.log(`Database currently contains ${result.count} slices`);
    
    if (result.count === 0) {
      console.log('No slices found, running database setup...');
      const { setupDatabase } = require('./setup-database');
      await setupDatabase();
      console.log('Database setup completed during startup');
    }
  } catch (error) {
    console.error('Database initialization error:', error);
    console.error('Stack trace:', error.stack);
  }
})();

// Load balancing: Get assignment counts for each slice
async function getSliceCounts() {
  try {
    const rows = await db.query(`
      SELECT slice_id, COUNT(*) as count 
      FROM assignments 
      GROUP BY slice_id
    `);
    const counts = {};
    rows.forEach(row => {
      counts[row.slice_id] = row.count;
    });
    return counts;
  } catch (error) {
    throw error;
  }
}

// Get all slice IDs
async function getAllSliceIds() {
  try {
    const rows = await db.query('SELECT id FROM slices');
    return rows.map(row => row.id);
  } catch (error) {
    throw error;
  }
}

// Shuffle array function
function shuffleArray(array) {
  const shuffled = [...array];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}

// API Endpoints

// 1. Get assigned slices for a participant
app.get('/api/participant/:id/slices', async (req, res) => {
  const participantId = req.params.id;
  const SLICES_PER_PARTICIPANT = 15;

  try {
    console.log(`Loading slices for participant: ${participantId}`);
    
    // Check if participant already has assignments
    const existing = await db.query(
      'SELECT slice_id FROM assignments WHERE participant_id = ?',
      [participantId]
    );
    
    console.log(`Found ${existing.length} existing assignments for participant ${participantId}`);

    let assignedSliceIds;

    if (existing.length > 0) {
      // Return existing assignments
      assignedSliceIds = existing.map(row => row.slice_id);
    } else {
      // Create new assignments using load balancing
      const allSliceIds = await getAllSliceIds();
      const sliceCounts = await getSliceCounts();

      // Initialize counts for slices that haven't been assigned yet
      allSliceIds.forEach(id => {
        if (!(id in sliceCounts)) {
          sliceCounts[id] = 0;
        }
      });

      // Sort slices by count (least assigned first), then shuffle within equal counts
      const sortedSlices = allSliceIds.sort((a, b) => {
        const countDiff = sliceCounts[a] - sliceCounts[b];
        if (countDiff === 0) {
          // Equal counts - randomize order
          return Math.random() - 0.5;
        }
        return countDiff;
      });

      // Take the first SLICES_PER_PARTICIPANT slices (least assigned)
      assignedSliceIds = sortedSlices.slice(0, SLICES_PER_PARTICIPANT);

      // Insert assignments
      for (const sliceId of assignedSliceIds) {
        await db.run(
          'INSERT INTO assignments (participant_id, slice_id) VALUES (?, ?)',
          [participantId, sliceId]
        );
      }
    }

    // Fetch slice details
    const placeholders = assignedSliceIds.map(() => '?').join(',');
    const sliceRows = await db.query(
      `SELECT * FROM slices WHERE id IN (${placeholders})`,
      assignedSliceIds
    );

    // Parse JSON fields and maintain order
    const slicesData = assignedSliceIds.map(id => {
      const slice = sliceRows.find(s => s.id === id);
      return {
        ...slice,
        focus_turns: JSON.parse(slice.focus_turns || '[]'),
        hybrid_predictions: JSON.parse(slice.hybrid_predictions || '{}')
      };
    });

    res.json({
      participant_id: participantId,
      slices: slicesData,
      total: slicesData.length
    });
  } catch (error) {
    console.error('Error in /api/participant/:id/slices:', error);
    res.status(500).json({ error: 'Server error' });
  }
});

// 2. Submit annotation
app.post('/api/annotations', async (req, res) => {
  console.log('Received annotation submission:', req.body);
  
  const {
    participant_id,
    slice_id,
    interaction_types,
    curiosity_types,
    routing_validation,
    annotation_time_seconds
  } = req.body;

  // Validate required fields - allow empty interaction_types array
  if (!participant_id || !slice_id || interaction_types === undefined) {
    console.log('Missing required fields:', { participant_id, slice_id, interaction_types });
    return res.status(400).json({ error: 'Missing required fields' });
  }
  
  // Log if submission is empty (user didn't select anything)
  if (interaction_types.length === 0 && (!curiosity_types || curiosity_types.length === 0)) {
    console.log('Empty submission received (no selections made)');
  }
  
  console.log('Validated fields, attempting to save annotation...');

  try {
    const annotation = {
      participant_id,
      slice_id,
      interaction_types,
      curiosity_types: curiosity_types || [],
      routing_validation: routing_validation || {},
      annotation_time_seconds: annotation_time_seconds || 0
    };

    // Try to save to Google Sheets first
    const sheetsSaved = await sheetsService.saveAnnotation(annotation);
    
    // Also save to local database as backup
    try {
      await db.run(`
        INSERT INTO annotations 
        (participant_id, slice_id, interaction_types, curiosity_types, routing_validation, annotation_time_seconds)
        VALUES (?, ?, ?, ?, ?, ?)
      `, [
        participant_id,
        slice_id,
        JSON.stringify(interaction_types),
        JSON.stringify(curiosity_types || []),
        JSON.stringify(routing_validation || {}),
        annotation_time_seconds || 0
      ]);
      console.log('Annotation saved to local database as backup');
    } catch (dbError) {
      console.error('Database backup failed:', dbError.message);
    }

    if (sheetsSaved) {
      console.log('Annotation saved successfully to Google Sheets');
    } else {
      console.log('Google Sheets save failed, but local backup saved');
    }
    
    res.json({
      success: true,
      message: 'Annotation saved successfully',
      saved_to_sheets: sheetsSaved
    });
  } catch (error) {
    console.error('Error saving annotation:', error);
    console.error('Error details:', error.message);
    res.status(500).json({ error: 'Failed to save annotation' });
  }
});

// 3. Export data as CSV
app.get('/api/export', async (req, res) => {
  try {
    console.log('Export endpoint called');
    
    // First check if we have any annotations at all
    const annotationCount = await db.get('SELECT COUNT(*) as count FROM annotations');
    console.log(`Total annotations in database: ${annotationCount.count}`);
    
    // Check if we have any slices
    const sliceCount = await db.get('SELECT COUNT(*) as count FROM slices');
    console.log(`Total slices in database: ${sliceCount.count}`);
    
    // Check if we have any assignments
    const assignmentCount = await db.get('SELECT COUNT(*) as count FROM assignments');
    console.log(`Total assignments in database: ${assignmentCount.count}`);
    
    const rows = await db.query(`
      SELECT 
        a.participant_id,
        a.slice_id,
        a.interaction_types,
        a.curiosity_types,
        a.routing_validation,
        a.annotation_time_seconds,
        a.submitted_at,
        s.conversation_id
      FROM annotations a
      JOIN assignments ass ON a.participant_id = ass.participant_id AND a.slice_id = ass.slice_id
      JOIN slices s ON a.slice_id = s.id
      ORDER BY a.participant_id, a.submitted_at
    `);
    
    console.log(`Export query returned ${rows.length} rows`);

    // Convert to CSV
    const headers = [
      'participant_id',
      'slice_id', 
      'conversation_id',
      'interaction_types',
      'curiosity_types',
      'routing_validation',
      'annotation_time_seconds',
      'submitted_at'
    ];

    let csv = headers.join(',') + '\n';
    
    rows.forEach(row => {
      const csvRow = [
        row.participant_id,
        row.slice_id,
        row.conversation_id,
        `"${row.interaction_types}"`,
        `"${row.curiosity_types}"`,
        `"${row.routing_validation}"`,
        row.annotation_time_seconds,
        row.submitted_at
      ];
      csv += csvRow.join(',') + '\n';
    });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="validation_data.csv"');
    res.send(csv);
  } catch (error) {
    console.error('Export error:', error);
    res.status(500).json({ error: 'Export failed' });
  }
});

// 4. Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// 5. Manual setup trigger (for debugging)
app.get('/api/setup', async (req, res) => {
  try {
    const { setupDatabase } = require('./setup-database');
    await setupDatabase();
    res.json({ success: true, message: 'Database setup completed' });
  } catch (error) {
    console.error('Manual setup failed:', error);
    res.status(500).json({ error: 'Setup failed', details: error.message });
  }
});

// 7. Setup Google Sheets headers
app.get('/api/setup-sheets', async (req, res) => {
  try {
    const success = await sheetsService.setupHeaders();
    if (success) {
      res.json({ success: true, message: 'Google Sheets headers set up successfully' });
    } else {
      res.json({ success: false, message: 'Google Sheets not configured or failed to set up headers' });
    }
  } catch (error) {
    console.error('Sheets setup failed:', error);
    res.status(500).json({ error: 'Sheets setup failed', details: error.message });
  }
});

// 6. Check slice count (for debugging)
app.get('/api/debug/slices', async (req, res) => {
  try {
    const result = await db.get('SELECT COUNT(*) as count FROM slices');
    res.json({ slice_count: result.count });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Validation server running on port ${PORT}`);
});