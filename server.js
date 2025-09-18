// Simple Express server for validation platform
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(express.static('public'));

// Database setup - use persistent volume if available, otherwise local
const dbPath = process.env.NODE_ENV === 'production' ? '/data/validation.db' : 'validation.db';
const db = new sqlite3.Database(dbPath);

// Initialize database tables
db.serialize(() => {
  // Slices table
  db.run(`CREATE TABLE IF NOT EXISTS slices (
    id TEXT PRIMARY KEY,
    conversation_id TEXT,
    context TEXT,
    focus_turns TEXT,
    hybrid_predictions TEXT
  )`);

  // Assignments table - tracks which slices are assigned to which participants
  db.run(`CREATE TABLE IF NOT EXISTS assignments (
    participant_id TEXT,
    slice_id TEXT,
    assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (participant_id, slice_id)
  )`);

  // Annotations table - stores participant responses
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

// Load balancing: Get assignment counts for each slice
function getSliceCounts() {
  return new Promise((resolve, reject) => {
    db.all(`
      SELECT slice_id, COUNT(*) as count 
      FROM assignments 
      GROUP BY slice_id
    `, (err, rows) => {
      if (err) reject(err);
      else {
        const counts = {};
        rows.forEach(row => {
          counts[row.slice_id] = row.count;
        });
        resolve(counts);
      }
    });
  });
}

// Get all slice IDs
function getAllSliceIds() {
  return new Promise((resolve, reject) => {
    db.all('SELECT id FROM slices', (err, rows) => {
      if (err) reject(err);
      else resolve(rows.map(row => row.id));
    });
  });
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
    // Check if participant already has assignments
    db.all(
      'SELECT slice_id FROM assignments WHERE participant_id = ?',
      [participantId],
      async (err, existing) => {
        if (err) {
          return res.status(500).json({ error: 'Database error' });
        }

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

          // Sort by least assigned first
          const sortedSlices = Object.keys(sliceCounts)
            .sort((a, b) => sliceCounts[a] - sliceCounts[b]);

          // Take first N slices and randomize order
          assignedSliceIds = shuffleArray(
            sortedSlices.slice(0, SLICES_PER_PARTICIPANT)
          );

          // Store assignments in database
          const stmt = db.prepare('INSERT INTO assignments (participant_id, slice_id) VALUES (?, ?)');
          assignedSliceIds.forEach(sliceId => {
            stmt.run(participantId, sliceId);
          });
          stmt.finalize();
        }

        // Get slice data for assigned slices
        const placeholders = assignedSliceIds.map(() => '?').join(',');
        db.all(
          `SELECT * FROM slices WHERE id IN (${placeholders})`,
          assignedSliceIds,
          (err, sliceRows) => {
            if (err) {
              return res.status(500).json({ error: 'Error fetching slices' });
            }

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
          }
        );
      }
    );
  } catch (error) {
    res.status(500).json({ error: 'Server error' });
  }
});

// 2. Submit annotation
app.post('/api/annotations', (req, res) => {
  const {
    participant_id,
    slice_id,
    interaction_types,
    curiosity_types,
    routing_validation,
    annotation_time_seconds
  } = req.body;

  // Validate required fields
  if (!participant_id || !slice_id || !interaction_types) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  // Insert annotation
  db.run(`
    INSERT INTO annotations 
    (participant_id, slice_id, interaction_types, curiosity_types, routing_validation, annotation_time_seconds)
    VALUES (?, ?, ?, ?, ?, ?)
  `, [
    participant_id,
    slice_id,
    JSON.stringify(interaction_types),
    JSON.stringify(curiosity_types || {}),
    JSON.stringify(routing_validation || {}),
    annotation_time_seconds || null
  ], function(err) {
    if (err) {
      console.error('Error inserting annotation:', err);
      return res.status(500).json({ error: 'Database error' });
    }

    res.json({
      id: this.lastID,
      status: 'success',
      message: 'Annotation saved'
    });
  });
});

// 3. Export data (simple CSV format)
app.get('/api/export', (req, res) => {
  db.all(`
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
  `, (err, rows) => {
    if (err) {
      return res.status(500).json({ error: 'Export failed' });
    }

    // Convert to CSV
    const csvRows = ['participant_id,slice_id,conversation_id,interaction_types,curiosity_types,routing_validation,annotation_time_seconds,submitted_at'];
    
    rows.forEach(row => {
      csvRows.push([
        row.participant_id,
        row.slice_id,
        row.conversation_id,
        `"${row.interaction_types}"`,
        `"${row.curiosity_types}"`,
        `"${row.routing_validation}"`,
        row.annotation_time_seconds || '',
        row.submitted_at
      ].join(','));
    });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=annotations.csv');
    res.send(csvRows.join('\n'));
  });
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Serve main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Access the application at http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down server...');
  db.close((err) => {
    if (err) {
      console.error('Error closing database:', err);
    } else {
      console.log('Database connection closed.');
    }
    process.exit(0);
  });
});