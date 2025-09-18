// Database abstraction layer - supports both SQLite and PostgreSQL
const sqlite3 = require('sqlite3').verbose();
const { Pool } = require('pg');

class Database {
  constructor() {
    // For now, always use SQLite to avoid SSL issues
    // TODO: Fix PostgreSQL SSL connection later
    this.type = 'sqlite';
    
    // Use persistent path if in production, otherwise local
    const dbPath = process.env.NODE_ENV === 'production' ? '/tmp/validation.db' : 'validation.db';
    this.db = new sqlite3.Database(dbPath);
    console.log(`Using SQLite database at: ${dbPath}`);
    
    /* Commented out PostgreSQL for now due to SSL issues
    if (process.env.DATABASE_URL) {
      // Use PostgreSQL for production
      this.type = 'postgres';
      
      // Try to completely disable SSL verification
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
      
      this.db = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: false
      });
      console.log('Using PostgreSQL database with SSL completely disabled');
    } else {
      // Use SQLite for local development
      this.type = 'sqlite';
      this.db = new sqlite3.Database('validation.db');
      console.log('Using SQLite database');
    }
    */
  }

  // Execute a query
  async query(sql, params = []) {
    if (this.type === 'postgres') {
      // Convert ? placeholders to $1, $2, etc. for PostgreSQL
      let pgSql = sql;
      let paramIndex = 1;
      pgSql = pgSql.replace(/\?/g, () => `$${paramIndex++}`);
      
      const result = await this.db.query(pgSql, params);
      return result.rows;
    } else {
      return new Promise((resolve, reject) => {
        this.db.all(sql, params, (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        });
      });
    }
  }

  // Execute a single row query
  async get(sql, params = []) {
    if (this.type === 'postgres') {
      // Convert ? placeholders to $1, $2, etc. for PostgreSQL
      let pgSql = sql;
      let paramIndex = 1;
      pgSql = pgSql.replace(/\?/g, () => `$${paramIndex++}`);
      
      const result = await this.db.query(pgSql, params);
      return result.rows[0];
    } else {
      return new Promise((resolve, reject) => {
        this.db.get(sql, params, (err, row) => {
          if (err) reject(err);
          else resolve(row);
        });
      });
    }
  }

  // Execute an insert/update/delete
  async run(sql, params = []) {
    if (this.type === 'postgres') {
      // Convert ? placeholders to $1, $2, etc. for PostgreSQL
      let pgSql = sql;
      let paramIndex = 1;
      pgSql = pgSql.replace(/\?/g, () => `$${paramIndex++}`);
      
      const result = await this.db.query(pgSql, params);
      return { changes: result.rowCount, lastID: result.insertId };
    } else {
      return new Promise((resolve, reject) => {
        this.db.run(sql, params, function(err) {
          if (err) reject(err);
          else resolve({ changes: this.changes, lastID: this.lastID });
        });
      });
    }
  }

  // Initialize database tables
  async initialize() {
    const createSlicesTable = this.type === 'postgres' ?
      `CREATE TABLE IF NOT EXISTS slices (
        id TEXT PRIMARY KEY,
        conversation_id TEXT,
        context TEXT,
        focus_turns TEXT,
        hybrid_predictions TEXT
      )` :
      `CREATE TABLE IF NOT EXISTS slices (
        id TEXT PRIMARY KEY,
        conversation_id TEXT,
        context TEXT,
        focus_turns TEXT,
        hybrid_predictions TEXT
      )`;

    const createAssignmentsTable = this.type === 'postgres' ?
      `CREATE TABLE IF NOT EXISTS assignments (
        participant_id TEXT,
        slice_id TEXT,
        assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (participant_id, slice_id)
      )` :
      `CREATE TABLE IF NOT EXISTS assignments (
        participant_id TEXT,
        slice_id TEXT,
        assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (participant_id, slice_id)
      )`;

    const createAnnotationsTable = this.type === 'postgres' ?
      `CREATE TABLE IF NOT EXISTS annotations (
        id SERIAL PRIMARY KEY,
        participant_id TEXT,
        slice_id TEXT,
        interaction_types TEXT,
        curiosity_types TEXT,
        routing_validation TEXT,
        annotation_time_seconds INTEGER,
        submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )` :
      `CREATE TABLE IF NOT EXISTS annotations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        participant_id TEXT,
        slice_id TEXT,
        interaction_types TEXT,
        curiosity_types TEXT,
        routing_validation TEXT,
        annotation_time_seconds INTEGER,
        submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`;

    await this.run(createSlicesTable);
    await this.run(createAssignmentsTable);
    await this.run(createAnnotationsTable);
    
    console.log('Database tables initialized');
  }
}

module.exports = Database;