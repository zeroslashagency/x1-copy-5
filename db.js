const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

// Database configuration
const dbConfig = {
  user: 'postgres.sxnaopzgaddvziplrlbe',
  password: 'Qwerty@0073',
  host: 'aws-1-ap-south-1.pooler.supabase.com',
  port: 5432, // Using port 5432 which worked in the test
  database: 'postgres',
  ssl: {
    rejectUnauthorized: false
  },
  connectionTimeoutMillis: 5000,
  query_timeout: 10000
};

// Create a connection pool
const pool = new Pool(dbConfig);

// Test the connection
async function testConnection() {
  try {
    const client = await pool.connect();
    console.log('Successfully connected to the database');
    client.release();
    return true;
  } catch (error) {
    console.error('Database connection error:', error);
    return false;
  }
}

// Execute a query
async function query(text, params) {
  const start = Date.now();
  try {
    const res = await pool.query(text, params);
    const duration = Date.now() - start;
    console.log('Executed query', { text, duration, rows: res.rowCount });
    return res;
  } catch (error) {
    console.error('Query error:', { text, error });
    throw error;
  }
}

module.exports = {
  query,
  testConnection
};
