const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

// Direct connection test
async function testDirectConnection() {
  const config = {
    user: 'postgres.sxnaopzgaddvziplrlbe',
    password: 'Qwerty@0073',
    host: 'aws-1-ap-south-1.pooler.supabase.com',
    port: 5432, // Try both 5432 and 6543
    database: 'postgres',
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 5000
  };

  console.log('Testing direct connection with port 5432...');
  await testConnection(config);
  
  // Test with port 6543
  config.port = 6543;
  console.log('\nTesting direct connection with port 6543...');
  await testConnection(config);
}

async function testConnection(config) {
  const pool = new Pool(config);
  const client = await pool.connect().catch(err => {
    console.error('Connection failed:', err.message);
    return null;
  });
  
  if (client) {
    try {
      const res = await client.query('SELECT version()');
      console.log('✅ Connection successful!');
      console.log('Database version:', res.rows[0].version);
    } catch (err) {
      console.error('Query failed:', err.message);
    } finally {
      client.release();
    }
  }
  await pool.end();
}

// Run the test
testDirectConnection().catch(console.error);
