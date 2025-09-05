const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

const pool = new Pool({
  user: 'postgres.sxnaopzgaddvziplrlbe',
  password: 'Qwerty@0073',
  host: 'aws-1-ap-south-1.pooler.supabase.com',
  port: 5432,
  database: 'postgres',
  ssl: { rejectUnauthorized: false }
});

async function setupDatabase() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // 1. Enable necessary extensions
    await client.query(`
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
      CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
    `);

    // 2. Create profiles table
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.profiles (
        id uuid NOT NULL PRIMARY KEY,
        email text NOT NULL UNIQUE,
        full_name text,
        avatar_url text,
        created_at timestamp with time zone DEFAULT now(),
        updated_at timestamp with time zone DEFAULT now()
      );
    `);

    // 3. Create function for updating timestamps
    await client.query(`
      CREATE OR REPLACE FUNCTION public.handle_updated_at()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.updated_at = now();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // 4. Create trigger for profiles table
    await client.query(`
      DROP TRIGGER IF EXISTS on_profiles_updated ON public.profiles;
      CREATE TRIGGER on_profiles_updated
        BEFORE UPDATE ON public.profiles
        FOR EACH ROW
        EXECUTE FUNCTION public.handle_updated_at();
    `);

    await client.query('COMMIT');
    console.log('✅ Database setup completed successfully!');
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error setting up database:', error);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

// Run the setup
setupDatabase()
  .then(() => console.log('✅ Database setup completed!'))
  .catch(console.error);
