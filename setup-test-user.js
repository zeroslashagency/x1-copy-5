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

const TEST_USER = {
  email: 'admin@example.com',
  password: 'admin123',
  role: 'authenticated',
  email_confirmed_at: new Date().toISOString(),
  raw_user_meta_data: JSON.stringify({ name: 'Admin User' })
};

async function setupTestUser() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // 1. Create auth user
    const { rows: [user] } = await client.query(
      `INSERT INTO auth.users (
        instance_id, id, aud, role, email, encrypted_password, 
        email_confirmed_at, recovery_sent_at, last_sign_in_at, 
        raw_app_meta_data, raw_user_meta_data, created_at, updated_at, 
        confirmation_token, email_change, email_change_token_new, recovery_token
      ) VALUES (
        '00000000-0000-0000-0000-000000000000',
        gen_random_uuid(),
        'authenticated',
        'authenticated',
        $1,
        crypt($2, gen_salt('bf')),
        $3,
        now(),
        now(),
        '{"provider":"email","providers":["email"]}',
        $4,
        now(),
        now(),
        '',
        '',
        '',
        ''
      ) RETURNING *`,
      [TEST_USER.email, TEST_USER.password, TEST_USER.email_confirmed_at, TEST_USER.raw_user_meta_data]
    );

    // 2. Create user profile
    const userMeta = typeof user.raw_user_meta_data === 'string' 
      ? JSON.parse(user.raw_user_meta_data)
      : user.raw_user_meta_data;
      
    await client.query(
      `INSERT INTO public.profiles (
        id, email, full_name, created_at, updated_at
      ) VALUES ($1, $2, $3, now(), now())
      ON CONFLICT (id) DO UPDATE SET
        email = EXCLUDED.email,
        full_name = EXCLUDED.full_name,
        updated_at = now()`,
      [user.id, user.email, userMeta.name || 'Admin User']
    );

    await client.query('COMMIT');
    console.log('✅ Test user created successfully!');
    console.log('Email:', TEST_USER.email);
    console.log('Password:', TEST_USER.password);
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error setting up test user:', error);
  } finally {
    client.release();
    await pool.end();
  }
}

// Run the setup
setupTestUser().catch(console.error);
