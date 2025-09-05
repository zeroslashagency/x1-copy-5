const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// Initialize Supabase client with service role key for admin operations
const supabaseUrl = 'https://sxnaopzgaddvziplrlbe.supabase.co';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY; // You'll need to set this in your .env file
const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

async function createTestUser() {
  const email = `testadmin_${Date.now()}@example.com`;
  const password = 'test123456';
  const role = 'Admin';

  try {
    // Create auth user
    const { data: authData, error: authError } = await supabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true, // Skip email confirmation for testing
    });

    if (authError) throw authError;

    // Create profile with admin role
    const { data: profileData, error: profileError } = await supabase
      .from('profiles')
      .insert([
        {
          id: authData.user.id,
          email,
          role
        }
      ])
      .select();

    if (profileError) throw profileError;

    console.log('✅ Test user created successfully!');
    console.log('Email:', email);
    console.log('Password:', password);
    console.log('Role:', role);
    
    return { email, password };
  } catch (error) {
    console.error('Error creating test user:', error);
    throw error;
  }
}

createTestUser().catch(console.error);
