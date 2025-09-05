const { testConnection, query } = require('./db');

async function main() {
  console.log('Testing database connection...');
  const isConnected = await testConnection();
  
  if (!isConnected) {
    console.error('❌ Failed to connect to the database');
    process.exit(1);
  }
  
  console.log('✅ Successfully connected to the database');
  
  try {
    // Check if the test user exists
    const testEmail = 'admin@example.com';
    console.log(`\nChecking for test user: ${testEmail}`);
    
    const { rows } = await query(
      'SELECT * FROM auth.users WHERE email = $1',
      [testEmail]
    );
    
    if (rows.length > 0) {
      console.log('✅ Test user found in auth.users:', rows[0]);
      
      // Check if profile exists in public.profiles
      const userId = rows[0].id;
      const profileResult = await query(
        'SELECT * FROM public.profiles WHERE id = $1',
        [userId]
      );
      
      if (profileResult.rows.length > 0) {
        console.log('✅ User profile found in public.profiles:', profileResult.rows[0]);
      } else {
        console.log('⚠️  User profile not found in public.profiles');
      }
    } else {
      console.log('❌ Test user not found in auth.users');
    }
    
  } catch (error) {
    console.error('Error checking test user:', error);
  } finally {
    process.exit(0);
  }
}

main();
