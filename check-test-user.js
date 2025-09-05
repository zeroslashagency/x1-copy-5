require('dotenv').config({ path: '.env.local' });
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://sxnaopzgaddvziplrlbe.supabase.co';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE || 'your-service-role-key-here';

if (supabaseKey === 'your-service-role-key-here') {
    console.error('Please update the SUPABASE_SERVICE_ROLE in .env.local with your service role key from Supabase dashboard');
    process.exit(1);
}
const supabase = createClient(supabaseUrl, supabaseKey);

async function checkAndCreateTestUser() {
    try {
        console.log('Checking for test user...');
        
        // Check if user exists in auth.users
        const { data: authUser, error: authError } = await supabase.auth.admin.getUserByEmail('admin@example.com');
        
        if (authError && authError.status !== 404) {
            throw authError;
        }
        
        let userId;
        
        if (!authUser || !authUser.user) {
            console.log('Creating test user...');
            // Create auth user
            const { data: signUpData, error: signUpError } = await supabase.auth.admin.createUser({
                email: 'admin@example.com',
                password: 'admin123',
                email_confirm: true,
                user_metadata: { name: 'Test Admin' }
            });
            
            if (signUpError) throw signUpError;
            
            userId = signUpData.user.id;
            console.log('Auth user created:', userId);
        } else {
            userId = authUser.user.id;
            console.log('User exists in auth.users:', userId);
        }
        
        // Check if profile exists
        const { data: profile, error: profileError } = await supabase
            .from('profiles')
            .select('*')
            .eq('id', userId)
            .single();
            
        if (profileError && profileError.code !== 'PGRST116') { // PGRST116 = no rows returned
            throw profileError;
        }
        
        if (!profile) {
            console.log('Creating profile...');
            // Create profile
            const { error: insertError } = await supabase
                .from('profiles')
                .upsert({
                    id: userId,
                    email: 'admin@example.com',
                    role: 'Admin',
                    full_name: 'Test Admin'
                });
                
            if (insertError) throw insertError;
            console.log('Profile created');
        } else {
            console.log('Profile exists:', profile);
        }
        
        console.log('Test user is ready. Email: admin@example.com, Password: admin123');
        
    } catch (error) {
        console.error('Error setting up test user:');
        console.error(error);
    }
}

checkAndCreateTestUser();
