// Test script to verify Supabase auth and RLS
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL || 'https://sxnaopzgaddvziplrlbe.supabase.co';
const supabaseKey = process.env.SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4bmFvcHpnYWRkdnppcGxybGJlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY2MjUyODQsImV4cCI6MjA3MjIwMTI4NH0.o3UAaJtrNpVh_AsljSC1oZNkJPvQomedvtJlXTE3L6w';
const supabase = createClient(supabaseUrl, supabaseKey);

async function testAuth() {
  console.log('Testing authentication...');
  
  // Test sign in
  const { data, error } = await supabase.auth.signInWithPassword({
    email: 'admin@example.com',
    password: 'admin123'
  });

  if (error) {
    console.error('Auth error:', error.message);
    return;
  }

  console.log('✅ Successfully authenticated as:', data.user.email);
  
  // Test RLS by reading profiles (should work for admin)
  const { data: profiles, error: profilesError } = await supabase
    .from('profiles')
    .select('*');
  
  if (profilesError) {
    console.error('RLS test failed:', profilesError.message);
  } else {
    console.log('✅ Successfully read profiles:', profiles);
  }
  
  // Test creating an order
  const { data: order, error: orderError } = await supabase
    .from('orders')
    .insert([
      { 
        part_number: 'TEST-001',
        order_quantity: 10,
        priority: 'High',
        due_date: new Date().toISOString(),
        user_id: data.user.id
      }
    ])
    .select();
    
  if (orderError) {
    console.error('Order creation failed:', orderError.message);
  } else {
    console.log('✅ Successfully created order:', order);
  }
}

testAuth().catch(console.error);
