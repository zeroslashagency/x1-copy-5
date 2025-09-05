// Initialize Supabase client
const initializeSupabase = () => {
    try {
        const supabaseUrl = 'https://sxnaopzgaddvziplrlbe.supabase.co';
        const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4bmFvcHpnYWRkdnppcGxybGJlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY2MjUyODQsImV4cCI6MjA3MjIwMTI4NH0.o3UAaJtrNpVh_AsljSC1oZNkJPvQomedvtJlXTE3L6w';
        
        // Check if supabase is already defined globally
        if (window.supabase) {
            console.log('Using existing Supabase client');
            return window.supabase;
        }
        
        // Check if supabase.createClient exists
        if (typeof supabase === 'undefined' || !supabase.createClient) {
            throw new Error('Supabase client library not loaded');
        }
        
        // Create new client
        const client = supabase.createClient(supabaseUrl, supabaseKey, {
            auth: {
                autoRefreshToken: true,
                persistSession: true,
                detectSessionInUrl: true,
                storage: window.localStorage
            }
        });
        
        // Make it globally available
        window.supabase = client;
        return client;
        
    } catch (error) {
        console.error('Failed to initialize Supabase:', error);
        throw error;
    }
};

// Initialize supabase client
let supabase;
try {
    supabase = initializeSupabase();
} catch (error) {
    console.error('Critical: Failed to initialize Supabase client', error);
    // Show error to user
    document.addEventListener('DOMContentLoaded', () => {
        const errorElement = document.createElement('div');
        errorElement.style.color = 'red';
        errorElement.style.padding = '20px';
        errorElement.style.textAlign = 'center';
        errorElement.innerHTML = 'Failed to initialize the application. Please refresh the page or contact support.';
        document.body.prepend(errorElement);
    });
}

// Check authentication status
async function checkAuth() {
    try {
        // If supabase is not initialized, try to get it from window
        if (!supabase && window.supabase) {
            supabase = window.supabase;
        }
        
        // If still not initialized, show error
        if (!supabase) {
            console.error('Supabase client not initialized');
            if (!window.location.pathname.includes('auth.html')) {
                window.location.href = 'auth.html';
            }
            return null;
        }
        
        // Ensure auth methods are available
        if (!supabase.auth || typeof supabase.auth.getSession !== 'function') {
            console.error('Supabase auth methods not available');
            throw new Error('Authentication service not available');
        }

        // Get the current session
        const { data: { session }, error } = await supabase.auth.getSession();
        
        // If there was an error getting the session
        if (error) {
            console.error('Error getting session:', error);
            throw error;
        }
        
        // Check if we're on the auth page
        const isAuthPage = window.location.pathname.includes('auth.html');
        
        // If no session and not on auth page, redirect to login
        if (!session && !isAuthPage) {
            console.log('No session found, redirecting to login');
            // Clear any existing session data
            await supabase.auth.signOut();
            window.location.href = 'auth.html';
            return null;
        }
        
        // If on auth page and already logged in, redirect to index
        if (session && isAuthPage) {
            console.log('Session exists, redirecting to index');
            // Clear any URL parameters and redirect
            window.location.href = 'index.html';
            return null;
        }
        
        // If no session and on auth page, stay on auth page
        if (!session) {
            console.log('No session, staying on auth page');
            return null;
        }
        
        console.log('Session found, fetching profile...');
        
        // Get user profile with role
        const { data: profile, error: profileError } = await supabase
            .from('profiles')
            .select('*')
            .eq('id', session.user.id)
            .single();
        
        if (profileError) {
            console.error('Error fetching profile:', profileError);
            // If profile fetch fails, log out the user
            if (supabase?.auth?.signOut) {
                await supabase.auth.signOut();
            }
            if (!window.location.pathname.includes('auth.html')) {
                window.location.href = 'auth.html';
            }
            return null;
        }
        
        console.log('User authenticated:', session.user.email);
        return { session, profile };
        
    } catch (error) {
        console.error('Authentication error:', error);
        // Clear any existing session on error
        if (supabase?.auth?.signOut) {
            await supabase.auth.signOut().catch(e => console.error('Error during sign out:', e));
        }
        if (!window.location.pathname.includes('auth.html')) {
            window.location.href = 'auth.html';
        }
        return null;
    }
}

// Initialize the application
async function initApp() {
    const auth = await checkAuth();
    if (!auth) return;
    
    const { profile } = auth;
    
    // Update UI based on user role
    updateUIForRole(profile.role);
    
    // Load data based on permissions
    loadData(profile.role);
    
    // Add logout button handler
    const logoutBtn = document.getElementById('logoutBtn');
    if (logoutBtn) {
        logoutBtn.addEventListener('click', handleLogout);
    }
}

// Update UI based on user role
function updateUIForRole(role) {
    // Show/hide elements based on role
    const adminOnlyElements = document.querySelectorAll('.admin-only');
    const subadminElements = document.querySelectorAll('.subadmin-only');
    const operatorElements = document.querySelectorAll('.operator-only');
    
    adminOnlyElements.forEach(el => el.style.display = role === 'Admin' ? 'block' : 'none');
    subadminElements.forEach(el => el.style.display = (role === 'Admin' || role === 'Subadmin') ? 'block' : 'none');
    operatorElements.forEach(el => el.style.display = 'block'); // All roles can see operator elements
    
    // Update user info in UI
    const userInfo = document.getElementById('userInfo');
    if (userInfo) {
        userInfo.textContent = `Logged in as: ${role}`;
    }
}

// Load data based on user role
async function loadData(role) {
    let query = supabase.from('orders').select('*');
    
    // Operators can only see their own orders
    if (role === 'Operator') {
        const { data: { user } } = await supabase.auth.getUser();
        query = query.eq('user_id', user.id);
    }
    
    const { data: orders, error } = await query;
    
    if (error) {
        console.error('Error loading orders:', error);
        return;
    }
    
    // Update your UI with the loaded orders
    updateOrdersTable(orders);
}

// Handle logout
async function handleLogout() {
    try {
        const { error } = await supabase.auth.signOut();
        if (error) throw error;
        
        // Clear any stored session data
        localStorage.removeItem('supabase.auth.token');
        
        // Redirect to login page
        window.location.href = 'auth.html?logged_out=true';
    } catch (error) {
        console.error('Logout error:', error);
        alert('Failed to log out. Please try again.');
    }
}

// Initialize the app when the DOM is loaded
document.addEventListener('DOMContentLoaded', initApp);
