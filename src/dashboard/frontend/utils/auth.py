"""
Authentication Utilities

Handle user authentication and session management for the dashboard.
"""

import streamlit as st
import hashlib
import jwt
import datetime
from typing import Optional, Dict, Any
import os

def hash_password(password: str) -> str:
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash"""
    return hash_password(password) == hashed

def create_jwt_token(user_id: str, secret_key: str) -> str:
    """Create JWT token for user"""
    
    payload = {
        'user_id': user_id,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24),
        'iat': datetime.datetime.utcnow()
    }
    
    return jwt.encode(payload, secret_key, algorithm='HS256')

def verify_jwt_token(token: str, secret_key: str) -> Optional[Dict[str, Any]]:
    """Verify JWT token and return payload"""
    
    try:
        payload = jwt.decode(token, secret_key, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def get_mock_users() -> Dict[str, Dict[str, str]]:
    """Get mock user database for demonstration"""
    
    # In production, this would be replaced with a real user database
    return {
        'admin': {
            'password': hash_password('admin123'),
            'role': 'administrator',
            'email': 'admin@quantstream.ai',
            'full_name': 'System Administrator'
        },
        'analyst': {
            'password': hash_password('analyst123'),
            'role': 'analyst',
            'email': 'analyst@quantstream.ai',
            'full_name': 'Financial Analyst'
        },
        'trader': {
            'password': hash_password('trader123'),
            'role': 'trader',
            'email': 'trader@quantstream.ai',
            'full_name': 'Quantitative Trader'
        },
        'guest': {
            'password': hash_password('guest123'),
            'role': 'viewer',
            'email': 'guest@quantstream.ai',
            'full_name': 'Guest User'
        }
    }

def authenticate_user(username: str, password: str) -> Optional[Dict[str, str]]:
    """Authenticate user credentials"""
    
    users = get_mock_users()
    
    if username in users:
        user_data = users[username]
        if verify_password(password, user_data['password']):
            return {
                'user_id': username,
                'role': user_data['role'],
                'email': user_data['email'],
                'full_name': user_data['full_name']
            }
    
    return None

def render_login_form():
    """Render login form"""
    
    st.markdown("### 🔐 QuantStream Analytics Login")
    
    with st.form("login_form"):
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.markdown("---")
            
            username = st.text_input("Username", placeholder="Enter username")
            password = st.text_input("Password", type="password", placeholder="Enter password")
            
            st.markdown("---")
            
            col_a, col_b, col_c = st.columns([1, 2, 1])
            with col_b:
                submitted = st.form_submit_button("🚀 Login", use_container_width=True)
    
    if submitted:
        if username and password:
            user_data = authenticate_user(username, password)
            
            if user_data:
                # Set session state
                st.session_state.authenticated = True
                st.session_state.user_id = user_data['user_id']
                st.session_state.user_role = user_data['role']
                st.session_state.user_email = user_data['email']
                st.session_state.user_full_name = user_data['full_name']
                st.session_state.login_time = datetime.datetime.now()
                
                # Create JWT token (for API access)
                secret_key = os.getenv('SECRET_KEY', 'your-secret-key-here')
                token = create_jwt_token(user_data['user_id'], secret_key)
                st.session_state.auth_token = token
                
                st.success(f"Welcome, {user_data['full_name']}!")
                st.rerun()
            else:
                st.error("Invalid username or password")
        else:
            st.warning("Please enter both username and password")
    
    # Demo credentials
    with st.expander("📋 Demo Credentials"):
        st.markdown("""
        **Available Demo Accounts:**
        
        • **Administrator:** admin / admin123
        • **Analyst:** analyst / analyst123  
        • **Trader:** trader / trader123
        • **Guest:** guest / guest123
        
        *Note: These are demo credentials for testing purposes only.*
        """)

def check_authentication() -> bool:
    """Check if user is authenticated"""
    
    # Skip authentication in demo mode
    if os.getenv('DEMO_MODE', 'true').lower() == 'true':
        # Auto-login as admin for demo
        if not st.session_state.get('authenticated', False):
            st.session_state.authenticated = True
            st.session_state.user_id = 'admin'
            st.session_state.user_role = 'administrator'
            st.session_state.user_email = 'admin@quantstream.ai'
            st.session_state.user_full_name = 'Demo Administrator'
            st.session_state.login_time = datetime.datetime.now()
        return True
    
    if not st.session_state.get('authenticated', False):
        render_login_form()
        return False
    
    # Check token expiration
    login_time = st.session_state.get('login_time')
    if login_time:
        # Token expires after 24 hours
        if datetime.datetime.now() - login_time > datetime.timedelta(hours=24):
            st.session_state.authenticated = False
            st.warning("Session expired. Please log in again.")
            st.rerun()
            return False
    
    return True

def logout_user():
    """Log out current user"""
    
    # Clear session state
    keys_to_clear = [
        'authenticated', 'user_id', 'user_role', 'user_email', 
        'user_full_name', 'login_time', 'auth_token'
    ]
    
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]

def get_current_user() -> Optional[Dict[str, str]]:
    """Get current authenticated user data"""
    
    if not st.session_state.get('authenticated', False):
        return None
    
    return {
        'user_id': st.session_state.get('user_id'),
        'role': st.session_state.get('user_role'),
        'email': st.session_state.get('user_email'),
        'full_name': st.session_state.get('user_full_name'),
        'login_time': st.session_state.get('login_time')
    }

def check_permission(required_role: str) -> bool:
    """Check if current user has required role"""
    
    if not st.session_state.get('authenticated', False):
        return False
    
    user_role = st.session_state.get('user_role')
    
    # Role hierarchy
    role_hierarchy = {
        'viewer': 1,
        'analyst': 2,
        'trader': 3,
        'administrator': 4
    }
    
    user_level = role_hierarchy.get(user_role, 0)
    required_level = role_hierarchy.get(required_role, 0)
    
    return user_level >= required_level

def require_permission(required_role: str):
    """Decorator to require specific role for access"""
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not check_permission(required_role):
                st.error(f"Access denied. Required role: {required_role}")
                st.stop()
            return func(*args, **kwargs)
        return wrapper
    return decorator

def get_auth_headers() -> Dict[str, str]:
    """Get authentication headers for API calls"""
    
    token = st.session_state.get('auth_token')
    
    if token:
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    return {'Content-Type': 'application/json'}

def refresh_token() -> bool:
    """Refresh authentication token"""
    
    if not st.session_state.get('authenticated', False):
        return False
    
    user_id = st.session_state.get('user_id')
    if not user_id:
        return False
    
    try:
        secret_key = os.getenv('SECRET_KEY', 'your-secret-key-here')
        new_token = create_jwt_token(user_id, secret_key)
        st.session_state.auth_token = new_token
        st.session_state.login_time = datetime.datetime.now()
        return True
    except Exception:
        return False