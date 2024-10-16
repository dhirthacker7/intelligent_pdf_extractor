import streamlit as st
import requests
import datetime
from streamlit_option_menu import option_menu
import openai  # Ensure this is at the top of your file
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# FastAPI backend URL from .env file
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000")  # Default to localhost if not found

# Function to register a new user
def register_user(username, password):
    response = requests.post(f"{FASTAPI_URL}/signup?username={username}&password={password}")
    return response.json()

# Function to login and retrieve JWT token
def login_user(username, password):
    response = requests.post(f"{FASTAPI_URL}/login?username={username}&password={password}")
    return response.json()

# Function to check if the session is expired
def is_session_expired():
    if "token_expiration" not in st.session_state:
        return True  # No expiration time set

    current_time = datetime.datetime.utcnow()
    return current_time >= st.session_state["token_expiration"]

# Function to view user profile
def view_profile(token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{FASTAPI_URL}/profile", headers=headers)
    return response.json()

# Function to update password
def update_password(old_password, new_password, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.put(f"{FASTAPI_URL}/update-password", headers=headers, json={"old_password": old_password, "new_password": new_password})
    return response.json()

# Function to get an answer to a question using the FastAPI backend
def get_answer(question, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(f"{FASTAPI_URL}/answer-question", headers=headers, json={"question": question})
    return response.json()

# Main Streamlit App
def main():
    st.title("Text Extraction & Query Hub")

    # Menu options based on the user's login status
    if "access_token" not in st.session_state or is_session_expired():
        # Show login and signup options if not logged in
        menu_options = ["Login", "Signup"]
    else:
        # Show authenticated options if the user is logged in
        menu_options = ["View Profile", "Update Password"]

    # Sidebar menu
    with st.sidebar:
        choice = option_menu(
            "Menu",
            menu_options,
            icons=["box-arrow-in-right", "person-plus"] if "access_token" not in st.session_state else ["person-circle", "lock", "shield-lock", "box-arrow-right"],
            key="main_menu_option"
        )

    # Menu Logic
    if choice == "Signup":
        show_signup_page()
    elif choice == "Login":
        show_login_page()
    elif choice == "View Profile":
        show_profile_page()
    elif choice == "Update Password":
        show_update_password_page()
    elif choice == "Protected":
        show_protected_page()

# Show the signup page
def show_signup_page():
    st.subheader("Signup Page")
    new_username = st.text_input("Create a Username")
    new_password = st.text_input("Create a Password", type="password")
    confirm_password = st.text_input("Confirm Password", type="password")

    if st.button("Signup"):
        if new_password == confirm_password:
            result = register_user(new_username, new_password)
            st.success(result.get("msg", "Signup successful"))
        else:
            st.warning("Passwords do not match!")

# Show the login page
def show_login_page():
    st.subheader("Login Page")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        result = login_user(username, password)
        if "access_token" in result:
            st.success(f"Login Successful! Welcome, {username}!")
            # Store access token and expiration time in session state
            st.session_state["access_token"] = result["access_token"]
            st.session_state["username"] = username  # Store the username for later use
            st.session_state["token_expiration"] = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
            
            # Update query parameters to force a rerun without using experimental_rerun
            st.experimental_set_query_params(logged_in="true")
        else:
            st.error(result.get("detail", "Login Failed"))

# Show the user profile page
def show_profile_page():
    st.subheader("User Profile")
    if "access_token" in st.session_state:
        profile_data = view_profile(st.session_state["access_token"])
        if "username" in profile_data:
            st.write(f"Username: {profile_data['username']}")
            st.write(f"Created At: {profile_data['created_at']}")
        else:
            st.error(profile_data.get("detail", "Could not retrieve profile"))
    else:
        st.warning("You need to login first.")

# Show the update password page
def show_update_password_page():
    st.subheader("Update Password")
    old_password = st.text_input("Old Password", type="password")
    new_password = st.text_input("New Password", type="password")

    if st.button("Update Password"):
        result = update_password(old_password, new_password, st.session_state["access_token"])
        if "msg" in result:
            st.success(result["msg"])
        else:
            st.error(result.get("detail", "Password update failed"))

# Show the protected page
def show_protected_page():
    st.subheader("Protected Page")
    if "access_token" in st.session_state:
        # Include the token in the headers
        headers = {"Authorization": f"Bearer {st.session_state['access_token']}"}
        response = requests.get(f"{FASTAPI_URL}/protected", headers=headers)

        if response.status_code == 200:
            st.success(response.json().get("message"))
            # Display the user's JWT token
            st.write(f"**Your JWT Token:** {st.session_state['access_token']}")
        else:
            st.error("Access Denied!")
    else:
        st.warning("You need to login first.")

# Handle user logout
def handle_logout():
    st.session_state.clear()
    st.experimental_set_query_params(logged_in="false")
    st.success("You have been logged out successfully!")

if __name__ == "__main__":
    main()
