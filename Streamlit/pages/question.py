import streamlit as st
import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# FastAPI backend URL from the .env file
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")  # Default to localhost if not set

# Function to get an answer from the backend API
def get_answer(question, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(f"{FASTAPI_URL}/answer-question", headers=headers, json={"question": question})
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.json().get("detail", "Failed to get an answer")}
    
# Function to get the questions from the FastAPI endpoint
def get_questions_from_db():
    """Fetch questions from FastAPI with authentication."""
    if "access_token" not in st.session_state:
        st.error("You need to login first to access questions.")
        return []

    # Include the JWT token in the request headers
    headers = {"Authorization": f"Bearer {st.session_state['access_token']}"}
    response = requests.get(f"{FASTAPI_URL}/questions", headers=headers)
    if response.status_code == 200:
        return response.json().get("questions", [])
    else:
        st.error(f"Failed to retrieve questions: {response.status_code} - {response.json().get('detail', 'Unknown error')}")
        return []
    
# Function to retrieve the extracted text and file name from FastAPI
def get_extracted_text_and_filename(question=None):
    """Fetch the extracted text and file name from the FastAPI endpoint."""
    headers = {"Authorization": f"Bearer {st.session_state['access_token']}"}
    params = {"question": question} if question else {}
    response = requests.get(f"{FASTAPI_URL}/get_extracted_text", headers=headers, params=params)
    if response.status_code == 200:
        # Return both the extracted text and the file name
        return response.json().get("extracted_text", ""), response.json().get("file_name", "")
    else:
        st.error(f"Failed to retrieve extracted text: {response.status_code} - {response.text}")
        return "", ""

# Function to process the OpenAI query
def process_openai_query(extracted_text, question, prompt):
    """Send the extracted text, selected question, and custom prompt to FastAPI."""
    headers = {"Authorization": f"Bearer {st.session_state['access_token']}"}
    data = {
        "prompt": prompt,                   # Custom prompt defined by the user
        "extracted_text": extracted_text,  # Text extracted from the document
        "question": question              # Selected question
    }

    # Make a POST request to the FastAPI endpoint with all three fields
    response = requests.post(f"{FASTAPI_URL}/process_openai_query", headers=headers, json=data)

    # Check for successful response
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error {response.status_code}: {response.text}")
        return None

# Main Streamlit function to handle question answering and text extraction with a custom prompt box
def question_answering_page():
    if "access_token" not in st.session_state:
        st.warning("You are not logged in! Please log in to access this page.")
        st.stop()

    st.title("Text Extraction & Query Hub")
    method = st.selectbox("Select Method:", options=["Open Source", "API"], index=1)
    st.subheader("Prompt Selection")

    # Retrieve the list of questions from the FastAPI backend
    prompt_options = ["Select a prompt"] + get_questions_from_db()

    selected_prompt = st.selectbox("Select a Prompt", prompt_options)

    if selected_prompt and selected_prompt != "Select a prompt":
        try:
            extracted_text, file_name = get_extracted_text_and_filename(selected_prompt)
        except Exception as e:
            st.error(f"Failed to retrieve extracted text: {e}")
            return

        if extracted_text:
            st.session_state["extracted_text"] = extracted_text
            st.session_state["file_name"] = file_name

            st.success(f"Selected Question: {selected_prompt}")
            st.info(f"**Corresponding File Name:** {file_name}")
            st.text_area("Extracted Text Preview", value=extracted_text, height=150)

            user_prompt = st.text_area("Write your custom prompt here:", height=100)

            if st.button("Run Prompt"):
                if not user_prompt:
                    st.warning("Please enter a valid custom prompt before running.")
                    return

                st.info(f"Sending extracted text, question, and custom prompt to OpenAI:\n**Question**: {selected_prompt}\n**Custom Prompt**: {user_prompt}")
                
                try:
                    openai_response = process_openai_query(extracted_text, selected_prompt, user_prompt)
                except Exception as e:
                    st.error(f"Failed to process the OpenAI query: {e}")
                    return

                if openai_response:
                    st.subheader("OpenAI Response")
                    st.write(openai_response['response'])
                else:
                    st.warning("Failed to get OpenAI response. Check the inputs and try again.")
        else:
            st.warning("No extracted text found for the selected prompt.")
    else:
        st.warning("Please select a valid prompt to proceed.")

    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.experimental_set_query_params(page="main")

if "access_token" in st.session_state:
    question_answering_page()
else:
    st.sidebar.warning("Please log in to access this page.")
