from fastapi import FastAPI, HTTPException, Depends, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from jose import JWTError, jwt
import mysql.connector
from passlib.context import CryptContext
import openai
from dotenv import load_dotenv
import os
import requests

# Load environment variables from the .env file
load_dotenv()

# Database connection configuration using environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# JWT and security configurations
SECRET_KEY = os.getenv('SECRET_KEY')
ALGORITHM = os.getenv('ALGORITHM')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', 15))

# OpenAI API Key from the environment
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

# Validate if the key is set correctly
if not openai.api_key:
    raise ValueError("OpenAI API key is missing.")

# FastAPI app
app = FastAPI()

# Password context for hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Security scheme for JWT tokens
security = HTTPBearer()

# Pydantic models
class UserCreate(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class UserProfile(BaseModel):
    username: str
    created_at: datetime

class UpdatePassword(BaseModel):
    old_password: str
    new_password: str

class OpenAIQueryRequest(BaseModel):
    extracted_text: str  # Text extracted from the document
    question: str  
    prompt: str      # Selected question

class OpenAIResponse(BaseModel):
    response: str  # The generated response from OpenAI

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )

# Utility functions
def get_password_hash(password: str):
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_user(username: str):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    connection.close()
    return user

def create_user(username: str, password: str):
    hashed_password = get_password_hash(password)
    created_at = datetime.utcnow()
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO users (username, password, created_at) VALUES (%s, %s, %s)",
                   (username, hashed_password, created_at))
    connection.commit()
    cursor.close()
    connection.close()

# Custom JWT authentication and user retrieval
def decode_jwt_token(token: str) -> Dict:
    """Decode JWT and return the payload."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_current_user(authorization: HTTPAuthorizationCredentials = Depends(security)):
    """Retrieve the current user based on the JWT token."""
    token = authorization.credentials
    payload = decode_jwt_token(token)
    username = payload.get("sub")
    if username is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = get_user(username)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user

@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI app"}

# API Endpoints
@app.post("/signup", response_model=Token)
async def signup(
    username: str = Query(..., description="The username for the new user"),
    password: str = Query(..., description="The password for the new user")
):
    existing_user = get_user(username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    create_user(username, password)
    access_token = create_access_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/login", response_model=Token)
async def login(
    username: str = Query(..., description="The username of the user"),
    password: str = Query(..., description="The password of the user")
):
    user = get_user(username)
    if not user or not verify_password(password, user["password"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/profile", response_model=UserProfile)
async def read_user_profile(current_user: dict = Depends(get_current_user)):
    return {"username": current_user["username"], "created_at": current_user["created_at"]}

@app.put("/update-password")
async def update_password(
    old_password: str = Query(..., description="The current password of the user"),
    new_password: str = Query(..., description="The new password to set"),
    current_user: dict = Depends(get_current_user)
):
    if not verify_password(old_password, current_user["password"]):
        raise HTTPException(status_code=400, detail="Old password is incorrect")
    hashed_password = get_password_hash(new_password)
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("UPDATE users SET password = %s WHERE username = %s", (hashed_password, current_user["username"]))
    connection.commit()
    cursor.close()
    connection.close()
    return {"msg": "Password updated successfully"}

@app.get("/protected")
async def read_protected(current_user: dict = Depends(get_current_user)):
    return {"message": f"Hello, {current_user['username']}! You have access to this protected route."}

@app.get("/questions", dependencies=[Depends(get_current_user)])
def get_questions():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT question FROM gaia_merged_pdf")
    questions = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"questions": [q["question"] for q in questions]}

@app.post("/process_openai_query", response_model=OpenAIResponse)
async def process_openai_query(request: OpenAIQueryRequest, current_user: str = Depends(get_current_user)):
    try:
        messages = [
            {"role": "system", "content": "Instructions..."},
            {"role": "user", "content": f"Question: {request.question}"},
            {"role": "user", "content": f"Prompt: {request.prompt}"},
            {"role": "user", "content": f"Extracted Text: {request.extracted_text}"}
        ]
        payload = {
            "model": "gpt-4",
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 2048,
            "n": 1
        }
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return {"response": response.json()['choices'][0]['message']['content']}
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Request error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/get_extracted_text", dependencies=[Depends(get_current_user)])
def get_extracted_text(question: str = Query(None, description="Question text to extract associated text and file name")):
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        if question:
            cursor.execute("SELECT extracted_text, file_name FROM gaia_merged_pdf WHERE question = %s", (question,))
        else:
            cursor.execute("SELECT extracted_text, file_name FROM gaia_merged_pdf LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        if not result:
            raise HTTPException(status_code=404, detail="Document not found or no extracted text available.")
        return {"extracted_text": result["extracted_text"], "file_name": result["file_name"]}
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database error: {err}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
