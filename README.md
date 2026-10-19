# Chat With Anything - Intelligent Document Processing & Chat Assistant

## Table of Contents
1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Architecture](#architecture)
4. [System Requirements](#system-requirements)
5. [Installation & Setup](#installation--setup)
6. [Project Structure](#project-structure)
7. [Core Components](#core-components)
8. [Configuration](#configuration)
9. [Usage Instructions](#usage-instructions)
10. [API Endpoints](#api-endpoints)
11. [Troubleshooting](#troubleshooting)
12. [Contributing](#contributing)

## Project Overview

Chat With Anything is a document processing and AI-powered chat system that combines document ingestion, vector storage, and intelligent query processing. It processes various document formats, creates vector embeddings for semantic search, and provides responses through an LLM-powered chat interface.

### Key Features

- **🎤 Multi-format Document Processing**: Supports PDF, DOCX, PPTX, XLSX, TXT, and more
- **🔍 Vector-based Semantic Search**: Uses ChromaDB with HuggingFace embeddings for intelligent document retrieval
- **🤖 AI-Powered Chat**: Integrates with advanced LLM services for intelligent responses
- **🎵 Audio Processing**: Supports multiple audio formats with transcription capabilities
- **📡 Kafka Integration**: Asynchronous message processing and queuing for scalable operations
- **🌐 RESTful API**: BentoML-based service for seamless external integrations
- **💾 Database Integration**: PostgreSQL for chat history and metadata storage
- **🔄 Real-time Streaming**: Support for streaming responses and real-time processing
- **🌍 Web Content Processing**: URL scraping and web content extraction
- **📊 Advanced Logging**: Comprehensive logging and monitoring capabilities

## Architecture

The system follows a microservices architecture with the following components:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client Apps   │    │   Kafka Topics   │    │   BentoML      │
│                 │◄──►│                  │◄──►│   Service      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   ChromaDB       │    │   Document      │
│   Database      │◄──►│   Vector Store   │◄──►│   Processor     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### System Components

- **Document Processing Pipeline**: Handles multi-format document ingestion and text extraction
- **Vector Database**: ChromaDB for semantic search and document retrieval
- **Chat Pipeline**: Core AI logic for processing queries and generating responses
- **API Service**: BentoML-based RESTful API for external integrations
- **Message Queue**: Kafka for asynchronous processing and scalability
- **Database Layer**: PostgreSQL for persistent storage and chat history

## System Requirements

### Hardware Requirements
- **CPU**: 4+ cores recommended
- **RAM**: 8GB minimum, 16GB recommended
- **Storage**: 100GB+ available space
- **Network**: Stable internet connection for external API calls

### Software Requirements
- **Operating System**: Linux (Ubuntu 18.04+ recommended) or Windows 10+
- **Python**: 3.8+
- **Conda**: For environment management
- **PostgreSQL**: 12+
- **Kafka**: 2.8+ (optional for local development)
- **Docker**: For containerized deployment (optional)

### Dependencies
- **Core ML**: PyTorch, Transformers, LangChain
- **Vector Database**: ChromaDB
- **Document Processing**: PyPDF2, python-pptx, python-docx, pandas
- **Web Framework**: BentoML, FastAPI
- **Database**: psycopg2, SQLAlchemy
- **Message Queue**: kafka-python
- **Utilities**: requests, beautifulsoup4, unstructured

## Installation & Setup

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd chat-with-anything

# Create conda environment
conda create -n chat-with-anything python=3.8
conda activate chat-with-anything

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Configuration

Set up PostgreSQL database:

```sql
CREATE DATABASE Unifile_Chat;
CREATE SCHEMA chat_schema;
CREATE TABLE chat_schema.message (
    id SERIAL PRIMARY KEY,
    chat_id VARCHAR(255),
    query TEXT,
    response TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3. Directory Structure Setup

```bash
# Create necessary directories
mkdir -p UPLOAD DUMP chroma_db logs SERVICES
```

### 4. Environment Variables

Create a `.env` file:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=Unifile_Chat
DB_USER=your_username
DB_PASSWORD=your_password

# Kafka Configuration (optional)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_INPUT_TOPIC=chat-input
KAFKA_OUTPUT_TOPIC=chat_ai_output

# API Keys
OPENAI_API_KEY=your_openai_key
GROQ_API_KEY=your_groq_key

# File Paths
UPLOAD_DIR=./UPLOAD
CHROMA_PERSIST_DIR=./chroma_db
LOG_PATH=./logs
```

For local runs, place these settings in a `.env` file. The batch query processor
loads `GROQ_API_KEY` from that file; do not commit real API keys.

## Project Structure

```
chat-with-anything/
├── README.md                           # This documentation
├── requirements.txt                    # Python dependencies
├── .env                               # Environment variables
├── service.py                         # BentoML service definition
├── chat_pipeline.py                  # Core chat processing logic
├── data_ingestion_pipeline.py        # Document processing pipeline
├── chat_batch_processing_version5.py # Main processing pipeline
├── send_post.py                      # Audio processing integration
├── logger_config.py                  # Logging configuration
├── client.py                         # Client utilities
├── test.py                           # Test scripts
├── UPLOAD/                           # Document upload directory
├── DUMP/                             # Processed document storage
├── chroma_db/                        # Vector database storage
├── logs/                             # Application logs
├── SERVICES/                         # Additional services
│   └── web_scrape_service.py        # Web scraping service
└── myproject/                        # BentoML project files
    ├── runner.py                     # Service runner
    └── *.json                        # Model artifacts
```

## Core Components

### 1. Document Processing Pipeline
**File**: `chat_batch_processing_version5.py`

**Purpose**: Kafka consumer that processes document uploads, extracts text, and stores embeddings in ChromaDB

**Key Features**:
- Multi-format document support (PDF, DOCX, PPTX, TXT, audio files, web links)
- Text extraction and processing pipeline
- ChromaDB vector storage integration
- LLM-powered text summarization
- Audio transcription processing
- Duplicate message prevention

**Supported File Types**:
- **Documents**: PDF, DOC, DOCX, PPT, PPTX, TXT, XLSX, XLS
- **Audio**: MP3, WAV, M4A, OGG, FLAC, OPUS, AAC, AMR, WMA, WEBM, ALAC
- **Video**: MP4, AVI, MOV
- **Web Content**: URL links with web scraping
- **Text**: Direct text input

### 2. Chat Pipeline
**File**: `chat_pipeline.py`

**Purpose**: Core logic for processing chat messages and generating responses

**Key Features**:
- Document retrieval using ensemble retriever
- Chat history extraction from PostgreSQL
- LLM integration for intelligent responses
- Response formatting and validation
- Error handling and logging

### 3. Data Ingestion
**File**: `data_ingestion_pipeline.py`

**Purpose**: Processes various document formats and converts them to searchable text

**Supported Formats**:
- **PDF**: OCR-enabled text extraction
- **DOCX/DOC**: Word document processing
- **PPTX/PPT**: PowerPoint presentation extraction
- **XLSX/XLS**: Excel spreadsheet processing
- **TXT**: Plain text files
- **URL**: Web page content extraction
- **Audio/Video**: Transcription processing

### 4. API Service
**File**: `service.py`

**Purpose**: RESTful API service for external integrations

**Features**:
- Chat completion endpoints
- Streaming response support
- CORS configuration
- Request validation
- Error handling

## Configuration

### Model Configuration

```python
# Embeddings Model
embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    model_kwargs={'device': 'cpu'}
)

# Text Chunking
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200
)

# LLM Configuration
model="llama-3.3-70b-versatile"
temperature=0.1
max_tokens=4096
```

## Usage Instructions

### Starting the System

#### Option 1: Using TMUX (Recommended)

```bash
# Create new tmux session
tmux new-session -d -s chat-with-anything

# Split session into two panes
tmux split-window -h

# First pane: Start document processing
tmux send-keys -t 0 "conda activate chat-with-anything" Enter
tmux send-keys -t 0 "python chat_batch_processing_version5.py" Enter

# Second pane: Start API service
tmux send-keys -t 1 "conda activate chat-with-anything" Enter
tmux send-keys -t 1 "bentoml serve service:ChatService" Enter
```

#### Option 2: Manual Session Management

```bash
# Terminal 1: Document Processing
conda activate chat-with-anything
python chat_batch_processing_version5.py

# Terminal 2: API Service
conda activate chat-with-anything
bentoml serve service:ChatService
```

### Document Processing Workflow

#### 1. Upload Documents
```bash
# Copy documents to UPLOAD directory
cp document.pdf ./UPLOAD/
```

#### 2. Process Documents
```python
from data_ingestion_pipeline import LoadData

loader = LoadData()
result = loader.load_pdf("./UPLOAD/document.pdf")
```

#### 3. Query Documents
```python
import requests

# Send query through API
response = requests.post('http://localhost:8000/chat/completions', json={
    "model": "chat-with-anything-llm",
    "messages": [{
        "query": "What is the main topic?",
        "noteRef": [123],
        "chat_id": "456",
        "uniqueId": ["document-uuid"]
    }],
    "max_tokens": 4096,
    "temperature": 0.1
})
```

## API Endpoints

### Chat Completions
**Endpoint**: `POST /chat/completions`

**Request Body**:
```json
{
    "model": "chat-with-anything-llm",
    "messages": [
        {
            "query": "What is the document about?",
            "noteRef": [123],
            "chat_id": "456",
            "uniqueId": ["uuid-123"]
        }
    ],
    "max_tokens": 4096,
    "temperature": 0.1,
    "stream": false
}
```

**Response**:
```json
{
    "id": "chat_1234567890",
    "object": "chat.completion",
    "created": 1640995200,
    "model": "chat-with-anything-llm",
    "choices": [{
        "message": {
            "role": "assistant",
            "content": "The document discusses..."
        }
    }]
}
```

### Health Check
**Endpoint**: `GET /health`

**Response**:
```json
{
    "status": "healthy",
    "timestamp": "2024-01-01T00:00:00Z"
}
```

## Troubleshooting

### Common Issues

#### 1. Environment Not Found
```bash
# List available environments
conda env list

# Create environment if it doesn't exist
conda create -n chat-with-anything python=3.8
```

#### 2. Port Already in Use
```bash
# Check what's using the port
lsof -i :8000

# Kill the process if needed
kill -9 <PID>
```

#### 3. Import Errors
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

#### 4. Database Connection Issues
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Test connection
psql -h localhost -U your_username -d Unifile_Chat
```

### Logging

Check logs for debugging:

```bash
# Real-time log monitoring
tail -f logs/chat_combined.log

# Check specific log files
tail -f logs/chat_pipeline.log
tail -f logs/chat_service.log
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the logs for error details

---

**Chat With Anything** - Transforming documents into intelligent conversations through advanced AI.