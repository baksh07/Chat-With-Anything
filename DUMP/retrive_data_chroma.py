from langchain.vectorstores import Chroma
from langchain.embeddings import HuggingFaceEmbeddings
from pathlib import Path

# Load the same embeddings model used during saving
embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    model_kwargs={'device': 'cuda'}
)

# Directory where Chroma DB is persisted
persist_directory = str(Path(__file__).resolve().parents[1] / "chroma_db")

# Unique collection name (uniqueId used while saving)
collection_name = "be9738a8-3a9b-49a5-8e5e-09edd83e0119"

# Load the Chroma collection
db = Chroma(
    collection_name=collection_name,
    embedding_function=embeddings,
    persist_directory=persist_directory
)

# Get all documents by doing similarity search with an empty string (not perfect, but works)
results = db.similarity_search("", k=100)  # increase k based on your storage

for doc in results:
    print("Content:", doc.page_content)
    print("Metadata:", doc.metadata)
