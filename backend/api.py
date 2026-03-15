import os
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

from pymongo import MongoClient
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI
from langchain_mongodb.agent_toolkit import MongoDBDatabase, MongoDBDatabaseToolkit

# Load environment variables from the .env file
load_dotenv()

# Global variables to hold our agent and our logging collection
mongo_agent = None
log_collection = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager to initialize the agent and DB connections on startup."""
    global mongo_agent, log_collection
    print("Starting up: Initializing MongoDB Agent and Logger...")
    
    MONGODB_URI = os.getenv("MONGODB_URI")
    if not MONGODB_URI:
        raise ValueError("MONGODB_URI is not set in the environment.")

    # Fetch configurations from the environment
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
    OPERATIONAL_DB_NAME = os.getenv("OPERATIONAL_DB_NAME", "sample_mflix")
    LOG_DB_NAME = os.getenv("LOG_DB_NAME", "agent_telemetry_db")
    LOG_COL_NAME = os.getenv("LOG_COLLECTION_NAME", "chat_history_logs")
    
    print(f"Using LLM Model: {OPENAI_MODEL}")
    print(f"Logging to Database: '{LOG_DB_NAME}', Collection: '{LOG_COL_NAME}'")

    # 1. Setup pure PyMongo client specifically for our logging collection
    mongo_client = MongoClient(MONGODB_URI)
    log_db = mongo_client[LOG_DB_NAME]
    log_collection = log_db[LOG_COL_NAME]

    # 2. Setup the LangChain Deep Agent (Querying the operational database)
    llm = ChatOpenAI(model=OPENAI_MODEL, temperature=0)
    db_wrapper = MongoDBDatabase.from_connection_string(MONGODB_URI, database=OPERATIONAL_DB_NAME)
    toolkit = MongoDBDatabaseToolkit(db=db_wrapper, llm=llm)
    
    system_prompt = f"""
    You are an expert MongoDB data analyst working with the '{OPERATIONAL_DB_NAME}' database.
    Your goal is to answer questions by writing and executing MongoDB Query Language (MQL).

    Follow this exact workflow for every query:
    1. Discovery: Use `mongodb_list_collections` to identify available data.
    2. Schema Understanding: Use `mongodb_schema` to examine the structure of relevant collections.
    3. Query Generation: Convert the natural language into a full MongoDB aggregation command string in the format `db.collection_name.aggregate([...])`.
    4. Validation: Use `mongodb_query_checker` to verify your syntax and field references. Use the full `db.collection_name.aggregate([...])` string here too.
    5. Execution: Use `mongodb_query` to run the validated command string.

    CRITICAL: You MUST provide the full command string (e.g., `db.movies.aggregate([...])`) to the `mongodb_query` tool. 
    ALL keys in your MQL must be enclosed in double quotes (e.g., `{{"$match": {{"year": 1995}}}}` instead of `{{$match: {{year: 1995}}}}`).
    Only aggregate pipelines are supported. Do NOT use find() or other methods.
    If an execution fails, analyze the error, adjust your pipeline, and try again.
    Return your final answer in a clean, human-readable format.
    """
    
    mongo_agent = create_deep_agent(
        model=llm,
        tools=toolkit.get_tools(),
        system_prompt=system_prompt
    )
    print("✅ Agent and Logger are ready!")
    yield
    print("Shutting down API and closing connections...")
    mongo_client.close()

# Initialize FastAPI
app = FastAPI(lifespan=lifespan, title="MongoDB Deep Agent API")

# Define the request payload structure
class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    """Receives a string, runs the agent, logs the interaction (and MQL), and returns the response."""
    if not mongo_agent or log_collection is None:
        raise HTTPException(status_code=500, detail="Server not fully initialized.")
    
    # Prepare the base log document
    log_entry = {
        "timestamp": datetime.now(timezone.utc),
        "user_message": request.message,
        "status": "pending",
        "agent_response": None,
        "mql_queries_executed": [],
        "error_details": None
    }
    
    try:
        # 1. Run the agent
        initial_state = {"messages": [{"role": "user", "content": request.message}]}
        final_state = await mongo_agent.ainvoke(initial_state)
        
        # 2. Extract the final message content from the agent
        final_message = final_state["messages"][-1].content
        
        # Ensure the response is a string (if it's a list or dict, stringify it)
        if not isinstance(final_message, str):
            import json
            final_message = json.dumps(final_message, indent=2)
        
        # 3. Extract the actual MongoDB queries the agent executed from the intermediate steps
        executed_queries = []
        for msg in final_state["messages"]:
            # Check if the AI message contains tool calls
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                for tool_call in msg.tool_calls:
                    # We specifically want to log when it executes a database query
                    if tool_call["name"] == "mongodb_query":
                        # The tool's argument contains the actual MQL query payload
                        executed_queries.append(tool_call["args"].get("query"))

        # 4. Update the log entry with success details and the extracted queries
        log_entry["status"] = "success"
        log_entry["agent_response"] = final_message
        log_entry["mql_queries_executed"] = executed_queries
        
        # 5. Insert the complete log into MongoDB telemetry database
        log_collection.insert_one(log_entry)
        
        # 6. Cleanup the _id injected by PyMongo so it doesn't break our JSON return
        if "_id" in log_entry:
            del log_entry["_id"] 
        
        return {"response": final_message, "mql": executed_queries}
        
    except Exception as e:
        # If anything fails, update the log entry with the error
        error_msg = str(e)
        
        log_entry["status"] = "error"
        log_entry["error_details"] = error_msg
        
        # Insert the error log into MongoDB telemetry database
        log_collection.insert_one(log_entry)
        
        # Raise the HTTP exception to the frontend
        raise HTTPException(status_code=500, detail=error_msg)

if __name__ == "__main__":
    import uvicorn
    # Run the API on port 8000
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)