import os
from datetime import datetime, timezone
from dotenv import load_dotenv

from pymongo import MongoClient
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI
from langchain_mongodb.agent_toolkit import (
    MongoDBDatabase,
    MongoDBDatabaseToolkit,
)

# 1. Load Environment Variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
LOG_DB_NAME = os.getenv("LOG_DB_NAME", "agent_telemetry_db")
LOG_COL_NAME = os.getenv("LOG_COLLECTION_NAME", "chat_history_logs")

if not MONGODB_URI:
    raise ValueError("Please set MONGODB_URI in your .env file.")

def main():
    print(f"Initializing LLM Model: {OPENAI_MODEL}...")
    llm = ChatOpenAI(model=OPENAI_MODEL, temperature=0)

    # 2. Setup the Logging Connection
    print(f"Connecting to Logging DB: {LOG_DB_NAME}.{LOG_COL_NAME}...")
    mongo_client = MongoClient(MONGODB_URI)
    log_collection = mongo_client[LOG_DB_NAME][LOG_COL_NAME]

    # 3. Connect to the Operational Database
    print(f"Connecting to Operational DB: '{OPERATIONAL_DB_NAME}'...")
    db_wrapper = MongoDBDatabase.from_connection_string(
        MONGODB_URI,
        database=OPERATIONAL_DB_NAME
    )

    # 4. Initialize the MongoDB Toolkit
    toolkit = MongoDBDatabaseToolkit(db=db_wrapper, llm=llm)
    mongo_tools = toolkit.get_tools()

    system_prompt = f"""
    You are an expert MongoDB data analyst working with the '{OPERATIONAL_DB_NAME}' database.
    Your goal is to answer questions by writing and executing MongoDB Query Language (MQL).

    Follow this exact workflow for every query:
    1. Discovery: Use `mongodb_list_collections` to identify available data.
    2. Schema Understanding: Use `mongodb_schema` to examine the structure of relevant collections.
    3. Query Generation: Convert the natural language into a MongoDB aggregation pipeline.
    4. Validation: Use `mongodb_query_checker` to verify your syntax and field references.
    5. Execution: Use `mongodb_query` to run the validated pipeline.

    CRITICAL: Only aggregate(...) pipelines are supported. Do NOT use find() or other methods.
    If an execution fails, analyze the error, adjust your pipeline, and try again.
    Return your final answer in a clean, readable format.
    """

    # 5. Create the Deep Agent
    print("Building the Deep Agent...\n")
    agent = create_deep_agent(
        model=llm,
        tools=mongo_tools,
        system_prompt=system_prompt
    )

    # 6. Interactive Agent Loop
    print("\n" + "=" * 60)
    print("✅ Terminal Agent is ready! Chat history and MQL queries are being logged.")
    print("Type your questions below. Type 'exit' or 'quit' to stop.")
    print("=" * 60)

    while True:
        user_input = input("\nAsk your MongoDB question: ")
        
        if user_input.strip().lower() in ['exit', 'quit']:
            print("Shutting down the agent. Goodbye!")
            mongo_client.close()
            break
            
        if not user_input.strip():
            continue
            
        print("\nAgent is reasoning and executing tools...")
        
        # Prepare base log entry
        log_entry = {
            "timestamp": datetime.now(timezone.utc),
            "user_message": user_input,
            "source": "terminal", # Distinguishes terminal testing from web API calls
            "status": "pending",
            "agent_response": None,
            "mql_queries_executed": [], # New array to hold MQL
            "error_details": None
        }
        
        try:
            # Stream the reasoning process to the terminal
            events = agent.stream(
                {"messages": [{"role": "user", "content": user_input}]},
                stream_mode="values"
            )
            
            final_state = None
            for event in events:
                final_state = event
                message = event["messages"][-1]
                message.pretty_print()
                
            # Extract final message from the last state
            final_message = final_state["messages"][-1].content
            
            # Extract the actual MongoDB queries the agent executed
            executed_queries = []
            if final_state and "messages" in final_state:
                for msg in final_state["messages"]:
                    if hasattr(msg, "tool_calls") and msg.tool_calls:
                        for tool_call in msg.tool_calls:
                            if tool_call["name"] == "mongodb_query":
                                executed_queries.append(tool_call["args"].get("query"))
                
            # Log success and attach the executed queries
            log_entry["status"] = "success"
            log_entry["agent_response"] = final_message
            log_entry["mql_queries_executed"] = executed_queries
            log_collection.insert_one(log_entry)
                
        except Exception as e:
            print(f"\nAn error occurred: {e}")
            print("Please try rephrasing your question.")
            
            # Log failure
            log_entry["status"] = "error"
            log_entry["error_details"] = str(e)
            log_collection.insert_one(log_entry)

if __name__ == "__main__":
    main()