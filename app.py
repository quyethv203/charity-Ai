import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import traceback

from RAG.service import DatabaseChatbotService
from RAG.llm_client import LlmClient

from flask_cors import CORS

from datetime import datetime

load_dotenv()

service: DatabaseChatbotService = None
llm_client: LlmClient = None


def main():
    global service, llm_client
    print("Initializing components...")

    llm_api_key = os.getenv("GEMINI_API_KEY")

    llm_model = os.getenv("GEMINI_MODEL")

    if not llm_api_key:
        print("Error: GEMINI_API_KEY environment variable not set. Cannot initialize LLM Client.")

        return

    try:

        llm_client = LlmClient(api_key=llm_api_key, model=llm_model)
        print("LLM Client initialized successfully.")
    except Exception as e:

        print(f"Error initializing LLM Client: {e}")
        traceback.print_exc()
        return

    db_context_path = os.getenv("DB_CONTEXT_PATH", "./chatbot_core/db_context.txt")

    try:

        service = DatabaseChatbotService(
            llm_client=llm_client,
            db_context_path=db_context_path
        )
        print("Database Chatbot Service initialized successfully.")
    except Exception as e:
        print(f"Error initializing Database Chatbot Service: {e}")
        traceback.print_exc()
        return

    app = Flask(__name__)

    allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000")
    allowed_origins = [origin.strip() for origin in allowed_origins_str.split(',')]
    CORS(app, origins=allowed_origins)

    print(f"CORS configured for origins: {allowed_origins}")

    @app.route('/chat', methods=['POST'])
    def chat():

        user_message = request.json.get('message')

        if not user_message:
            return jsonify({'response_text': 'No message provided', 'query_results_data': []}), 400

        print(f"Received message: '{user_message}'")

        process_result = service.process_query(user_message)

        if len(process_result) == 1:
            error_message = process_result[0]
            print(f"Processing failed in step 1: {error_message}")
            return jsonify({'response_text': error_message, 'query_results_data': []})
        else:
            sql_cleaned = process_result[0]
            raw_results_list = process_result[1]
            formatted_results_table_string = process_result[2]
            print(f"Processing step 1 successful. Calling LLM for friendly response...")

            if llm_client is None:
                print("Error: LLM Client not initialized for friendly response generation.")
                return jsonify(
                    {'response_text': "Xin lỗi, hệ thống xử lý phản hồi gặp sự cố nội bộ.", 'query_results_data': []})

            prompt_friendly_response = f"""
              You are a helpful assistant synthesizing information from a database query result for a user.
              Based on the user's original question, the SQL query executed, and the results obtained from the database,
              create a **concise**, friendly, and easy-to-understand natural language response in Vietnamese
              that directly answers the user's original question.

              --- Instructions for Friendly Response ---
              - The response must be in Vietnamese.
              - Be **concise** and to the point. Avoid unnecessary details.
              - If the database results list multiple items (e.g., a list of events), **summarize them briefly**.
              - **When listing multiple events**, focus on key identifying information like the **event name** and **location**. Avoid including full descriptions, exact start/end dates and times, or detailed quantity numbers for each item within the text response, as the user's frontend will handle displaying the detailed data separately.
              - If the database results are empty, clearly state that no results were found for their query based on the available information.
              - Do NOT include the SQL query or the raw table results in your final answer.

              User's original question: {user_message}
              Executed SQL query: ```sql
              {sql_cleaned}
              ```
              Database Results:
              ```
              {formatted_results_table_string}
              ```

              Friendly natural language response:
              """
            try:

                final_response_text = llm_client.generate_text(prompt_friendly_response)
                print(f"Final friendly response from LLM: {final_response_text}")

                query_results_data = []

                if raw_results_list:

                    for row in raw_results_list:

                        item_data_standardized = {}

                        key_mapping = {
                            'id': ['event_id', 'result_id'],
                            'name': ['name', 'event_name'],
                            'description': ['description', 'event_description'],
                            'location': ['location'],
                            'start_date': ['start_date'],
                            'end_date': ['end_date'],
                            'quantity_now': ['quantity_now'],
                            'max_quantity': ['max_quantity'],
                            'image': ['image', 'event_image'],
                            'organization_name': ['username', 'organization_name'],

                            'result_id': ['result_id'],
                            'result_description': ['content', 'result_description'],
                            'result_image': ['images', 'result_image'],

                            'related_event_id': ['event_id'],
                            'related_event_name': ['name', 'event_name'],
                            'related_event_description': ['description', 'event_description'],
                            'related_event_image': ['image', 'event_image'],
                        }

                        for standardized_key, possible_raw_keys in key_mapping.items():
                            for raw_key in possible_raw_keys:
                                if raw_key in row:
                                    value = row[raw_key]

                                    if isinstance(value, datetime):
                                        try:

                                            item_data_standardized[standardized_key] = value.isoformat()
                                        except Exception as date_e:
                                            print(
                                                f"Warning: Could not format date {standardized_key} ({raw_key}) {value}: {date_e}")
                                            item_data_standardized[standardized_key] = str(value)
                                    else:

                                        item_data_standardized[standardized_key] = value
                                    break

                        if 'result_id' in row or item_data_standardized.get('result_id') is not None:
                            item_data_standardized['type'] = 'result'
                        elif 'event_id' in row or item_data_standardized.get('id') is not None:
                            item_data_standardized['type'] = 'event'
                        else:
                            item_data_standardized['type'] = 'unknown'

                        if item_data_standardized:
                            query_results_data.append(item_data_standardized)

                    print(f"Data for frontend (query_results_data): {query_results_data}")

                return jsonify({
                    'response_text': final_response_text,
                    'query_results_data': query_results_data
                })

            except Exception as e:

                print(f"Error calling LLM (Friendly Response Generation) or processing results for data: {e}")
                traceback.print_exc()

                return jsonify({'response_text': "Xin lỗi, tôi gặp sự cố khi tạo phản hồi hoặc xử lý kết quả.",
                                'query_results_data': []})

    app.run(debug=False, host='0.0.0.0', port=5000)


if __name__ == '__main__':
    main()
