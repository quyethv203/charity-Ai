import os
from dotenv import load_dotenv

load_dotenv()


class AppConfig:
    def __init__(self):

        db_conn_str_full = os.getenv('DB_CONNECTION_STRING')
        if db_conn_str_full:
            self.db_connection_string = db_conn_str_full
            print("Using full DB_CONNECTION_STRING from .env")
        else:
            self.db_host = os.getenv('DB_HOST')
            self.db_port = os.getenv('DB_PORT')
            self.db_name = os.getenv('DB_NAME')
            self.db_user = os.getenv('DB_USER')
            self.db_password = os.getenv('DB_PASSWORD')
            self.db_driver = os.getenv('DB_DRIVER', 'mysql').lower()

            if self.db_host and self.db_name and self.db_user:
                try:
                    if self.db_driver == 'mysql':
                        conn_str_parts = [f"mysql+mysqlconnector://{self.db_user}"]
                        if self.db_password: conn_str_parts.append(f":{self.db_password}")
                        conn_str_parts.append(f"@{self.db_host}")
                        if self.db_port: conn_str_parts.append(f":{self.db_port}")
                        conn_str_parts.append(f"/{self.db_name}")
                        self.db_connection_string = "".join(conn_str_parts)

                    else:
                        print(
                            f"Warning: Không hỗ trợ xây dựng chuỗi kết nối từ biến riêng lẻ cho driver '{self.db_driver}'.")
                        self.db_connection_string = None
                except Exception as e:
                    print(f"Error constructing DB connection string: {e}")
                    self.db_connection_string = None
            else:
                print("Warning: Cấu hình Database (DB_CONNECTION_STRING hoặc DB_HOST, DB_NAME, DB_USER) chưa đầy đủ.")
                self.db_connection_string = None

        if not self.db_connection_string:
            raise ValueError(
                "Cấu hình Database (DB_CONNECTION_STRING hoặc biến riêng lẻ) chưa đầy đủ hoặc không hợp lệ trong .env.")

        self.llm_provider = os.getenv('LLM_PROVIDER', 'gemini').lower()

        if self.llm_provider == 'gemini':
            self.llm_api_key = os.getenv('GEMINI_API_KEY')
            self.llm_model = os.getenv('GEMINI_MODEL')

        elif self.llm_provider == 'openai':
            self.llm_api_key = os.getenv('OPENAI_API_KEY')
            self.llm_model = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
        else:

            self.llm_api_key = None
            self.llm_model = None
            print(f"Warning: LLM Provider '{self.llm_provider}' không được hỗ trợ cấu hình key/model tự động.")

        if not self.llm_api_key:

            if self.llm_provider == 'gemini':
                raise ValueError("GEMINI_API_KEY chưa được cấu hình trong .env.")
            elif self.llm_provider == 'openai':
                raise ValueError("OPENAI_API_KEY chưa được cấu hình trong .env.")
            else:
                raise ValueError(f"API Key cho LLM Provider '{self.llm_provider}' chưa được cấu hình trong .env.")

        if not self.llm_model:

            if self.llm_provider == 'gemini':
                raise ValueError("GEMINI_MODEL chưa được cấu hình trong .env.")
            elif self.llm_provider == 'openai':
                raise ValueError("OPENAI_MODEL chưa được cấu hình trong .env.")
            else:
                raise ValueError(f"Tên model cho LLM Provider '{self.llm_provider}' chưa được cấu hình trong .env.")


config = AppConfig()
