import os
from dotenv import load_dotenv
import traceback

load_dotenv()


class AppConfig:
    """
    Lớp chứa cấu hình ứng dụng, đọc từ biến môi trường (.env).
    Đã đơn giản hóa chỉ hỗ trợ cấu hình DB bằng biến riêng lẻ và LLM Gemini.
    """

    def __init__(self):
        print("Loading application configuration...")

        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')

        if not self.db_host or not self.db_name or not self.db_user:
            error_msg = "Cấu hình Database (DB_HOST, DB_NAME, DB_USER) chưa đầy đủ trong .env."
            print(f"Warning: {error_msg}")
            raise ValueError(error_msg)

        try:
            conn_str_parts = [f"mysql+mysqlconnector://{self.db_user}"]
            if self.db_password:
                conn_str_parts.append(f":{self.db_password}")
            conn_str_parts.append(f"@{self.db_host}")
            if self.db_port:
                conn_str_parts.append(f":{self.db_port}")
            conn_str_parts.append(f"/{self.db_name}")
            self.db_connection_string = "".join(conn_str_parts)
            print("Database connection string constructed successfully.")


        except Exception as e:

            print(f"Error constructing DB connection string: {e}")
            traceback.print_exc()
            raise ValueError(f"Lỗi khi xây dựng chuỗi kết nối Database từ biến riêng lẻ: {e}")

        self.llm_api_key = os.getenv('GEMINI_API_KEY')

        self.llm_model = os.getenv('GEMINI_MODEL', 'gemini-2.0-flash')

        if not self.llm_api_key:
            error_msg = "GEMINI_API_KEY chưa được cấu hình trong .env."
            print(f"Warning: {error_msg}")
            raise ValueError(error_msg)

        if not self.llm_model:
            error_msg = "GEMINI_MODEL chưa được cấu hình trong .env (hoặc giá trị rỗng)."
            print(f"Warning: {error_msg}")
            raise ValueError(error_msg)

        print(f"LLM Configuration loaded: Provider=Gemini, Model={self.llm_model}.")

        print("Application configuration loaded successfully.")


try:
    config = AppConfig()
except ValueError as e:
    print(f"Fatal Configuration Error: {e}")
    config = None
    raise
