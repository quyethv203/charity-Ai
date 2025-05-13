from typing import List, Dict, Any
import os
from sqlalchemy import create_engine, text, inspect
import traceback

from .constants import ALLOWED_TABLES, BLACKLISTED_COLUMNS

from dotenv import load_dotenv


class DatabaseConnector:
    def __init__(self):

        load_dotenv()

        db_url = os.getenv("DATABASE_URL")

        if not db_url:

            db_connection = os.getenv("DB_CONNECTION", "mysql+mysqlconnector")
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "3306")
            db_database = os.getenv("DB_DATABASE")
            db_username = os.getenv("DB_USERNAME")
            db_password = os.getenv("DB_PASSWORD")

            if not db_database or not db_username or not db_password:
                print("Error: Database connection details (DATABASE_URL or individual DB vars) not fully set in .env")
                self.engine = None
                self.allowed_tables = ALLOWED_TABLES
                self.blacklisted_columns = BLACKLISTED_COLUMNS
                print("DatabaseConnector initialized with connection error.")
                return

            db_url = f"{db_connection}://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}"
            print(f"Constructed DB URL: {db_url.split('@')[-1]}")

        print(f"Attempting to connect to database: {db_url.split('@')[-1]}")

        try:

            self.engine = create_engine(db_url)
            self.allowed_tables = ALLOWED_TABLES
            self.blacklisted_columns = BLACKLISTED_COLUMNS
            print("DatabaseConnector initialized. Connection successful.")

        except Exception as e:
            print(f"Error connecting to database during initialization: {e}")
            traceback.print_exc()
            self.engine = None
            self.allowed_tables = ALLOWED_TABLES
            self.blacklisted_columns = BLACKLISTED_COLUMNS
            print("DatabaseConnector initialized with connection error.")

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Thực thi câu lệnh SQL SELECT đã được xác thực.
        """
        if self.engine is None:
            print("Error: Database engine is not initialized due to connection failure.")
            return []

        try:
            with self.engine.connect() as connection:

                result = connection.execute(text(query))

                return [dict(zip(result.keys(), row)) for row in result.fetchall()]
        except Exception as e:
            print(f"Error executing query: {query} - {e}")
            traceback.print_exc()
            return []

    def get_schema_description(self) -> str:
        """
        Lấy mô tả schema của các bảng được phép (bao gồm Views) từ DB thực tế.
        Kết hợp kết quả từ get_table_names() và get_view_names().
        """
        if self.engine is None:
            print("Error: Database engine is not initialized for schema retrieval.")
            return "Database Schema: Error retrieving schema due to connection failure."

        schema_description = "Database Schema (Allowed Tables and Columns - excluding blacklisted):\n"
        try:
            inspector = inspect(self.engine)

            all_table_names = inspector.get_table_names()
            all_view_names = inspector.get_view_names()

            all_entity_names = all_table_names + all_view_names

            allowed_physical_entities = []
            for entity_name in all_entity_names:

                if entity_name.lower() in self.allowed_tables:
                    allowed_physical_entities.append(entity_name)

            if not allowed_physical_entities:
                schema_description += "Không tìm thấy bảng hoặc views nào được phép truy vấn theo cấu hình ALLOWED_TABLES trong database thực tế.\n"
                print(schema_description)

                return schema_description

            for table_name in allowed_physical_entities:
                schema_description += f"\n-Bảng '{table_name}':\n"

                columns = inspector.get_columns(table_name)

                allowed_columns = [
                    col for col in columns
                    if col['name'].lower() not in self.blacklisted_columns
                ]

                if not allowed_columns:
                    schema_description += "  Không có cột nào được phép hiển thị.\n"
                    continue

                for i, col in enumerate(allowed_columns):
                    col_name = col['name']
                    col_type = str(col['type'])
                    is_pk = "Khóa Chính" if col.get('primary_key') else ""

                    is_index = "Chỉ mục" if col.get('index') else ""
                    comment = col.get('comment') or ""

                    col_info = f"    {i + 1}\t{col_name}\t{is_pk}\t{is_index}\t{col_type}"

                    if comment:
                        col_info += f": {comment}"

                    schema_description += col_info + "\n"

            print(schema_description)
            return schema_description

        except Exception as e:

            print(f"Warning: Cannot retrieve valid database schema. Error: {e}")
            traceback.print_exc()
            schema_description += "Error retrieving schema details from database.\n"

            return schema_description


def format_results(results: List[Dict[str, Any]]) -> str:
    """
    Định dạng kết quả từ DB (List of Dict) thành chuỗi văn bản dạng bảng.
    Hàm này nhận kết quả TRỰC TIẾP TỪ DB.
    """
    if not results: return "Không có kết quả từ database."
    max_rows_for_llm = 15
    display_results = results[:max_rows_for_llm]
    formatted_string = "Results:\n"
    columns = list(display_results[0].keys())
    formatted_string += "| " + " | ".join(columns) + " |\n"
    col_widths = [max(len(col), 3) for col in columns]
    formatted_string += "|-" + "-|-".join(['-' * width for width in col_widths]) + "-|\n"

    for row_dict in display_results:
        row_values = []
        for col in columns:
            value = str(row_dict.get(col, 'NULL')) if row_dict.get(col) is not None else 'NULL'
            value = value.replace('|', '-')
            row_values.append(value)
        formatted_string += "| " + " | ".join(row_values) + " |\n"

    if len(results) > max_rows_for_llm:
        formatted_string += f"(... {len(results) - max_rows_for_llm} hàng khác bị ẩn ...)\n"

    return formatted_string
