import os
import traceback
from typing import List, Dict, Any, Tuple, Union

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Function, TokenList
from sqlparse.tokens import Keyword, DML

from .llm_client import LlmClient
from .database import DatabaseConnector


class DatabaseChatbotService:

    def __init__(self, llm_client: LlmClient, db_context_path: str):
        """
        Khởi tạo DatabaseChatbotService.

        Args:
            llm_client: Instance của LlmClient.
            db_context_path: Đường dẫn đến file mô tả schema tĩnh (dùng làm fallback).
        """
        self.llm_client = llm_client

        self.db_connector = DatabaseConnector()
        self.db_context_path = db_context_path
        print("DatabaseChatbotService initialized.")

    def get_schema_description(self) -> str:
        """
        Lấy mô tả schema của các bảng được phép (bao gồm Views) từ DB thực tế.
        Kết hợp kết quả từ get_table_names() và get_view_names().
        Fallback sang file nếu lấy động thất bại hoặc không tìm thấy bảng/view được phép.
        """
        print("Attempting to get database schema description...")

        db_schema_description = self.db_connector.get_schema_description()

        if db_schema_description.startswith(
                "Error retrieving schema details") or "Không tìm thấy bảng hoặc views nào" in db_schema_description:
            print(
                f"Warning: Dynamic schema retrieval failed or found no allowed tables/views by inspector. Attempting to use schema from {self.db_context_path}")

            return self.get_schema_description_from_file()
        else:

            print("Dynamic schema description retrieved successfully.")
            return db_schema_description

    def get_schema_description_from_file(self) -> str:
        """
        Đọc mô tả schema từ file (fallback).
        """
        try:
            if os.path.exists(self.db_context_path):
                print(f"Successfully read DB context from: {self.db_context_path}")
                with open(self.db_context_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                print(
                    f"Warning: DB context file not found at {self.db_context_path}. Schema description will not include additional context.")

                return "Database schema description file not found or could not be read."
        except Exception as e:
            print(f"Error reading DB context file {self.db_context_path}: {e}")
            traceback.print_exc()
            return "Database schema description file not found or could not be read."

    def process_query(self, user_query: str) -> Union[Tuple[str, List[Dict[str, Any]], str], Tuple[str,]]:
        """
        Xử lý truy vấn từ người dùng: lấy schema, gọi LLM (lần 1 tạo SQL), xác thực SQL, thực thi SQL.
        Trả về tuple (sql_cleaned, raw_results_list, formatted_results_string) nếu thành công,
        hoặc tuple (error_message,) nếu có lỗi.
        """

        db_schema = self.get_schema_description()

        if not db_schema or db_schema.startswith("Error") or "Không tìm thấy bảng hoặc views nào" in db_schema:
            return (db_schema,)

        prompt = f"""
        You are a helpful assistant that can answer questions about the database by generating SQL queries.
        You can only query the tables and columns provided in the schema below.
        You must only generate SELECT statements.
        Do NOT generate queries with INSERT, UPDATE, DELETE, ALTER, DROP, CREATE, TRUNCATE, REPLACE, GRANT, REVOKE, UNION, INTO OUTFILE, DUMPFILE, or multiple statements separated by semicolons.
        Use only standard SQL syntax compatible with MySQL/MariaDB.
        Do NOT include comments in the SQL query.

        --- Instructions for SQL Generation ---
        - When comparing string values, especially for names (like event name, organization name), use the LIKE operator for partial or fuzzy matching.
        - Enclose LIKE patterns in single quotes and use the '%' wildcard at the beginning and end of the pattern for matching sequences of characters (e.g., WHERE LOWER(name) LIKE '%partial name%').
        - To make comparisons case-insensitive, convert both the database column and the string value from the user question to lowercase before using LIKE (e.mb_schema.g., WHERE LOWER(name) LIKE '%partial name%').
        - Understand table/view relationships: Note the common columns like 'event_id' linking 'events_view' and 'results_view', and 'organization_id' linking 'events_view' and 'organizations_view'.
        - To find information that requires combining data from multiple tables/views, you MUST JOIN the necessary tables/views on their common ID columns.
        - **Hướng dẫn Cột SELECT Chi tiết:**
          - Khi truy vấn thông tin về **sự kiện** từ 'events_view' và kết quả cần được liệt kê chi tiết hoặc hiển thị thông tin, hãy **LUÔN bao gồm các cột sau** trong mệnh đề SELECT: 'event_id', 'name', 'description', 'location', 'start_date', 'end_date', 'quantity_now', 'max_quantity', 'image'. Bao gồm cả các cột khác được yêu cầu cụ thể trong câu hỏi người dùng nếu có.
          - Khi truy vấn thông tin về **kết quả sự kiện** từ 'results_view', hãy **LUÔN bao gồm các cột sau** trong mệnh đề SELECT: 'result_id', 'content' AS result_description, 'images' AS result_image. Nếu cần thông tin về sự kiện liên quan, hãy JOIN 'events_view' và bao gồm thêm 'e.event_id', 'e.name' AS event_name, 'e.description' AS event_description, 'e.image' AS event_image từ events_view với bí danh 'e'.

        {db_schema}

        --- Examples ---
        User question: Các sự kiện nào còn chỗ đăng kí?
        SQL query: SELECT event_id, name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE quantity_now < max_quantity; -- Bao gồm các cột cần thiết

        User question: Sự kiện nào sắp hết lượt đăng kí?
        SQL query: SELECT event_id, name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE quantity_now >= max_quantity * 0.75 AND quantity_now < max_quantity ORDER BY (max_quantity - quantity_now) ASC; -- Tìm sự kiện đầy từ 75% trở lên, sắp xếp sự kiện ít chỗ trống nhất lên đầu

        User question: Tìm sự kiện còn nhiều chỗ đăng kí.
        SQL query: SELECT event_id, name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE max_quantity - quantity_now > 20; -- Bao gồm các cột cần thiết

        User question: Sự kiện nào đã đầy lượt đăng kí?
        SQL query: SELECT event_id, name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE quantity_now >= max_quantity; -- Bao gồm các cột cần thiết

        User question: Sự kiện "Hoi nghi cong nghe" của nhà tổ chức nào?
        SQL query: SELECT e.name AS event_name, o.username AS organization_name, e.event_id, e.description AS event_description, e.location, e.start_date, e.end_date, e.quantity_now, e.max_quantity, e.image AS event_image FROM events_view AS e JOIN organizations_view AS o ON e.organization_id = o.organization_id WHERE LOWER(e.name) LIKE '%hoi nghi cong nghe%'; -- Bao gồm các cột cần thiết từ event và tổ chức, dùng alias cho rõ

        User question: Có kết quả của sự kiện "Fuga quia beatae" chưa?
        SQL query: SELECT r.result_id, r.content AS result_description, r.images AS result_image, e.event_id, e.name AS event_name, e.description AS event_description, e.image AS event_image FROM results_view r JOIN events_view e ON r.event_id = e.event_id WHERE LOWER(e.name) LIKE '%fuga quia beatae%'; -- Bao gồm các cột cần thiết từ kết quả và sự kiện liên quan

        User question: Liệt kê các sự kiện diễn ra tại "Hà Nội".
        SQL query: SELECT event_id, event_name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE LOWER(location) LIKE '%hà nội%'; -- Bao gồm các cột cần thiết
        
        User question: Các sự kiện nào vừa kết thúc?
        SQL query: SELECT event_id, name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE end_date < NOW() AND end_date >= NOW() - INTERVAL '7' DAY ORDER BY end_date DESC LIMIT 10; -- Tìm các sự kiện có end_date trong vòng 7 ngày qua và đã kết thúc (end_date < NOW()), sắp xếp theo ngày kết thúc gần nhất

        User question: Các sự kiện nào sắp diễn ra?
        SQL query: SELECT event_id, name, description, location, start_date, end_date, quantity_now, max_quantity, image FROM events_view WHERE start_date > NOW() ORDER BY start_date ASC LIMIT 10; -- Tìm các sự kiện có start_date trong tương lai (start_date > NOW()), sắp xếp theo ngày bắt đầu sớm nhất

        --- End Examples ---

        Based on the user's question, generate a single SQL SELECT query using the schema and following the instructions and examples.

        User question: {user_query}
        SQL query:
        """
        print(f"Sending prompt to LLM (SQL Generation)...")
        try:
            raw_sql = self.llm_client.generate_text(prompt)
            print(f"Raw SQL generated by LLM: {raw_sql}")
        except Exception as e:
            print(f"Error calling LLM (SQL Generation): {e}")
            traceback.print_exc()

            return ("Xin lỗi, tôi gặp sự cố khi tạo truy vấn SQL.",)

        sql_cleaned = clean_sql_query(raw_sql)
        print(f"Cleaned SQL query: {sql_cleaned}")

        if not is_valid_sql(sql_cleaned):
            print(f"SQL Validation Failed for query: ```sql\n{sql_cleaned}\n```")
            return ("Xin lỗi, truy vấn SQL được tạo ra không hợp lệ hoặc bị cấm vì lý do bảo mật.",)

        print(f"Executing validated SQL query: {sql_cleaned}")

        raw_results = self.db_connector.execute_query(sql_cleaned)

        formatted_results_string = format_results(raw_results)
        print(f"Formatted results string for LLM2:\n```\n{formatted_results_string}\n```")

        return (sql_cleaned, raw_results, formatted_results_string)


from .sql_utils import is_valid_sql, format_results, clean_sql_query
