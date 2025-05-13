import os
import traceback
from typing import List, Dict, Any, Tuple, Union

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Function, TokenList
from sqlparse.tokens import Keyword, DML, Punctuation

from .constants import ALLOWED_TABLES


def clean_sql_query(sql: str) -> str:
    """
    Làm sạch chuỗi SQL được tạo ra bởi LLM, loại bỏ định dạng Markdown code block.
    """
    if not isinstance(sql, str):
        return ""

    sql_cleaned = sql.strip()

    if sql_cleaned.lower().startswith('```sql'):
        end_of_marker = sql_cleaned.lower().find('```sql') + len('```sql')
        sql_cleaned = sql_cleaned[end_of_marker:].lstrip()
    if sql_cleaned.lower().endswith('```'):
        start_of_marker = sql_cleaned.lower().rfind('```')
        sql_cleaned = sql_cleaned[:start_of_marker].rstrip()

    return sql_cleaned


def is_valid_sql(sql_cleaned: str) -> bool:
    """
    *** KIỂM TRA TÍNH AN TOÀN VÀ QUYỀN TRUY CẬP CỦA Câu lệnh SQL (Đơn giản hóa mới, CÓ DEBUG LOG) ***
    Chỉ kiểm tra các BẢNG/VIEWS được phép truy vấn và các lệnh nguy hiểm.
    Sử dụng sqlparse để kiểm tra bảng được tham chiếu bằng cách lấy tên thật từ Identifier, bỏ qua bí danh.
    """
    print(f"DEBUG VALIDATION: Starting validation for: {sql_cleaned}")

    if not isinstance(sql_cleaned, str) or not sql_cleaned.strip():
        print(f"Blocked SQL (empty or invalid input after cleaning): {sql_cleaned}")
        return False

    sql_lower = sql_cleaned.lower().strip()
    forbidden_keywords_or_patterns = [
        ' insert ', ' update ', ' delete ', ' alter ', ' create ', ' drop ',
        ' truncate ', ' replace ', ' grant ', ' revoke ', '--', '/*', '*/',
        ' union', ' into ', ' outfile ', ' dumpfile '
    ]
    if any(keyword in sql_lower for keyword in forbidden_keywords_or_patterns):
        print(f"Blocked SQL (contains forbidden parts): {sql_cleaned}")
        return False

    if ';' in sql_cleaned.strip(' ;'):
        print(f"Blocked SQL (multiple statements detected): {sql_cleaned}")
        return False

    try:
        statements = sqlparse.parse(sql_cleaned)
        if len(statements) != 1:
            print(f"Blocked SQL (sqlparse check: expected 1 statement, got {len(statements)}): {sql_cleaned}")
            return False

        statement = statements[0]

        if statement.get_type() != 'SELECT':
            print(
                f"Blocked SQL (sqlparse check: statement type is not SELECT): {sql_cleaned} - Type: {statement.get_type()}")
            return False

        referenced_table_names_lower = set()
        iterator = iter(statement.tokens)
        print("DEBUG VALIDATION: Iterating tokens to find FROM/JOIN...")
        try:
            while True:
                token = next(iterator)

                if token.is_whitespace: continue

                if token.ttype is Keyword and token.value.upper() in ['FROM', 'JOIN']:
                    print(
                        f"DEBUG VALIDATION: Found keyword {token.value.upper()}. Looking for next token...")

                    next_token = None
                    try:

                        while True:
                            next_token = next(iterator)
                            if not next_token.is_whitespace:
                                print(
                                    f"DEBUG VALIDATION: Found next token after {token.value.upper()}: Type={type(next_token)}, TType={next_token.ttype}, Value='{next_token.value}'")
                                break
                        if next_token is None:
                            print(f"DEBUG VALIDATION: StopIteration before finding next token.")
                            return False
                    except StopIteration:
                        print(
                            f"DEBUG VALIDATION: StopIteration after {token.value.upper()}. No table name found.")
                        print(f"Blocked SQL (sqlparse check: missing table/view name after FROM/JOIN): {sql_cleaned}")
                        return False

                    names_to_add = set()

                    print(
                        f"DEBUG VALIDATION: Processing token immediately after FROM/JOIN: Type={type(next_token)}, TType={next_token.ttype}, Value='{next_token.value}'")

                    if isinstance(next_token, Identifier):

                        real_name = next_token.get_name()
                        print(f"DEBUG VALIDATION:   Is Identifier. get_name()='{real_name}'")
                        if real_name:
                            names_to_add.add(real_name.lower())
                            print(f"DEBUG VALIDATION:   Added '{real_name.lower()}' to names_to_add.")

                        continue

                    elif isinstance(next_token, IdentifierList):

                        print(f"DEBUG VALIDATION:   Is IdentifierList. Iterating its tokens...")

                        for list_token in next_token.tokens:
                            print(
                                f"DEBUG VALIDATION:     Processing token in IdentifierList: Type={type(list_token)}, TType={list_token.ttype}, Value='{list_token.value}'")

                            if list_token.is_whitespace or list_token.ttype in (
                                    sqlparse.tokens.Punctuation, sqlparse.tokens.Keyword):
                                continue

                            if isinstance(list_token, Identifier):
                                real_name = list_token.get_name()
                                print(
                                    f"DEBUG VALIDATION:       Found Identifier in list '{list_token.value}'. get_name()='{real_name}'")

                                if real_name:
                                    names_to_add.add(real_name.lower())
                                    print(
                                        f"DEBUG VALIDATION:       Added '{real_name.lower()}' to names_to_add.")


                    elif isinstance(next_token, Parenthesis):

                        print(
                            f"DEBUG VALIDATION:   Found Parenthesis after FROM/JOIN. Skipping table extraction from complex structure.")
                        pass

                    referenced_table_names_lower.update(names_to_add)
                    print(
                        f"DEBUG VALIDATION: Current referenced_table_names_lower after processing next_token: {referenced_table_names_lower}")








        except StopIteration:

            print("DEBUG VALIDATION: StopIteration reached.")
            pass
        except Exception as e:
            print(f"DEBUG VALIDATION: Error during table/view extraction loop: {e}")
            print(f"Blocked SQL (sqlparse check: error during table/view extraction): {sql_cleaned} - {e}")
            traceback.print_exc()
            return False

        print(
            f"DEBUG VALIDATION: Final referenced_table_names_lower before check: {referenced_table_names_lower}")
        for table_name in referenced_table_names_lower:

            if table_name not in ALLOWED_TABLES:
                print(f"Blocked SQL (sqlparse check: accesses disallowed table/view '{table_name}'): {sql_cleaned}")
                return False

        print(f"SQL validation passed (sqlparse check on allowed tables/views): {sql_cleaned}")
        return True

    except Exception as e:
        print(f"DEBUG VALIDATION: Error during initial sqlparse parsing/checking: {e}")
        print(f"Blocked SQL (sqlparse check: error during parsing/checking): {sql_cleaned} - {e}")
        traceback.print_exc()
        return False


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
