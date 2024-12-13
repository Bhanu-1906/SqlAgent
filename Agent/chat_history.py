import mysql.connector
from datetime import datetime
import ast
from core.llm_manager import LLMManger
from Prompt.prompt_loader import PromptLoader


llm=LLMManger()

class ChatHistory:
    def __init__(self, host, user, password, database):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.conn.cursor(dictionary=True)
        except mysql.connector.Error as err:
            raise Exception(f"Error connecting to MySQL: {err}")

    def fetch_previous_conversations(self, user_id: str):
        try:
            # Fetch user details
            user_query = """
                SELECT name, `like` as likes, dislike, age
                FROM user_password2
                WHERE user_id = %s
            """
            self.cursor.execute(user_query, (user_id,))
            user_record = self.cursor.fetchone()

            if user_record:
                name = user_record.get('name', 'N/A')
                likes = user_record.get('likes', 'none')
                dislikes = user_record.get('dislike', 'none')
                age = user_record.get('age', 'N/A')
            else:
                name, likes, dislikes, age = 'N/A', 'none', 'none', 'N/A'

            user_details = {
                'name': name,
                'likes': f"({likes})" if likes != 'none' else likes,
                'dislikes': f"({dislikes})" if dislikes != 'none' else dislikes,
                'age': age
            }

            # Fetch recent events
            mysql_query = """
                SELECT content, context, created_at
                FROM user_events
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 5
            """
            self.cursor.execute(mysql_query, (user_id,))
            mysql_events_list = self.cursor.fetchall()

            events_list = [
                {'role': 'user', 'content': row['content'], 'context': row['context']}
                for row in mysql_events_list
            ]

            return [user_details, events_list]

        except mysql.connector.Error as err:
            return f"Error fetching data from MySQL: {err}"

    def insert(self, user_id: str, input_message: str, final_response_content: str, state: dict):
        try:
            system_message = PromptLoader().get_prompt("chat_history_system_prompt")
            messages = [{"role": "user", "content": input_message}]
            raw_output = llm.invoke([{"role": "system", "content": system_message}] + messages)
            output = raw_output.content.strip().replace("'''", "").replace("json", "").replace("```", "")

            result = {"ans_type": "events", "details": {}}
            try:
                detected_info = ast.literal_eval(output)
                if isinstance(detected_info, dict) and any(detected_info.values()):
                    result["ans_type"] = "personal_details"
                    result["details"] = detected_info
            except (ValueError, SyntaxError):
                result["details"] = {"message": output}

            # Insert or update user details
            if result.get('ans_type') == "personal_details":
                details = result.get('details', {})
                self.cursor.execute("SELECT name, age, `like`, dislike FROM user_password2 WHERE user_id = %s", (user_id,))
                existing_data = self.cursor.fetchone()

                if existing_data:
                    existing_name, existing_age, existing_likes, existing_dislikes = (
                        existing_data["name"] or '',
                        existing_data["age"] or '',
                        existing_data["like"] or '',
                        existing_data["dislike"] or ''
                    )
                    updated_likes = (existing_likes + ', ' + details.get('likes', '')).strip(', ')
                    updated_dislikes = (existing_dislikes + ', ' + details.get('dislikes', '')).strip(', ')

                    update_query = """
                        UPDATE user_password2
                        SET name = %s, age = %s, `like` = %s, dislike = %s
                        WHERE user_id = %s
                    """
                    self.cursor.execute(update_query, (
                        details.get('name', existing_name),
                        details.get('age', existing_age),
                        updated_likes,
                        updated_dislikes,
                        user_id
                    ))
                else:
                    insert_query = """
                        INSERT INTO user_password2 (user_id, password, name, `like`, dislike, age)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        user_id,
                        'default_password',
                        details.get('name', ''),
                        details.get('likes', ''),
                        details.get('dislikes', ''),
                        details.get('age', None)
                    ))

            # Insert user events into `user_events` table
            if result.get('ans_type') == "events" or result.get('ans_type') == "personal_details":
                event_query = """
                    INSERT INTO user_events (user_id, content, context, created_at)
                    VALUES (%s, %s, %s, %s)
                """
                self.cursor.execute(event_query, (
                    user_id,
                    input_message,
                    final_response_content,
                    datetime.now()
                ))

            self.conn.commit()
            return "Data inserted successfully."

        except mysql.connector.Error as err:
            return f"Database error: {err}"

    def close(self):
        if self.conn.is_connected():
            self.cursor.close()
            self.conn.close()