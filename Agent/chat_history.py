import mysql.connector
from datetime import datetime
import ast
from core.llm_manager import LLMManger
from Prompt.prompt_loader import PromptLoader
from Database import database_utils

llm = LLMManger()

class ChatHistory:
    def __init__(self, conn):
        try:
            if conn.is_connected():
                self.conn = conn
                self.cursor = self.conn.cursor(dictionary=True)
            else:
                raise Exception("Connection is not valid")
        except mysql.connector.Error as err:
            raise Exception(f"Error connecting to MySQL: {err}")

    def fetch_previous_conversations(self, user_id: str):
        try:
            user_query = """
                SELECT users.user_name AS name, user_details.likes, user_details.dislikes, user_details.age
                FROM user_details
                JOIN users ON user_details.user_id = users.id
                WHERE user_details.user_id = %s
            """
            self.cursor.execute(user_query, (user_id,))
            user_record = self.cursor.fetchone()

            if user_record:
                name = user_record.get('name', 'N/A')
                likes = user_record.get('likes', 'none')
                dislikes = user_record.get('dislikes', 'none')
                age = user_record.get('age', 'N/A')
            else:
                name, likes, dislikes, age = 'N/A', 'none', 'none', 'N/A'

            user_details = {
                'name': name,
                'likes': f"({likes})" if likes != 'none' else likes,
                'dislikes': f"({dislikes})" if dislikes != 'none' else dislikes,
                'age': age
            }
            chat_query = """
                SELECT message, is_bot, created_at
                FROM chat_history
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 10
            """
            self.cursor.execute(chat_query, (user_id,))
            chat_history = self.cursor.fetchall()

            chat_list = [
                {'role': 'assistant' if row['is_bot'] else 'user', 'content': row['message']}
                for row in reversed(chat_history)  
            ]
            events_list = [
                {'role': msg['role'], 'content': msg['content']}  
                for msg in chat_list
            ]
            

            return [user_details, events_list]

        except mysql.connector.Error as err:
            return f"Error fetching data from MySQL: {err}"

    def insert(self, user_id: str, input_message: str, final_response_content: str):
        try:
            system_message = PromptLoader().get_prompt("chat_history_system_prompt")
            messages = [{"role": "human", "content": input_message}]
            raw_output = llm.invoke([{"role": "system", "content": system_message}] + messages)
            output = raw_output.content.strip().replace("'''", "").replace("json", "").replace("\n", "")

            result = {"ans_type": "events", "details": {}}
            try:
                detected_info = ast.literal_eval(output)
                if isinstance(detected_info, dict) and any(detected_info.values()):
                    result["ans_type"] = "personal_details"
                    result["details"] = detected_info
            except (ValueError, SyntaxError):
                result["details"] = {"message": output}
            self.cursor.execute("SELECT user_name FROM users WHERE id = %s", (user_id,))
            user_record = self.cursor.fetchone()
            user_name = user_record['user_name'] if user_record else 'N/A'
            
            if result.get('ans_type') == "personal_details":
                details = result.get('details', {})
                self.cursor.execute("SELECT * FROM user_details WHERE user_id = %s", (user_id,))
                existing_data = self.cursor.fetchone()

                if existing_data:
                    updated_likes = (existing_data['likes'] or '') + ', ' + details.get('likes', '')
                    updated_dislikes = (existing_data['dislikes'] or '') + ', ' + details.get('dislikes', '')

                    update_query = """
                        UPDATE user_details
                        SET likes = %s, dislikes = %s, age = %s
                        WHERE user_id = %s
                    """
                    self.cursor.execute(update_query, (
                        updated_likes.strip(', '),
                        updated_dislikes.strip(', '),
                        details.get('age', existing_data['age']),
                        user_id
                    ))
                else:
                   
                    insert_query = """
                        INSERT INTO user_details (user_id, likes, dislikes, age)
                        VALUES (%s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        user_id,
                        details.get('likes', ''),
                        details.get('dislikes', ''),
                        details.get('age', None)
                    ))

            else: 
                if input_message.strip():  
                    chat_query = """
                        INSERT INTO chat_history (user_id, message, is_bot, created_at)
                        VALUES (%s, %s, %s, %s)
                    """
                    self.cursor.execute(chat_query, (user_id, input_message, 0, datetime.now()))
                if final_response_content.strip():  
                    bot_response_query = """
                        INSERT INTO chat_history (user_id, message, is_bot, created_at)
                        VALUES (%s, %s, %s, %s)
                    """
                    self.cursor.execute(bot_response_query, (user_id, final_response_content, 1, datetime.now()))

            self.conn.commit()
            return "Data inserted successfully."

        except mysql.connector.Error as err:
            return f"Database error: {err}"

    def close(self):
        if self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
