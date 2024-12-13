import os

class PromptLoader:
    PROMPT_DIR = os.path.join(os.path.dirname(__file__))

    @staticmethod
    def get_prompt(prompt_name):
        try:
            prompt_file = os.path.join(PromptLoader.PROMPT_DIR, f"{prompt_name}.txt")
            
            if not os.path.exists(prompt_file):
                raise FileNotFoundError(f"{prompt_file} does not exist")

            with open(prompt_file, 'r') as f:
                return f.read().strip()
            
        except Exception as e:
            return f"Error loading prompt {prompt_name}: {str(e)}"
