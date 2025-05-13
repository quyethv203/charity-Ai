from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage


class LlmClient:
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model

        if not self.api_key:
            raise ValueError('GEMINI_API_KEY chưa được cung cấp.')

        try:
            self._client = ChatGoogleGenerativeAI(google_api_key=self.api_key, model=self.model)
        except Exception as e:
            print(f"ERROR: Loi khi khoi tao Chat Model Gemini: {e}")
            raise e

    def generate_text(self, prompt: str) -> str:
        try:
            messages = [HumanMessage(content=prompt)]
            response = self._client.invoke(messages)
            if response and response.content:
                return response.content.strip()
            else:
                return "Không nhận được phản hồi dạng văn bản từ LLM (có thể do nội dung không phù hợp)."
        except Exception as e:
            print(f"Lỗi khi gọi API LLM qua LangChain: {e}")
            raise
