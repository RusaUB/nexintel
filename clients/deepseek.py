from __future__ import annotations
import os, json
from typing import Any, Dict, List, Optional
from openai import OpenAI 
from dotenv import load_dotenv

load_dotenv()

DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL    = "deepseek-chat"   

class DeepSeek:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        default_model: str = DEFAULT_MODEL,
    ):
        key = os.getenv("DEEPSEEK_API_KEY") 
        if not key:
            raise RuntimeError("Set DEEPSEEK_API_KEY or pass api_key explicitly.")
        self.client = OpenAI(api_key=key, base_url=base_url)
        self.default_model = default_model

    def json_chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 1200,
    ) -> Any:
        resp = self.client.chat.completions.create(
            model=model or self.default_model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            stream=False,
        )
        text = resp.choices[0].message.content
        return json.loads(text)