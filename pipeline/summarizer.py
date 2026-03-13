from config.prompts import SUMMARIZATION_PROMPT
from core.llm import run_inference


def summarize_with_llm(abstract: str) -> str:
    if not abstract.strip():
        return "No abstract available to summarize."

    prompt = SUMMARIZATION_PROMPT.format(abstract=abstract)
    return run_inference(prompt, task="summarization")