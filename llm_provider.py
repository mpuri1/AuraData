"""
AuraData LLM Provider Factory
==============================
Provides a runtime-configurable LLM instance for the LangGraph pipeline.

Set LLM_PROVIDER in .env to switch between providers:
  LLM_PROVIDER=openai   → ChatOpenAI  (default, requires OPENAI_API_KEY)
  LLM_PROVIDER=bedrock  → ChatBedrock (requires AWS credentials or IAM role)

No code changes required to switch — only .env changes.

AWS Bedrock models (set via BEDROCK_MODEL_ID):
  - anthropic.claude-3-haiku-20240307-v1:0   (fast, cost-optimized — default)
  - anthropic.claude-3-5-sonnet-20240620-v1:0 (highest quality)
  - amazon.titan-text-express-v1              (AWS-native alternative)
"""

import os
from langchain_core.language_models import BaseLanguageModel
from dotenv import load_dotenv

load_dotenv()


def get_llm(temperature: float = 0) -> BaseLanguageModel:
    """
    Returns a configured LangChain LLM instance based on LLM_PROVIDER env var.

    Args:
        temperature: Sampling temperature. Use 0 for deterministic data
                     correction tasks; higher values for creative generation.

    Returns:
        ChatOpenAI or ChatBedrock instance, ready for use in LCEL chains.

    Usage:
        from llm_provider import get_llm
        llm = get_llm()                    # uses LLM_PROVIDER from .env
        chain = prompt | llm               # standard LCEL pattern
        response = chain.invoke({...})
    """
    provider = os.getenv("LLM_PROVIDER", "openai").lower().strip()

    if provider == "bedrock":
        return _get_bedrock_llm(temperature)

    # Default: OpenAI
    return _get_openai_llm(temperature)


def _get_openai_llm(temperature: float) -> BaseLanguageModel:
    """
    Returns a ChatOpenAI instance.
    Requires: OPENAI_API_KEY in .env
    """
    from langchain_openai import ChatOpenAI

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    return ChatOpenAI(
        model=model,
        temperature=temperature,
        # api_key read automatically from OPENAI_API_KEY env var
    )


def _get_bedrock_llm(temperature: float) -> BaseLanguageModel:
    """
    Returns a ChatBedrock instance via Amazon Bedrock.
    Requires: AWS credentials in .env OR IAM role attached to the runtime.

    Authentication options (in priority order):
      1. IAM role (recommended for ECS/EC2 — no credentials in .env)
      2. AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in .env
      3. AWS CLI profile (~/.aws/credentials)
    """
    from langchain_aws import ChatBedrock

    model_id = os.getenv(
        "BEDROCK_MODEL_ID",
        "anthropic.claude-3-haiku-20240307-v1:0"  # cost-optimized default
    )
    region = os.getenv("AWS_REGION", "us-east-1")

    return ChatBedrock(
        model_id=model_id,
        region_name=region,
        model_kwargs={
            "temperature": temperature,
            "max_tokens": 2048,
            "anthropic_version": "bedrock-2023-05-31",
        },
    )


def get_provider_info() -> dict:
    """
    Returns metadata about the currently configured LLM provider.
    Useful for logging and observability.
    """
    provider = os.getenv("LLM_PROVIDER", "openai").lower().strip()

    if provider == "bedrock":
        return {
            "provider": "bedrock",
            "model": os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0"),
            "region": os.getenv("AWS_REGION", "us-east-1"),
        }

    return {
        "provider": "openai",
        "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
    }
