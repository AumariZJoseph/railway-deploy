import logging
import asyncio
import time
from typing import List, Dict, Any
from dotenv import load_dotenv

from services.supabase_client import supabase_client
from services.fast_embedding_service import fast_embedding_service
from services.error_handler import (
    ResourceNotFoundError,
    ExternalServiceError,
    ValidationError,
    RateLimitError,
    log_error
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()


class QueryService:
    def __init__(self):
        self.conversation_context = {}
        self.groq_rate_limits = {
            "last_request_time": 0,
            "min_interval": 1.0,  # Minimum 1 second between requests
            "max_requests_per_minute": 30,  # Groq's typical rate limit
            "request_times": []
        }
        
        try:
            from llama_index.llms.groq import Groq as GroqLLM
            self.llm = GroqLLM(
                model="meta-llama/llama-4-scout-17b-16e-instruct",
                temperature=0.1,
                max_tokens=1024,
                timeout=30.0
            )
            logger.info("QueryService initialized with Groq LLM and rate limiting")
        except Exception as e:
            logger.error(f"Error initializing QueryService: {str(e)}")
            raise ExternalServiceError(
                f"Failed to initialize query service: {str(e)}",
                "AI service is temporarily unavailable. Please try again later."
            )

    async def query_documents(self, user_id: str, question: str) -> Dict[str, Any]:
        """Query user's documents with comprehensive error and rate limit handling"""
        try:
            # -----------------------------
            # Input validation
            # -----------------------------
            if not user_id or not isinstance(user_id, str):
                raise ValidationError("Invalid user ID", "Invalid user identifier.")

            if not question or not isinstance(question, str) or len(question.strip()) < 2:
                raise ValidationError("Invalid question", "Please provide a valid question (at least 2 characters).")

            if len(question) > 5000:
                raise ValidationError("Question too long", "Question is too long. Please keep it under 5000 characters.")

            # -----------------------------
            # Fetch documents
            # -----------------------------
            documents = supabase_client.get_active_documents(user_id)
            if not documents:
                raise ResourceNotFoundError(
                    f"No documents found for user {user_id}",
                    "I don't have a knowledge base yet. Please upload documents first."
                )

            logger.info(f"Querying documents for user {user_id}: {question}")

            # -----------------------------
            # Rate limit check
            # -----------------------------
            await self._apply_groq_rate_limit()

            # -----------------------------
            # Generate question embedding
            # -----------------------------
            try:
                question_embedding = await fast_embedding_service.get_embedding(question)
                logger.info(f"Generated question embedding with BAAI/bge-small-en-v1.5")
            except Exception as e:
                raise ExternalServiceError(
                    f"Embedding generation failed: {str(e)}",
                    "Unable to process your question at the moment. Please try again."
                )

            # -----------------------------
            # Search for similar chunks
            # -----------------------------
            try:
                similar_chunks = supabase_client.search_similar_chunks(
                    user_id, question_embedding, limit=10
                )
                logger.info(f"Found {len(similar_chunks)} potential chunks")
            except Exception as e:
                raise ExternalServiceError(
                    f"Vector search failed: {str(e)}",
                    "Search service is temporarily unavailable. Please try again."
                )

            # -----------------------------
            # Filter chunks by similarity
            # -----------------------------
            filtered_chunks = [
                chunk for chunk in similar_chunks
                if chunk.get('similarity', 0) > 0.4
            ]
            logger.info(f"After filtering: {len(filtered_chunks)} relevant chunks")

            if not filtered_chunks:
                return {
                    "success": True,
                    "answer": "I don't have information about this in my knowledge base. Please try asking about the content of your uploaded documents.",
                    "sources": []
                }

            # Take top 5
            similar_chunks = filtered_chunks[:5]

            # -----------------------------
            # Build enhanced context
            # -----------------------------
            context_str, sources = self._build_enhanced_context(similar_chunks)

            # -----------------------------
            # Generate answer with retry
            # -----------------------------
            try:
                answer = await self._generate_enhanced_answer_with_retry(question, context_str, user_id)
            except RateLimitError:
                raise RateLimitError(
                    "Groq API rate limit exceeded",
                    "I'm getting too many requests right now. Please wait a moment and try again."
                )
            except Exception as e:
                raise ExternalServiceError(
                    f"LLM response generation failed: {str(e)}",
                    "I'm having trouble generating a response right now. Please try again."
                )

            # -----------------------------
            # Save chat history
            # -----------------------------
            try:
                source_files = list(set([
                    chunk.get('metadata', {}).get('source', 'Unknown')
                    for chunk in similar_chunks
                ]))
                supabase_client.save_chat_history(user_id, question, answer, source_files)
            except Exception as e:
                logger.warning(f"Failed to save chat history: {str(e)}")

            return {
                "success": True,
                "answer": answer,
                "sources": source_files,
                "chunks_used": len(similar_chunks)
            }

        except (ValidationError, ResourceNotFoundError, ExternalServiceError, RateLimitError):
            raise
        except Exception as e:
            log_error(e, user_id, "query_documents", {"question_length": len(question)})
            raise ExternalServiceError(
                f"Unexpected error during query: {str(e)}",
                "An unexpected error occurred while processing your question. Please try again."
            )

    # ======================================================
    # Rate Limiting
    # ======================================================
    async def _apply_groq_rate_limit(self):
        """Apply rate limiting for Groq API calls"""
        current_time = time.time()

        # Remove timestamps older than 1 minute
        self.groq_rate_limits["request_times"] = [
            t for t in self.groq_rate_limits["request_times"]
            if current_time - t < 60
        ]

        # Check max requests per minute
        if len(self.groq_rate_limits["request_times"]) >= self.groq_rate_limits["max_requests_per_minute"]:
            wait_time = 60 - (current_time - self.groq_rate_limits["request_times"][0])
            logger.warning(f"Groq rate limit exceeded. Waiting {wait_time:.1f} seconds")
            raise RateLimitError(
                f"Groq rate limit exceeded. Wait {wait_time:.1f} seconds",
                "I'm processing too many requests right now. Please wait a moment and try again."
            )

        # Ensure minimum interval between requests
        time_since_last = current_time - self.groq_rate_limits["last_request_time"]
        if time_since_last < self.groq_rate_limits["min_interval"]:
            wait_time = self.groq_rate_limits["min_interval"] - time_since_last
            await asyncio.sleep(wait_time)

        # Update rate tracking
        self.groq_rate_limits["last_request_time"] = time.time()
        self.groq_rate_limits["request_times"].append(time.time())

    # ======================================================
    # Answer Generation with Retry
    # ======================================================
    async def _generate_enhanced_answer_with_retry(self, question: str, context: str, user_id: str) -> str:
        """Generate answer with retry logic for transient failures"""
        max_retries = 2
        retry_delay = 1.0

        for attempt in range(max_retries + 1):
            try:
                conversation_history = self._get_conversation_history(user_id)

                prompt = f"""
                You are an expert document analysis assistant. Answer ONLY using information from the provided context.

                IMPORTANT:
                1. If information is not in the context, say "I don't have information about this in my knowledge base."
                2. Provide detailed answers with specific examples from the source material.
                3. Always cite your sources with document names.

                {conversation_history}

                CONTEXT INFORMATION:
                {context}

                QUESTION: {question}

                Provide a thorough, well-structured answer with source references.
                """

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, self.llm.complete, prompt)
                answer = str(response).strip()

                self._update_conversation_context(user_id, question, answer)
                return answer

            except Exception as e:
                error_msg = str(e).lower()
                if "429" in error_msg or "rate limit" in error_msg or "too many requests" in error_msg:
                    if attempt < max_retries:
                        logger.warning(f"Groq rate limit hit, retrying in {retry_delay}s (attempt {attempt + 1})")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    else:
                        raise RateLimitError(
                            "Groq API rate limit exceeded after retries",
                            "The AI service is currently very busy. Please wait a minute and try again."
                        )
                elif "timeout" in error_msg or "timed out" in error_msg:
                    if attempt < max_retries:
                        logger.warning(f"Groq timeout, retrying in {retry_delay}s (attempt {attempt + 1})")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        raise ExternalServiceError(
                            "Groq API timeout after retries",
                            "The AI service is taking too long to respond. Please try again."
                        )
                else:
                    raise

        raise ExternalServiceError(
            "Maximum retries exceeded for Groq API",
            "The AI service is currently unavailable. Please try again later."
        )

    # ======================================================
    # Helper Methods (Conversation & Context)
    # ======================================================
    def _build_enhanced_context(self, chunks: List[Dict[str, Any]]) -> tuple:
        context_str = ""
        sources = set()
        for i, chunk in enumerate(chunks):
            source = chunk.get('metadata', {}).get('source', 'Unknown document')
            sources.add(source)
            context_str += f"[Source {i + 1}: {source}]\n{chunk['chunk_text']}\n\n"
        return context_str, list(sources)

    def _update_conversation_context(self, user_id: str, question: str, answer: str):
        if user_id not in self.conversation_context:
            self.conversation_context[user_id] = []
        self.conversation_context[user_id].append({"question": question, "answer": answer[:500]})
        if len(self.conversation_context[user_id]) > 5:
            self.conversation_context[user_id] = self.conversation_context[user_id][-5:]

    def _get_conversation_history(self, user_id: str) -> str:
        if user_id not in self.conversation_context or not self.conversation_context[user_id]:
            return ""
        history = "CONVERSATION HISTORY:\n"
        for exchange in self.conversation_context[user_id]:
            history += f"Q: {exchange['question']}\nA: {exchange['answer']}...\n\n"
        return history

    def clear_conversation_context(self, user_id: str):
        if user_id in self.conversation_context:
            self.conversation_context[user_id] = []


# Global instance
query_service = QueryService()
