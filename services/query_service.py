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
            "min_interval": 1.0,
            "max_requests_per_minute": 30,
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

    # ======================================================
    # PUBLIC: Query entry point WITH TRIAL LIMIT LOGIC
    # ======================================================
    async def query_documents(self, user_id: str, question: str) -> Dict[str, Any]:
        """Query with trial limit check (20 free queries)"""
        try:
            # -------------------------
            # Trial / free-query limit
            # -------------------------
            user_settings = supabase_client.get_user_settings(user_id)
            query_count = user_settings.get("query_count", 0)

            if query_count >= 20:
                return {
                    "success": False,
                    "answer": (
                        "🚀 **Free trial limit reached!**\n\n"
                        "You've used your 20 free queries.\n"
                        "Join our waitlist to be notified when the full version launches!"
                    ),
                    "trial_ended": True
                }

            # -------------------------
            # Process main query logic
            # -------------------------
            result = await self._process_query(user_id, question)

            # -------------------------
            # Only increment if successful
            # -------------------------
            supabase_client.increment_query_count(user_id)

            return result

        except Exception as e:
            log_error(e, user_id, "query_documents", {"question_length": len(question)})
            raise ExternalServiceError(
                f"Unexpected error during query: {str(e)}",
                "An unexpected error occurred while processing your question. Please try again."
            )

    # ======================================================
    # INTERNAL: Original logic moved here cleanly
    # ======================================================
    async def _process_query(self, user_id: str, question: str) -> Dict[str, Any]:
        """Original full query logic (moved from query_documents)"""

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
        # Generate embedding
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
        # Search DB
        # -----------------------------
        try:
            similar_chunks = supabase_client.search_similar_chunks(
                user_id, question_embedding, limit=15
            )
            logger.info(f"Found {len(similar_chunks)} potential chunks")
        except Exception as e:
            raise ExternalServiceError(
                f"Vector search failed: {str(e)}",
                "Search service is temporarily unavailable. Please try again."
            )

        # -----------------------------
        # Filter low similarity
        # -----------------------------
        filtered_chunks = [
            chunk for chunk in similar_chunks
            if chunk.get("similarity", 0) > 0.25
        ]

        logger.info(f"After filtering: {len(filtered_chunks)} relevant chunks")

        if not filtered_chunks:
            return {
                "success": True,
                "answer": (
                    "I don't have information about this in your knowledge base. "
                    "Try asking about the content inside your uploaded documents."
                ),
                "sources": []
            }

        similar_chunks = filtered_chunks[:8]

        # -----------------------------
        # Build rich cross-document context
        # -----------------------------
        context_str, sources = self._build_cross_document_context(similar_chunks)

        # -----------------------------
        # Generate answer
        # -----------------------------
        try:
            answer = await self._generate_enhanced_answer_with_retry(
                question, context_str, user_id
            )
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
                chunk.get("metadata", {}).get("source", "Unknown")
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

    # ======================================================
    # RATE LIMITING
    # ======================================================
    async def _apply_groq_rate_limit(self):
        current_time = time.time()

        # Remove timestamps older than 1 minute
        self.groq_rate_limits["request_times"] = [
            t for t in self.groq_rate_limits["request_times"]
            if current_time - t < 60
        ]

        # Max requests per minute
        if len(self.groq_rate_limits["request_times"]) >= self.groq_rate_limits["max_requests_per_minute"]:
            wait_time = 60 - (current_time - self.groq_rate_limits["request_times"][0])
            logger.warning(f"Groq rate limit exceeded. Waiting {wait_time:.1f} seconds")
            raise RateLimitError(
                f"Groq rate limit exceeded. Wait {wait_time:.1f} seconds",
                "I'm processing too many requests right now. Please wait a moment and try again."
            )

        # Ensure minimum spacing
        time_since_last = current_time - self.groq_rate_limits["last_request_time"]
        if time_since_last < self.groq_rate_limits["min_interval"]:
            await asyncio.sleep(self.groq_rate_limits["min_interval"] - time_since_last)

        now = time.time()
        self.groq_rate_limits["last_request_time"] = now
        self.groq_rate_limits["request_times"].append(now)

    # ======================================================
    # ANSWER GENERATION WITH RETRY
    # ======================================================
    async def _generate_enhanced_answer_with_retry(self, question: str, context: str, user_id: str) -> str:
        max_retries = 2
        retry_delay = 1.0

        for attempt in range(max_retries + 1):
            try:
                conversation_history = self._get_conversation_history(user_id)

                prompt = f"""
You are a helpful document analysis assistant. Answer questions using ONLY the information from the provided context.

CONTEXT ANALYSIS RULES:
1. For specific questions → provide detailed, exact answers from context.
2. For vague questions → provide a synthesized overview using multiple documents.
3. If no info exists → say: "I don't have specific information about this in my knowledge base."
4. Do NOT add outside knowledge.
5. Always cite the documents you used.

CONVERSATION HISTORY:
{conversation_history}

CONTEXT:
{context}

QUESTION: {question}
"""

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, self.llm.complete, prompt)

                answer = str(response).strip()
                self._update_conversation_context(user_id, question, answer)
                return answer

            except Exception as e:
                msg = str(e).lower()

                if "429" in msg or "rate limit" in msg:
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    raise RateLimitError(
                        "Groq API rate limit exceeded after retries",
                        "The AI service is currently very busy. Please wait and try again."
                    )

                if "timeout" in msg:
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                        continue
                    raise ExternalServiceError(
                        "Groq API timeout after retries",
                        "The AI service is taking too long to respond. Please try again."
                    )

                raise

        raise ExternalServiceError(
            "Maximum retries exceeded for Groq API",
            "The AI service is currently unavailable. Please try again later."
        )

    # ======================================================
    # CONTEXT BUILDER (DOCUMENT-GROUPED)
    # ======================================================
    def _build_cross_document_context(self, chunks: List[Dict[str, Any]]) -> tuple:
        if not chunks:
            return "", []

        documents = {}
        for chunk in chunks:
            src = chunk.get("metadata", {}).get("source", "Unknown document")
            doc_id = chunk.get("metadata", {}).get("document_id", "unknown")

            if doc_id not in documents:
                documents[doc_id] = {
                    "name": src,
                    "chunks": [],
                    "chunk_count": 0
                }

            documents[doc_id]["chunks"].append(chunk)
            documents[doc_id]["chunk_count"] += 1

        context_str = "KNOWLEDGE BASE CONTEXT:\n\n"
        sources = set()

        for doc_id, info in documents.items():
            name = info["name"]
            sources.add(name)

            context_str += f"📄 DOCUMENT: {name} ({info['chunk_count']} relevant sections)\n"
            context_str += "─" * 50 + "\n"

            sorted_chunks = sorted(
                info["chunks"],
                key=lambda x: x.get("metadata", {}).get("chunk_index", 0)
            )

            for i, chunk in enumerate(sorted_chunks):
                context_str += f"\nSection {i+1}:\n{chunk['chunk_text']}\n\n"

            context_str += "\n" + "=" * 60 + "\n\n"

        return context_str, list(sources)

    # ======================================================
    # CONVERSATION MEMORY
    # ======================================================
    def _update_conversation_context(self, user_id: str, question: str, answer: str):
        if user_id not in self.conversation_context:
            self.conversation_context[user_id] = []

        self.conversation_context[user_id].append({
            "question": question,
            "answer": answer[:500]
        })

        if len(self.conversation_context[user_id]) > 5:
            self.conversation_context[user_id] = self.conversation_context[user_id][-5:]

    def _get_conversation_history(self, user_id: str) -> str:
        if user_id not in self.conversation_context:
            return ""

        history = ""
        for exch in self.conversation_context[user_id]:
            history += f"Q: {exch['question']}\nA: {exch['answer']}...\n\n"

        return history

    def clear_conversation_context(self, user_id: str):
        if user_id in self.conversation_context:
            self.conversation_context[user_id] = []


# Global instance
query_service = QueryService()
