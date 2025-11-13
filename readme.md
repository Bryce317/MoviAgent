# Movi – Multimodal Transport Agent (LangGraph + Streamlit)

This is a small but end-to-end prototype of **Movi**, an AI assistant for MoveInSync Shuttle.

It models the **Stop → Path → Route → Trip → Deployment** flow and exposes it through:

- A **dummy SQLite database** with realistic seed data.
- A **two-page admin console**:
  - `busDashboard` – daily trips + deployments
  - `manageRoute` – static Stops / Paths / Routes
- A **LangGraph-based agent** that:
  - Understands text + voice input
  - Can read and update the transport data
  - Implements a **“Tribal knowledge” consequence flow** when removing vehicles
  - Can look at a **busDashboard screenshot** and act on the highlighted trip

---

## Stack

- **Python 3.11+**
- **Streamlit** for the admin UI
- **SQLite** for the data layer (`movi.db`)
- **LangGraph** + **LangChain OpenAI** for the agent
- **OpenAI**:
  - `gpt-4o-mini` (chat + tools + vision)
  - `whisper-1` (speech-to-text)
  - `gpt-4o-mini-tts` (text-to-speech)

---
