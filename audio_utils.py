# audio_utils.py
# Voice I/O helpers using OpenAI's audio endpoints.

from __future__ import annotations

from io import BytesIO
from typing import Optional

from openai import OpenAI

client = OpenAI()


def speech_to_text(uploaded_audio) -> Optional[str]:
    """
    Take a Streamlit audio_input (UploadedFile) and return transcribed text.
    If anything fails we just return None and the UI falls back gracefully.
    """
    try:
        if uploaded_audio is None:
            return None

        # Streamlit UploadedFile -> bytes
        raw_bytes = uploaded_audio.getvalue()
        if not raw_bytes:
            return None

        audio_file = BytesIO(raw_bytes)
        audio_file.name = "voice_input.wav"

        resp = client.audio.transcriptions.create(
            model="whisper-1",   #
            file=audio_file,
        )
        text = (resp.text or "").strip()
        return text or None

    except Exception as e:
        print("speech_to_text failed:", repr(e))
        return None


def text_to_speech(text: str) -> Optional[bytes]:
    """
    Convert text to mp3 bytes that Streamlit can play.
    Returns None if TTS fails for any reason.
    """
    text = (text or "").strip()
    if not text:
        return None

    try:
        resp = client.audio.speech.create(
            model="gpt-4o-mini-tts", 
            voice="alloy",
            input=text,
        )
        audio_bytes = resp.read()
        return audio_bytes
    except Exception as e:
        print("text_to_speech failed:", repr(e))
        return None
