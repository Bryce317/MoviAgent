import base64
from typing import List, Dict, Any

import streamlit as st

import db
from movi_agent import get_movi_graph
import audio_utils


st.set_page_config(
    page_title="Movi – MoveInSync Shuttle",
    layout="wide",
)


def _init_once():
    # Create DB/tables and seed dummy data
    db.init_db()


_init_once()
graph = get_movi_graph()


# ------------- Helpers -------------
def _render_bus_dashboard():
    st.subheader("busDashboard")

    rows = db.fetch_bus_dashboard_data()
    if not rows:
        st.info("No trips found in dummy data.")
        return

    left, right = st.columns([2, 3])
    with left:
        st.markdown("**Trips (left panel)**")
        trip_view = [
            {
                "Trip": r["display_name"],
                "Route": r["route_display_name"],
                "Status": r["live_status"],
                "Bookings %": r["booking_status_percentage"],
            }
            for r in rows
        ]
        st.dataframe(trip_view, use_container_width=True)

    with right:
        st.markdown("**Deployments (vehicle + driver)**")
        dep_view = [
            {
                "Trip": r["display_name"],
                "Vehicle": r["license_plate"] or "—",
                "Driver": r["driver_name"] or "—",
            }
            for r in rows
        ]
        st.dataframe(dep_view, use_container_width=True)


def _render_manage_route():
    st.subheader("manageRoute")

    routes = db.fetch_routes_data()
    if not routes:
        st.info("No routes found in dummy data.")
        return

    st.markdown("**Routes (derived from Paths + Stops)**")
    route_view = [
        {
            "Route": r["route_display_name"],
            "Path": r["path_name"],
            "Shift": r["shift_time"],
            "Direction": r["direction"],
            "From": r["start_point"],
            "To": r["end_point"],
            "Status": r["status"],
        }
        for r in routes
    ]
    st.dataframe(route_view, use_container_width=True)


def _history_to_graph_messages(
    history: List[Dict[str, Any]],
    last_image_b64: str | None = None,
) -> list:
    """
    Convert simple dict history into LangGraph-compatible chat messages.

    For the last user message we optionally attach the uploaded screenshot
    as a multimodal image_url content.
    """
    messages = []
    for i, msg in enumerate(history):
        role = msg["role"]
        content = msg["content"]
        is_last = i == len(history) - 1

        if role == "user":
            if is_last and last_image_b64:
                # multimodal: text + image
                messages.append(
                    {
                        "type": "human",
                        "content": [
                            {"type": "text", "text": content},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{last_image_b64}"
                                },
                            },
                        ],
                    }
                )
            else:
                messages.append({"type": "human", "content": content})
        else:
            messages.append({"type": "ai", "content": content})
    return messages


# ------------- Main UI -------------


def main():
    st.title("Movi – Multimodal Transport Agent")

    page = st.sidebar.radio("Admin page", ["busDashboard", "manageRoute"])
    st.sidebar.markdown("---")
    speak_out = st.sidebar.checkbox("Speak Movi's responses", value=False)

    st.sidebar.markdown("#### Voice input (optional)")
    audio_input = st.sidebar.audio_input("Record a query", key="voice_query")

    st.sidebar.markdown("#### Screenshot input (optional)")
    uploaded_img = st.sidebar.file_uploader(
        "Upload busDashboard screenshot", type=["png", "jpg", "jpeg"]
    )

    # Simple chat history kept entirely on the Streamlit side
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # Layout: left = page data, right = Movi chat
    col_main, col_chat = st.columns([3, 2])

    with col_main:
        if page == "busDashboard":
            _render_bus_dashboard()
        else:
            _render_manage_route()

    with col_chat:
        st.subheader("Movi – AI assistant")

        # Show history
        for msg in st.session_state["chat_history"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        voice_text = None
        if audio_input is not None and st.sidebar.button("Use voice input"):
            with st.spinner("Transcribing voice..."):
                voice_text = audio_utils.speech_to_text(audio_input)
                if voice_text:
                    st.sidebar.success(f"Recognized: {voice_text}")
                else:
                    st.sidebar.error("Couldn't understand audio, please try again.")

        user_input = st.chat_input("Ask Movi something")
        if not user_input and voice_text:
            user_input = voice_text

        if user_input:
            # Add user message to history
            st.session_state["chat_history"].append(
                {"role": "user", "content": user_input}
            )
            with st.chat_message("user"):
                st.markdown(user_input)

            # Prepare multimodal messages for the agent
            image_b64 = None
            if uploaded_img is not None:
                # We only feed the image with the *current* message.
                img_bytes = uploaded_img.read()
                image_b64 = base64.b64encode(img_bytes).decode("utf-8")

            graph_messages = _history_to_graph_messages(
                st.session_state["chat_history"], last_image_b64=image_b64
            )

            # Call Movi (LangGraph)
            with st.spinner("Movi is thinking..."):
                result_state = graph.invoke(
                    {
                        "messages": graph_messages,
                        "current_page": page,
                    }
                )

            final_messages = result_state["messages"]
            last_msg = final_messages[-1]
            # last_msg is a proper LangChain message object
            reply_text = (
                last_msg.content
                if isinstance(last_msg.content, str)
                else str(last_msg.content)
            )

            st.session_state["chat_history"].append(
                {"role": "assistant", "content": reply_text}
            )

            with st.chat_message("assistant"):
                st.markdown(reply_text)

                if speak_out:
                    audio_bytes = audio_utils.text_to_speech(reply_text)
                    if audio_bytes:
                        st.audio(audio_bytes, format="audio/mp3")


if __name__ == "__main__":
    main()
