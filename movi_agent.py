from typing import Annotated, List, Dict, Any, Literal
from typing_extensions import TypedDict
import sqlite3

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage
from langchain_core.tools import StructuredTool

import db


# ---- Graph state ----
class MoviState(TypedDict):
    """Graph state for Movi agent."""
    messages: Annotated[list, add_messages]
    current_page: str


# ---- Helper: schema extraction ----
def _get_db_schema() -> str:
    """
    Introspect the SQLite database and return a readable schema definition.
    This allows the LLM to know actual tables and columns when writing SQL.
    """
    try:
        conn = sqlite3.connect(db.DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        )
        schema_lines = []
        for name, sql in cur.fetchall():
            schema_lines.append(f"-- Table: {name}\n{sql.strip()}")
        return "\n\n".join(schema_lines) if schema_lines else "(no tables found)"
    except Exception as e:
        return f"(error loading schema: {e})"
    finally:
        conn.close()


# ---- Tool wrappers ----
def tool_count_unassigned_vehicles() -> str:
    """Return how many vehicles are not assigned to any trip."""
    return db.count_unassigned_vehicles()


def tool_get_trip_status(trip_display_name: str) -> str:
    """Get full status for a given trip display name."""
    return db.get_trip_status(trip_display_name)


def tool_list_stops_for_path(path_name: str) -> str:
    """List all stops for a given path, in order."""
    return db.list_stops_for_path(path_name)


def tool_list_routes_for_path(path_name: str) -> str:
    """List all routes that use a given path."""
    return db.list_routes_for_path(path_name)


def tool_assign_vehicle_and_driver(trip_display_name: str, vehicle_plate: str, driver_name: str) -> str:
    """Assign or update vehicle + driver for a given trip."""
    return db.assign_vehicle_and_driver(trip_display_name, vehicle_plate, driver_name)


def tool_remove_vehicle_from_trip(trip_display_name: str, force: bool = False) -> str:
    """Remove vehicle + driver from a trip."""
    return db.remove_vehicle_from_trip(trip_display_name, force=force)


def tool_create_stop(name: str, latitude: float | None = None, longitude: float | None = None) -> str:
    """Create a new stop if it does not exist."""
    return db.create_stop(name, latitude, longitude)


def tool_create_path(path_name: str, stop_names: List[str]) -> str:
    """Create a new path with an ordered list of stops."""
    return db.create_path(path_name, stop_names)


def tool_create_route(path_name: str, shift_time: str, direction: str) -> str:
    """Create a new route for an existing path."""
    return db.create_route(path_name, shift_time, direction)


def tool_list_active_routes() -> str:
    """List all active routes."""
    return db.list_active_routes()


def tool_list_unassigned_drivers() -> str:
    """List all drivers that are not assigned to any deployment."""
    return db.list_unassigned_drivers()


def tool_run_dynamic_quries(query: str, mode: Literal["read", "write"] = "read") -> str:
    """
    Run dynamic SQL queries safely using db.dynamic_run_sql_query.
    Used as a fallback when no structured tool matches the user's intent.
    """
    return db.dynamic_run_sql_query(query, mode)


# ---- Build tool list ----
TOOLS = [
    StructuredTool.from_function(tool_count_unassigned_vehicles),
    StructuredTool.from_function(tool_get_trip_status),
    StructuredTool.from_function(tool_list_stops_for_path),
    StructuredTool.from_function(tool_list_routes_for_path),
    StructuredTool.from_function(tool_assign_vehicle_and_driver),
    StructuredTool.from_function(tool_remove_vehicle_from_trip),
    StructuredTool.from_function(tool_create_stop),
    StructuredTool.from_function(tool_create_path),
    StructuredTool.from_function(tool_create_route),
    StructuredTool.from_function(tool_list_active_routes),
    StructuredTool.from_function(tool_list_unassigned_drivers),
    StructuredTool.from_function(tool_run_dynamic_quries),
]


# ---- LLM setup ----
LLM = ChatOpenAI(model="gpt-4o-mini", temperature=0.1)


# ---- System prompt builder ----
def _build_system_prompt(current_page: str) -> str:
    """
    Builds Movi's system prompt, injecting the live DB schema.
    """
    schema_text = _get_db_schema()

    return f"""
You are Movi, the transport manager assistant for MoveInSync Shuttle.

You manage a SQLite database with this schema:
{schema_text}

You know the relationships:
- Stops → Paths (ordered stops) → Routes (Path + time + direction + status)
- Routes → DailyTrips (per-day trip instances)
- DailyTrips → Deployments (vehicle + driver assigned)

Current UI Page: {current_page}

GENERAL BEHAVIOUR
- Speak like a helpful backend engineer.
- Use structured tools first for known operations.
- If no structured tool fits the user request, generate a valid SQL query based on the schema
  and execute it using the tool_run_dynamic_quries tool.
- Always use exact table and column names from the schema.
- Prefer SELECT queries unless user explicitly requests data changes.
- Never use unsafe commands (DROP, ALTER, PRAGMA, ATTACH, DETACH).

PAGE CONTEXT
- If current_page == "manageRoute": focus on paths, stops, and routes.
- If current_page == "busDashboard": focus on trips, drivers, and vehicles.

TRIBAL KNOWLEDGE
- Removing a vehicle from a trip may break bookings; always confirm before force removal.

Be concise, factual, and data-grounded.
"""


# ---- Graph nodes ----
def _agent_node(state: MoviState) -> Dict[str, Any]:
    """Main LLM node that decides which tool(s) to call."""
    sys_msg = SystemMessage(content=_build_system_prompt(state.get("current_page", "unknown")))
    llm_with_tools = LLM.bind_tools(TOOLS)
    result = llm_with_tools.invoke([sys_msg] + state["messages"])
    return {"messages": [result]}


# ---- Build LangGraph ----
def build_movi_graph():
    """Construct and compile Movi's LangGraph."""
    builder = StateGraph(MoviState)
    builder.add_node("agent", _agent_node)
    builder.add_node("tools", ToolNode(TOOLS))

    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", tools_condition)
    builder.add_edge("tools", "agent")
    builder.add_edge("agent", END)
    return builder.compile()


# ---- Lazy global instance ----
_MOVI_GRAPH = None

def get_movi_graph():
    """Return or initialize Movi's compiled LangGraph."""
    global _MOVI_GRAPH
    if _MOVI_GRAPH is None:
        _MOVI_GRAPH = build_movi_graph()
    return _MOVI_GRAPH
