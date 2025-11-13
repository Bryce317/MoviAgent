from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
from typing import Literal

DB_PATH = Path(__file__).parent / "movi.db"


# === Connection helpers =======================================================

def get_connection() -> sqlite3.Connection:
    """Return a sqlite connection with Row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# === Bootstrap ============================================

def init_db() -> None:
    """
    Create tables if missing.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    # ----- Layer 1: Static assets -----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stops (
            stop_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT UNIQUE NOT NULL,
            latitude    REAL,
            longitude   REAL
        );
        """
    )

    # Use path_name (explicit as per spec)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS paths (
            path_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            path_name   TEXT UNIQUE NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS path_stops (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            path_id     INTEGER NOT NULL,
            stop_id     INTEGER NOT NULL,
            seq         INTEGER NOT NULL,
            FOREIGN KEY (path_id) REFERENCES paths(path_id) ON DELETE CASCADE,
            FOREIGN KEY (stop_id) REFERENCES stops(stop_id) ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS routes (
            route_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            path_id             INTEGER NOT NULL,
            route_display_name  TEXT UNIQUE NOT NULL,
            shift_time          TEXT NOT NULL,         -- "HH:MM"
            direction           TEXT NOT NULL,         -- 'IN' | 'OUT'
            start_point         TEXT NOT NULL,
            end_point           TEXT NOT NULL,
            status              TEXT NOT NULL CHECK (status IN ('active','deactivated')),
            FOREIGN KEY (path_id) REFERENCES paths(path_id) ON DELETE CASCADE
        );
        """
    )

    # ----- Layer 2: Dynamic operations -----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            license_plate   TEXT UNIQUE NOT NULL,
            type            TEXT NOT NULL,            -- 'Bus' | 'Cab'
            capacity        INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT UNIQUE NOT NULL,
            phone_number    TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_trips (
            trip_id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            route_id                    INTEGER NOT NULL,
            display_name                TEXT UNIQUE NOT NULL,
            booking_status_percentage   REAL DEFAULT 0,
            live_status                 TEXT,          -- e.g. '00:01 IN'
            FOREIGN KEY (route_id) REFERENCES routes(route_id) ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deployments (
            deployment_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            trip_id         INTEGER UNIQUE NOT NULL,  -- one deployment per trip
            vehicle_id      INTEGER,
            driver_id       INTEGER,
            FOREIGN KEY (trip_id) REFERENCES daily_trips(trip_id) ON DELETE CASCADE,
            FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id) ON DELETE SET NULL,
            FOREIGN KEY (driver_id) REFERENCES drivers(driver_id) ON DELETE SET NULL
        );
        """
    )

    conn.commit()
    #_migrate_schema(conn)     # rename paths.name -> paths.path_name if needed
    _seed_if_empty(conn)      # populate once
    conn.close()


# === Seeding ==================================================================

def _seed_if_empty(conn: sqlite3.Connection) -> None:
    """Seed realistic demo data to match screenshots / examples."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM stops;")
    if cur.fetchone()["c"] > 0:
        return  # already seeded

    # ----- Stops -----
    stops = [
        ("Gavipuram", 12.942, 77.566),
        ("Temple", 12.945, 77.568),
        ("Peenya", 13.020, 77.515),
        ("Majestic", 12.978, 77.572),
        ("Tech Park", 12.997, 77.700),
    ]
    cur.executemany(
        "INSERT INTO stops (name, latitude, longitude) VALUES (?, ?, ?);", stops
    )

    # ----- Paths -----
    for p in ("Path-1", "Path-2"):
        cur.execute("INSERT INTO paths (path_name) VALUES (?);", (p,))

    # id maps
    cur.execute("SELECT stop_id, name FROM stops;")
    stop_id_by_name = {r["name"]: r["stop_id"] for r in cur.fetchall()}

    cur.execute("SELECT path_id, path_name FROM paths;")
    path_id_by_name = {r["path_name"]: r["path_id"] for r in cur.fetchall()}

    # Path-1: Gavipuram → Temple → Peenya
    path1_id = path_id_by_name["Path-1"]
    for seq, name in enumerate(["Gavipuram", "Temple", "Peenya"], start=1):
        cur.execute(
            "INSERT INTO path_stops (path_id, stop_id, seq) VALUES (?, ?, ?);",
            (path1_id, stop_id_by_name[name], seq),
        )

    # Path-2: Peenya → Majestic → Tech Park
    path2_id = path_id_by_name["Path-2"]
    for seq, name in enumerate(["Peenya", "Majestic", "Tech Park"], start=1):
        cur.execute(
            "INSERT INTO path_stops (path_id, stop_id, seq) VALUES (?, ?, ?);",
            (path2_id, stop_id_by_name[name], seq),
        )

    # ----- Routes (derived start/end from path stops) -----
    def _start_end_for_path(pid: int) -> tuple[str, str]:
        cur.execute(
            """
            SELECT s.name
            FROM path_stops ps
            JOIN stops s ON s.stop_id = ps.stop_id
            WHERE ps.path_id = ?
            ORDER BY ps.seq ASC;
            """,
            (pid,),
        )
        names = [r["name"] for r in cur.fetchall()]
        return names[0], names[-1]

    routes_to_insert = []
    for path_name, shift, direction, status in [
        ("Path-1", "08:30", "IN", "active"),
        ("Path-1", "19:45", "OUT", "active"),
        ("Path-2", "19:45", "IN", "active"),
    ]:
        pid = path_id_by_name[path_name]
        start, end = _start_end_for_path(pid)
        display_name = f"{path_name} - {shift}"
        routes_to_insert.append((pid, display_name, shift, direction, start, end, status))

    cur.executemany(
        """
        INSERT INTO routes (
            path_id, route_display_name, shift_time, direction,
            start_point, end_point, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        routes_to_insert,
    )

    # ----- Vehicles -----
    vehicles = [
        ("KA-01-1111", "Bus", 40),
        ("MH-12-3456", "Bus", 40),
        ("KA-05-9999", "Cab", 4),
    ]
    cur.executemany(
        "INSERT INTO vehicles (license_plate, type, capacity) VALUES (?, ?, ?);",
        vehicles,
    )

    # ----- Drivers -----
    drivers = [
        ("Amit", "9999990001"),
        ("Rahul", "9999990002"),
        ("Sneha", "9999990003"),
    ]
    cur.executemany(
        "INSERT INTO drivers (name, phone_number) VALUES (?, ?);", drivers
    )

    # Route map
    cur.execute("SELECT route_id, route_display_name FROM routes;")
    route_by_display = {r["route_display_name"]: r["route_id"] for r in cur.fetchall()}

    # ----- Daily trips (left panel items like 'Bulk - 00:01') -----
    trips = [
        # display_name            route_display_name  booking% live_status
        ("Bulk - 00:01",          "Path-1 - 08:30",  25.0,    "00:01 IN"),
        ("Bulk - 00:02",          "Path-1 - 08:30",   0.0,    "00:02 IN"),
        ("Path Path - 00:02",     "Path-2 - 19:45",  10.0,    "00:02 OUT"),
    ]
    trip_rows = []
    for disp, route_disp, pct, live in trips:
        trip_rows.append((route_by_display[route_disp], disp, pct, live))

    cur.executemany(
        """
        INSERT INTO daily_trips (route_id, display_name, booking_status_percentage, live_status)
        VALUES (?, ?, ?, ?);
        """,
        trip_rows,
    )

    # Map for deployments
    cur.execute("SELECT trip_id, display_name FROM daily_trips;")
    trip_id_by_name = {r["display_name"]: r["trip_id"] for r in cur.fetchall()}

    cur.execute("SELECT vehicle_id, license_plate FROM vehicles;")
    vehicle_id_by_plate = {r["license_plate"]: r["vehicle_id"] for r in cur.fetchall()}

    cur.execute("SELECT driver_id, name FROM drivers;")
    driver_id_by_name = {r["name"]: r["driver_id"] for r in cur.fetchall()}

    # ----- Deployments (vehicle+driver assigned to a trip) -----
    deployments = [
        ("Bulk - 00:01",      "KA-01-1111", "Amit"),
        ("Path Path - 00:02", "MH-12-3456", "Rahul"),
        # 'Bulk - 00:02' intentionally left unassigned
    ]
    dep_values = []
    for trip_name, plate, driver_name in deployments:
        dep_values.append(
            (trip_id_by_name[trip_name], vehicle_id_by_plate[plate], driver_id_by_name[driver_name])
        )

    cur.executemany(
        "INSERT INTO deployments (trip_id, vehicle_id, driver_id) VALUES (?, ?, ?);",
        dep_values,
    )

    conn.commit()


# === Simple UI queries (used by Streamlit pages) ==============================

def fetch_bus_dashboard_data() -> List[Dict[str, Any]]:
    """Return rows for busDashboard (trips + current deployment info)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            t.display_name,
            t.booking_status_percentage,
            t.live_status,
            r.route_display_name,
            v.license_plate,
            d.name AS driver_name
        FROM daily_trips t
        JOIN routes r ON r.route_id = t.route_id
        LEFT JOIN deployments dep ON dep.trip_id = t.trip_id
        LEFT JOIN vehicles v ON v.vehicle_id = dep.vehicle_id
        LEFT JOIN drivers d ON d.driver_id = dep.driver_id
        ORDER BY t.display_name;
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_routes_data() -> List[Dict[str, Any]]:
    """Return rows for manageRoute (routes derived from paths + ordered stops)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            r.route_display_name,
            p.path_name AS path_name,
            r.shift_time,
            r.direction,
            r.start_point,
            r.end_point,
            r.status
        FROM routes r
        JOIN paths p ON p.path_id = r.path_id
        ORDER BY p.path_name, r.shift_time;
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# === Tool-facing functions (used by Movi's actions) ===========================

def count_unassigned_vehicles() -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM vehicles v
        WHERE NOT EXISTS (SELECT 1 FROM deployments d WHERE d.vehicle_id = v.vehicle_id);
        """
    )
    c = cur.fetchone()["c"]
    conn.close()
    return f"There are {c} vehicles not assigned to any trip right now."


def list_unassigned_drivers() -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM drivers d
        WHERE NOT EXISTS (SELECT 1 FROM deployments dep WHERE dep.driver_id = d.driver_id);
        """
    )
    total = cur.fetchone()["c"]
    cur.execute(
        """
        SELECT d.name
        FROM drivers d
        WHERE NOT EXISTS (SELECT 1 FROM deployments dep WHERE dep.driver_id = d.driver_id)
        ORDER BY d.name;
        """
    )
    names = [r["name"] for r in cur.fetchall()]
    conn.close()
    return "All drivers are currently assigned." if not names else f"Unassigned drivers ({total}): " + ", ".join(names)


def get_trip_status(trip_display_name: str) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            t.display_name,
            t.booking_status_percentage,
            t.live_status,
            r.route_display_name,
            v.license_plate,
            d.name AS driver_name
        FROM daily_trips t
        JOIN routes r ON r.route_id = t.route_id
        LEFT JOIN deployments dep ON dep.trip_id = t.trip_id
        LEFT JOIN vehicles v ON v.vehicle_id = dep.vehicle_id
        LEFT JOIN drivers d ON d.driver_id = dep.driver_id
        WHERE t.display_name = ?;
        """,
        (trip_display_name,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return f"Trip '{trip_display_name}' not found."

    assigned = (
        f"Vehicle {row['license_plate']} with driver {row['driver_name']}"
        if row["license_plate"] else "No vehicle/driver assigned"
    )
    return (
        f"Trip '{row['display_name']}' is on route '{row['route_display_name']}', "
        f"booking status ~{row['booking_status_percentage']}%, "
        f"live status '{row['live_status']}'. {assigned}."
    )


def list_stops_for_path(path_name: str) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT path_id FROM paths WHERE path_name = ?;", (path_name,))
    p = cur.fetchone()
    if not p:
        conn.close()
        return f"Path '{path_name}' not found."

    cur.execute(
        """
        SELECT s.name
        FROM path_stops ps
        JOIN stops s ON s.stop_id = ps.stop_id
        WHERE ps.path_id = ?
        ORDER BY ps.seq ASC;
        """,
        (p["path_id"],),
    )
    stops = [r["name"] for r in cur.fetchall()]
    conn.close()
    return f"Path '{path_name}' has no stops configured." if not stops else f"Stops on {path_name}: " + " → ".join(stops)


def list_routes_for_path(path_name: str) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT path_id FROM paths WHERE path_name = ?;", (path_name,))
    p = cur.fetchone()
    if not p:
        conn.close()
        return f"Path '{path_name}' not found."

    cur.execute(
        """
        SELECT route_display_name, shift_time, direction, status
        FROM routes
        WHERE path_id = ?
        ORDER BY shift_time;
        """,
        (p["path_id"],),
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return f"No routes use path '{path_name}'."
    lines = [f"- {r['route_display_name']} ({r['direction']} @ {r['shift_time']}, {r['status']})" for r in rows]
    return f"Routes using path '{path_name}':\n" + "\n".join(lines)


def list_active_routes() -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT route_display_name, shift_time, direction
        FROM routes
        WHERE status = 'active'
        ORDER BY shift_time;
        """
    )
    rows = cur.fetchall()
    conn.close()
    return "There are no active routes." if not rows else "Active routes:\n" + "\n".join(
        f"- {r['route_display_name']} ({r['direction']} @ {r['shift_time']})" for r in rows
    )


def create_stop(name: str, latitude: Optional[float] = None, longitude: Optional[float] = None) -> str:
    """Create a new stop if not exists."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO stops (name, latitude, longitude) VALUES (?, ?, ?);",
            (name, latitude, longitude),
        )
        conn.commit()
        return f"Created new stop '{name}'."
    except sqlite3.IntegrityError:
        return f"Stop '{name}' already exists."
    finally:
        conn.close()


def create_path(path_name: str, stop_names: List[str]) -> str:
    """
    Create a new path with an ordered list of stops.
    Missing stops are created on the fly with null lat/long (demo-friendly).
    """
    if not stop_names:
        return "Need at least one stop to create a path."
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO paths (path_name) VALUES (?);", (path_name,))
        path_id = cur.lastrowid

        for seq, s_name in enumerate(stop_names, start=1):
            cur.execute("SELECT stop_id FROM stops WHERE name = ?;", (s_name,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO stops (name, latitude, longitude) VALUES (?, NULL, NULL);",
                    (s_name,),
                )
                stop_id = cur.lastrowid
            else:
                stop_id = row["stop_id"]
            cur.execute(
                "INSERT INTO path_stops (path_id, stop_id, seq) VALUES (?, ?, ?);",
                (path_id, stop_id, seq),
            )

        conn.commit()
        return f"Created path '{path_name}' with stops: " + " → ".join(stop_names)
    except sqlite3.IntegrityError:
        return f"Path '{path_name}' already exists."
    finally:
        conn.close()


def create_route(path_name: str, shift_time: str, direction: str) -> str:
    """Create a new route for an existing path (derives start/end from ordered stops)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT path_id FROM paths WHERE path_name = ?;", (path_name,))
    p = cur.fetchone()
    if not p:
        conn.close()
        return f"Path '{path_name}' not found, cannot create route."

    path_id = p["path_id"]

    # derive start/end from first/last stop in path
    cur.execute(
        """
        SELECT s.name
        FROM path_stops ps
        JOIN stops s ON s.stop_id = ps.stop_id
        WHERE ps.path_id = ?
        ORDER BY ps.seq ASC;
        """,
        (path_id,),
    )
    names = [r["name"] for r in cur.fetchall()]
    if not names:
        conn.close()
        return f"Path '{path_name}' has no stops configured, cannot create route."

    start_point, end_point = names[0], names[-1]
    route_display_name = f"{path_name} - {shift_time}"

    try:
        cur.execute(
            """
            INSERT INTO routes (
                path_id, route_display_name, shift_time, direction,
                start_point, end_point, status
            )
            VALUES (?, ?, ?, ?, ?, ?, 'active');
            """,
            (path_id, route_display_name, shift_time, direction, start_point, end_point),
        )
        conn.commit()
        return f"Created route '{route_display_name}' ({direction}) from {start_point} to {end_point}."
    except sqlite3.IntegrityError:
        return f"Route '{route_display_name}' already exists."
    finally:
        conn.close()


def assign_vehicle_and_driver(trip_display_name: str, vehicle_plate: str, driver_name: str) -> str:
    """Create/update a deployment: (vehicle, driver) → trip."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT trip_id FROM daily_trips WHERE display_name = ?;", (trip_display_name,))
    trip = cur.fetchone()
    if not trip:
        conn.close()
        return f"Trip '{trip_display_name}' not found."

    cur.execute("SELECT vehicle_id FROM vehicles WHERE license_plate = ?;", (vehicle_plate,))
    vehicle = cur.fetchone()
    if not vehicle:
        conn.close()
        return f"Vehicle '{vehicle_plate}' not found."

    cur.execute("SELECT driver_id FROM drivers WHERE name = ?;", (driver_name,))
    driver = cur.fetchone()
    if not driver:
        conn.close()
        return f"Driver '{driver_name}' not found."

    cur.execute("SELECT deployment_id FROM deployments WHERE trip_id = ?;", (trip["trip_id"],))
    existing = cur.fetchone()
    if existing:
        cur.execute(
            "UPDATE deployments SET vehicle_id = ?, driver_id = ? WHERE deployment_id = ?;",
            (vehicle["vehicle_id"], driver["driver_id"], existing["deployment_id"]),
        )
        msg = f"Updated deployment: trip '{trip_display_name}' now uses vehicle {vehicle_plate} with driver {driver_name}."
    else:
        cur.execute(
            "INSERT INTO deployments (trip_id, vehicle_id, driver_id) VALUES (?, ?, ?);",
            (trip["trip_id"], vehicle["vehicle_id"], driver["driver_id"]),
        )
        msg = f"Assigned vehicle {vehicle_plate} and driver {driver_name} to trip '{trip_display_name}'."

    conn.commit()
    conn.close()
    return msg


def remove_vehicle_from_trip(trip_display_name: str, force: bool = False) -> str:
    """
    Dangerous op with consequence check:
    - If booking% > 0 and force == False -> return WARNING, do NOT change DB.
    - If force == True (after user confirms) -> remove deployment.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT t.trip_id, t.display_name, t.booking_status_percentage, r.route_display_name
        FROM daily_trips t
        JOIN routes r ON r.route_id = t.route_id
        WHERE t.display_name = ?;
        """,
        (trip_display_name,),
    )
    trip = cur.fetchone()
    if not trip:
        conn.close()
        return f"Trip '{trip_display_name}' not found."

    cur.execute(
        """
        SELECT dep.deployment_id, v.license_plate, d.name AS driver_name
        FROM deployments dep
        LEFT JOIN vehicles v ON v.vehicle_id = dep.vehicle_id
        LEFT JOIN drivers d ON d.driver_id = dep.driver_id
        WHERE dep.trip_id = ?;
        """,
        (trip["trip_id"],),
    )
    dep = cur.fetchone()
    if not dep:
        conn.close()
        return f"No vehicle is currently assigned to trip '{trip_display_name}'."

    booking = trip["booking_status_percentage"] or 0.0
    if booking > 0 and not force:
        conn.close()
        return (
            "WARNING: "
            f"Trip '{trip['display_name']}' on route '{trip['route_display_name']}' is already ~{booking}% booked. "
            "Removing the vehicle will cancel these bookings and the trip-sheet will fail to generate. "
            "Ask the user for a yes/no confirmation. If they confirm, call this again with force=true."
        )

    # Actually remove
    cur.execute("DELETE FROM deployments WHERE deployment_id = ?;", (dep["deployment_id"],))
    conn.commit()
    conn.close()
    return f"Removed vehicle '{dep['license_plate']}' and driver '{dep['driver_name']}' from trip '{trip_display_name}'."



def dynamic_run_sql_query(query: str, mode: Literal["read", "write"] = "read") -> str:
    """
    Run a direct SQL query against the movi.db database.

    Args:
        query (str): SQL statement to execute (SELECT / INSERT / UPDATE / DELETE).
        mode (Literal["read", "write"]): Controls whether modification statements are allowed.

    Returns:
        str: Result table or status message.

    Safety:
        - 'read' mode only allows SELECT queries.
        - 'write' mode allows changes but commits explicitly.
        - Any dangerous statements (DROP, ALTER, PRAGMA) are blocked.
    """
    q_lower = query.strip().lower()
    banned = ("drop ", "alter ", "pragma", "attach", "detach")
    if any(b in q_lower for b in banned):
        return "Unsafe SQL command blocked."

    if mode == "read" and not q_lower.startswith("select"):
        return "Only SELECT queries allowed in read mode."

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query)

        if q_lower.startswith("select"):
            rows = cur.fetchall()
            if not rows:
                return "Query executed. No rows returned."
            headers = rows[0].keys()
            lines = [" | ".join(headers)]
            lines.append("-" * len(lines[0]))
            for r in rows:
                lines.append(" | ".join(str(x) for x in r))
            return "\n".join(lines)
        else:
            conn.commit()
            return f"Query executed successfully ({cur.rowcount} rows affected)."
    except Exception as e:
        return f"SQL error: {e}"
    finally:
        conn.close()


