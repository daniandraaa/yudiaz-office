"""Comprehensive test suite for Yudiaz Virtual HQ backend.

Verifies:
- Config loading and environment bindings
- Health check endpoints (/health and /api/v1/health)
- Complete spatial office state (/api/v1/office/state)
- Rooms and agent catalog endpoints with 404 validation
- Agent actions and spatial repositioning
- Office-wide macro protocols (gather_war_room, resume_deep_work, trigger_sleep_cycle)
- Chronological audit logging (/api/v1/activities)
- Security PIN verification (/api/v1/auth/verify)
- Server-Sent Events (SSE) telemetry stream (/api/v1/stream)
- Static frontend index.html and asset delivery
"""

import json
from typing import AsyncGenerator
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from backend.config import Settings, get_settings
from backend.main import app
from backend.models import AgentStatus, OfficeMode
from backend.office_engine import OfficeEngine, office_engine


@pytest.fixture(autouse=True)
def reset_office_state():
    """Ensure clean baseline state before each test."""
    office_engine.resume_deep_work()
    yield
    office_engine.resume_deep_work()


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Async test client with lifespan context."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        yield client


# ============================================================================
# 1. Config Tests
# ============================================================================

def test_config_defaults_and_env():
    """Verify settings defaults and environment mapping."""
    settings = get_settings()
    assert settings.host == "127.0.0.1"
    assert settings.port == 9449
    assert settings.pin == "2609"
    assert "Yudiaz" in settings.title
    assert settings.version == "1.0.0"


# ============================================================================
# 2. Health Check Tests
# ============================================================================

@pytest.mark.asyncio
async def test_health_endpoints(async_client: AsyncClient):
    """Verify both /health and /api/v1/health respond with status ok."""
    # Top-level alias
    res_root = await async_client.get("/health")
    assert res_root.status_code == 200
    data_root = res_root.json()
    assert data_root["status"] == "ok"
    assert data_root["active_agents"] == 11
    assert data_root["total_rooms"] == 9

    # API v1 route
    res_v1 = await async_client.get("/api/v1/health")
    assert res_v1.status_code == 200
    data_v1 = res_v1.json()
    assert data_v1["status"] == "ok"
    assert data_v1["service"] == data_root["service"]


# ============================================================================
# 3. Full Office State Tests
# ============================================================================

@pytest.mark.asyncio
async def test_get_office_state(async_client: AsyncClient):
    """Verify full spatial state contains 11 agents, 9 rooms, and valid telemetry."""
    res = await async_client.get("/api/v1/office/state")
    assert res.status_code == 200
    data = res.json()

    assert data["office_mode"] == OfficeMode.NORMAL.value
    assert len(data["agents"]) == 11
    assert len(data["rooms"]) == 9
    assert "server_telemetry" in data
    assert "uptime_seconds" in data["server_telemetry"]
    assert "cpu_load_percent" in data["server_telemetry"]
    assert len(data["recent_activities"]) >= 1


# ============================================================================
# 4. Rooms Tests
# ============================================================================

@pytest.mark.asyncio
async def test_list_and_get_rooms(async_client: AsyncClient):
    """Verify listing all 9 rooms and querying individual rooms by id."""
    res = await async_client.get("/api/v1/rooms")
    assert res.status_code == 200
    rooms = res.json()
    assert len(rooms) == 9

    expected_rooms = {
        "room-ceo": ("CEO Suite", "Executive", 6, [520.0, 340.0]),
        "room-war": ("Conference Room", "Deliberation", 12, [1300.0, 290.0]),
        "room-dev": ("Workstations", "Engineering", 6, [470.0, 840.0]),
        "room-atelier": ("Research Library", "R&D", 8, [1300.0, 820.0]),
        "room-creative": ("Creative Studio", "Creative", 6, [2080.0, 340.0]),
        "room-intel": ("Radar NOC", "Operations", 6, [2130.0, 840.0]),
        "room-concierge": ("Lounge & Ping-Pong", "Hospitality", 8, [520.0, 1350.0]),
        "room-pods": ("Bedroom & Rest Pods", "Resting", 8, [1300.0, 1370.0]),
        "room-server": ("Server Room", "Infrastructure", 4, [2080.0, 1350.0]),
    }

    room_map = {r["id"]: r for r in rooms}
    assert set(room_map.keys()) == set(expected_rooms.keys())

    for rid, (exp_name, exp_cat, exp_cap, exp_center) in expected_rooms.items():
        room = room_map[rid]
        assert room["name"] == exp_name
        assert room["category"] == exp_cat
        assert room["capacity"] == exp_cap
        assert room["center_coord"] == exp_center
        assert len(room["dimensions"]) == 2
        assert "status_accent" in room
        assert "description" in room

    # Query valid room
    res_ceo = await async_client.get("/api/v1/rooms/room-ceo")
    assert res_ceo.status_code == 200
    ceo_data = res_ceo.json()
    assert ceo_data["name"] == "CEO Suite"
    assert ceo_data["category"] == "Executive"
    assert ceo_data["capacity"] == 6
    assert ceo_data["center_coord"] == [520.0, 340.0]

    # Query 404 room
    res_404 = await async_client.get("/api/v1/rooms/room-nonexistent")
    assert res_404.status_code == 404
    assert "not found" in res_404.json()["detail"].lower()


# ============================================================================
# 5. Agents Tests
# ============================================================================

@pytest.mark.asyncio
async def test_list_and_get_agents(async_client: AsyncClient):
    """Verify listing all 11 personnel and checking their profiles against requirements."""
    res = await async_client.get("/api/v1/agents")
    assert res.status_code == 200
    agents = res.json()
    assert len(agents) == 11

    agent_map = {a["id"]: a for a in agents}
    expected_agents = [
        ("dani", "Daniandra Prayudisty", "Founder & CEO", "Executive", "room-ceo", "WORKING", "Strategic Direction & Studio Vision"),
        ("raziel", "Raziel Hendrix", "CTO & Lead Orchestrator", "Engineering Leadership", "room-ceo", "WORKING", "System Orchestration & Cloud Infrastructure"),
        ("kael", "Kael Ashford", "Lead Architect", "Architecture", "room-atelier", "RESEARCHING", "Distributed Multi-Agent Architecture Specs"),
        ("nara", "Nara Vasquez", "Lead Researcher", "R&D", "room-atelier", "RESEARCHING", "Telkom University Thesis Literature & arXiv Synthesis"),
        ("senna", "Senna Louviere", "Creative Director", "Design", "room-creative", "WORKING", "Cyber-Luxury Isometric Visual Tokens"),
        ("idris", "Idris Nakamura", "Senior Developer", "Engineering", "room-dev", "WORKING", "FastAPI Telemetry Engine & Socket Streamer"),
        ("mika", "Mika Stellan", "Frontend Engineer", "Engineering", "room-dev", "WORKING", "Isometric Canvas 2.5D Rendering Engine"),
        ("viktor", "Viktor Moreau", "Lead QA & Security Engineer", "QA & Sec", "room-server", "WORKING", "Automated E2E Suite & 4-Layer Defense Audit"),
        ("elara", "Elara Sinclair", "Personal Assistant CEO", "Executive Support", "room-concierge", "WORKING", "Daily Executive Schedule & Meal Logistics"),
        ("jovan", "Jovan Aritza", "Intelligence Officer", "Field Intelligence", "room-intel", "STANDBY", "Telkom University Campus Event Radar"),
        ("daffa", "Daffa", "CEO Office", "Executive Office", "room-ceo", "WORKING", "Executive Operations & Strategic Alignment"),
    ]

    for aid, name, role, dept, expected_room, exp_status, exp_task in expected_agents:
        assert aid in agent_map
        agent = agent_map[aid]
        assert agent["name"] == name
        assert agent["role"] == role
        assert agent["department"] == dept
        assert agent["position"]["room_id"] == expected_room
        assert agent["status"] == exp_status
        assert agent["current_task"] == exp_task
        assert agent["avatar_color"].startswith("#")
        assert "cpu_footprint" in agent
        assert "ram_footprint" in agent

    # Query valid agent
    res_single = await async_client.get("/api/v1/agents/idris")
    assert res_single.status_code == 200
    assert res_single.json()["name"] == "Idris Nakamura"

    # Query non-existent agent
    res_404 = await async_client.get("/api/v1/agents/ghost")
    assert res_404.status_code == 404


# ============================================================================
# 6. Agent Action / Relocation Tests
# ============================================================================

@pytest.mark.asyncio
async def test_agent_action_move(async_client: AsyncClient):
    """Verify relocating an agent updates their position and room occupants list."""
    payload = {
        "action": "move",
        "target_room_id": "room-war",
        "new_status": "MEETING",
        "new_task": "Architecture Council Review with Kael",
    }
    res = await async_client.post("/api/v1/agents/idris/action", json=payload)
    assert res.status_code == 200
    updated = res.json()

    assert updated["position"]["room_id"] == "room-war"
    assert updated["status"] == "MEETING"
    assert updated["current_task"] == "Architecture Council Review with Kael"

    # Verify room occupants updated
    war_room = await async_client.get("/api/v1/rooms/room-war")
    assert "idris" in war_room.json()["current_occupants"]

    dev_room = await async_client.get("/api/v1/rooms/room-dev")
    assert "idris" not in dev_room.json()["current_occupants"]


@pytest.mark.asyncio
async def test_agent_action_validation_errors(async_client: AsyncClient):
    """Verify invalid agent action returns 404 or 400 appropriately."""
    # Invalid agent
    res_404 = await async_client.post(
        "/api/v1/agents/phantom/action",
        json={"target_room_id": "room-dev"},
    )
    assert res_404.status_code == 404

    # Invalid room
    res_400 = await async_client.post(
        "/api/v1/agents/idris/action",
        json={"target_room_id": "room-dimension-x"},
    )
    assert res_400.status_code == 400


@pytest.mark.asyncio
async def test_agent_action_tool_and_task_in_place(async_client: AsyncClient):
    """Verify updating agent task and active tool without moving room."""
    payload = {
        "action": "set_task",
        "new_task": "Tuning Cython vector acceleration",
        "tool": "JupyterLab Cython Extension",
        "new_status": "WORKING",
    }
    res = await async_client.post("/api/v1/agents/idris/action", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["position"]["room_id"] == "room-dev"
    assert data["current_task"] == "Tuning Cython vector acceleration"
    assert data["active_tool"] == "JupyterLab Cython Extension"


@pytest.mark.asyncio
async def test_rooms_initial_occupants_distribution(async_client: AsyncClient):
    """Verify initial baseline distribution of occupants across the 9 rooms."""
    res = await async_client.get("/api/v1/rooms")
    assert res.status_code == 200
    rooms = {r["id"]: r["current_occupants"] for r in res.json()}

    assert set(rooms["room-ceo"]) == {"dani", "raziel", "daffa"}
    assert set(rooms["room-atelier"]) == {"kael", "nara"}
    assert set(rooms["room-dev"]) == {"idris", "mika"}
    assert set(rooms["room-creative"]) == {"senna"}
    assert set(rooms["room-intel"]) == {"jovan"}
    assert set(rooms["room-concierge"]) == {"elara"}
    assert set(rooms["room-server"]) == {"viktor"}
    assert len(rooms["room-war"]) == 0
    assert len(rooms["room-pods"]) == 0


# ============================================================================
# 7. Office-Wide Macro Protocols
# ============================================================================

@pytest.mark.asyncio
async def test_gather_war_room_action(async_client: AsyncClient):
    """Verify gather_war_room protocol moves all 11 agents to War Room."""
    res = await async_client.post(
        "/api/v1/office/action",
        json={"action": "gather_war_room"},
    )
    assert res.status_code == 200
    data = res.json()

    assert data["office_mode"] == OfficeMode.WAR_ROOM.value
    war_room = [r for r in data["rooms"] if r["id"] == "room-war"][0]
    assert len(war_room["current_occupants"]) == 11

    # Ensure all agents have meeting status
    for agent in data["agents"]:
        assert agent["position"]["room_id"] == "room-war"
        assert agent["status"] == AgentStatus.MEETING.value


@pytest.mark.asyncio
async def test_trigger_sleep_cycle_action(async_client: AsyncClient):
    """Verify trigger_sleep_cycle protocol moves agents to Cyber Rest Pods."""
    res = await async_client.post(
        "/api/v1/office/action",
        json={"action": "trigger_sleep_cycle"},
    )
    assert res.status_code == 200
    data = res.json()

    assert data["office_mode"] == OfficeMode.REST_CYCLE.value
    pods_room = [r for r in data["rooms"] if r["id"] == "room-pods"][0]
    assert len(pods_room["current_occupants"]) == 11

    for agent in data["agents"]:
        assert agent["position"]["room_id"] == "room-pods"
        assert agent["status"] in (AgentStatus.RESTING.value, AgentStatus.STANDBY.value)


@pytest.mark.asyncio
async def test_resume_deep_work_action(async_client: AsyncClient):
    """Verify resume_deep_work resets all agents to their primary workstations."""
    # First disrupt to war room
    await async_client.post("/api/v1/office/action", json={"action": "gather_war_room"})

    # Now restore deep work
    res = await async_client.post(
        "/api/v1/office/action",
        json={"action": "resume_deep_work"},
    )
    assert res.status_code == 200
    data = res.json()

    assert data["office_mode"] == OfficeMode.NORMAL.value
    agent_map = {a["id"]: a for a in data["agents"]}
    assert agent_map["dani"]["position"]["room_id"] == "room-ceo"
    assert agent_map["raziel"]["position"]["room_id"] == "room-ceo"
    assert agent_map["kael"]["position"]["room_id"] == "room-atelier"
    assert agent_map["nara"]["position"]["room_id"] == "room-atelier"
    assert agent_map["senna"]["position"]["room_id"] == "room-creative"
    assert agent_map["idris"]["position"]["room_id"] == "room-dev"
    assert agent_map["mika"]["position"]["room_id"] == "room-dev"
    assert agent_map["viktor"]["position"]["room_id"] == "room-server"
    assert agent_map["elara"]["position"]["room_id"] == "room-concierge"
    assert agent_map["jovan"]["position"]["room_id"] == "room-intel"


@pytest.mark.asyncio
async def test_trigger_recreation_action(async_client: AsyncClient):
    """Verify trigger_recreation protocol moves personnel to break activities."""
    res = await async_client.post(
        "/api/v1/office/action",
        json={"action": "trigger_recreation"},
    )
    assert res.status_code == 200
    data = res.json()

    assert data["office_mode"] == OfficeMode.RECREATION.value

    # Verify room occupants distribution
    room_occupants = {r["id"]: r["current_occupants"] for r in data["rooms"]}
    assert set(room_occupants["room-concierge"]) == {"idris", "mika", "elara", "senna", "jovan", "viktor"}
    assert set(room_occupants["room-ceo"]) == {"dani", "raziel", "daffa"}
    assert set(room_occupants["room-atelier"]) == {"kael", "nara"}
    assert len(room_occupants["room-war"]) == 0
    assert len(room_occupants["room-dev"]) == 0
    assert len(room_occupants["room-creative"]) == 0
    assert len(room_occupants["room-intel"]) == 0
    assert len(room_occupants["room-pods"]) == 0
    assert len(room_occupants["room-server"]) == 0

    # Verify agent tasks and assignments
    agent_map = {a["id"]: a for a in data["agents"]}
    assert agent_map["idris"]["position"]["room_id"] == "room-concierge"
    assert agent_map["idris"]["current_task"] == "Ping-pong table match in Lounge"
    assert agent_map["mika"]["position"]["room_id"] == "room-concierge"
    assert agent_map["mika"]["current_task"] == "Ping-pong table match in Lounge"

    assert agent_map["dani"]["position"]["room_id"] == "room-ceo"
    assert agent_map["dani"]["current_task"] == "Executive lounge sofa discussing vision"
    assert agent_map["raziel"]["position"]["room_id"] == "room-ceo"
    assert agent_map["raziel"]["current_task"] == "Executive lounge sofa discussing vision"

    assert agent_map["elara"]["position"]["room_id"] == "room-concierge"
    assert agent_map["elara"]["current_task"] == "Pantry coffee bar serving refreshments"

    assert agent_map["senna"]["position"]["room_id"] == "room-concierge"
    assert agent_map["senna"]["current_task"] == "Lounge armchair sketching"

    assert agent_map["jovan"]["position"]["room_id"] == "room-concierge"
    assert agent_map["jovan"]["current_task"] == "Pantry snacks"

    assert agent_map["kael"]["position"]["room_id"] == "room-atelier"
    assert agent_map["kael"]["current_task"] == "Research library discussion"
    assert agent_map["nara"]["position"]["room_id"] == "room-atelier"
    assert agent_map["nara"]["current_task"] == "Research library discussion"

    assert agent_map["viktor"]["position"]["room_id"] == "room-concierge"
    assert agent_map["viktor"]["current_task"] == "Checking coffee machine / casual chat"

    # All agents should have RESTING status
    for agent in data["agents"]:
        assert agent["status"] == AgentStatus.RESTING.value

    # Verify audit log recorded
    recent_acts = data["recent_activities"]
    assert any(
        "CEO triggered studio break & recreation session (ping-pong & lounge active)" in act["details"]
        for act in recent_acts
    )


@pytest.mark.asyncio
async def test_all_mode_transitions(async_client: AsyncClient):
    """Verify state machine transitions seamlessly across all office modes."""
    # 1. Start in NORMAL
    res = await async_client.get("/api/v1/office/state")
    assert res.json()["office_mode"] == OfficeMode.NORMAL.value

    # 2. NORMAL -> WAR_ROOM
    res = await async_client.post("/api/v1/office/action", json={"action": "gather_war_room"})
    assert res.json()["office_mode"] == OfficeMode.WAR_ROOM.value

    # 3. WAR_ROOM -> RECREATION
    res = await async_client.post("/api/v1/office/action", json={"action": "trigger_recreation"})
    data_rec = res.json()
    assert data_rec["office_mode"] == OfficeMode.RECREATION.value
    assert len([r for r in data_rec["rooms"] if r["id"] == "room-concierge"][0]["current_occupants"]) == 6

    # 4. RECREATION -> REST_CYCLE
    res = await async_client.post("/api/v1/office/action", json={"action": "trigger_sleep_cycle"})
    data_sleep = res.json()
    assert data_sleep["office_mode"] == OfficeMode.REST_CYCLE.value
    assert len([r for r in data_sleep["rooms"] if r["id"] == "room-pods"][0]["current_occupants"]) == 11

    # 5. REST_CYCLE -> NORMAL (resume deep work)
    res = await async_client.post("/api/v1/office/action", json={"action": "resume_deep_work"})
    data_norm = res.json()
    assert data_norm["office_mode"] == OfficeMode.NORMAL.value
    agent_map = {a["id"]: a for a in data_norm["agents"]}
    assert agent_map["idris"]["position"]["room_id"] == "room-dev"
    assert agent_map["idris"]["status"] == AgentStatus.WORKING.value

    # 6. NORMAL -> RECREATION -> NORMAL
    res = await async_client.post("/api/v1/office/action", json={"action": "trigger_recreation"})
    assert res.json()["office_mode"] == OfficeMode.RECREATION.value
    res = await async_client.post("/api/v1/office/action", json={"action": "resume_deep_work"})
    assert res.json()["office_mode"] == OfficeMode.NORMAL.value


@pytest.mark.asyncio
async def test_unknown_office_action(async_client: AsyncClient):
    """Verify unknown office macro action returns 400."""
    res = await async_client.post(
        "/api/v1/office/action",
        json={"action": "invalid_protocol"},
    )
    assert res.status_code == 400
    assert "unknown office action" in res.json()["detail"].lower()
    assert "trigger_recreation" in res.json()["detail"]


# ============================================================================
# 8. Activities Endpoint Tests
# ============================================================================

@pytest.mark.asyncio
async def test_get_activities_and_limit(async_client: AsyncClient):
    """Verify activity logging and query limit filtering."""
    # Add a custom event directly
    office_engine.add_activity(
        agent_id="viktor",
        action="TEST_PROBE",
        details="Automated security test payload executed.",
        severity="INFO",
    )

    res = await async_client.get("/api/v1/activities?limit=5")
    assert res.status_code == 200
    activities = res.json()
    assert isinstance(activities, list)
    assert 1 <= len(activities) <= 5

    first = activities[0]
    assert "id" in first
    assert "agent_id" in first
    assert "action" in first
    assert "details" in first
    assert "timestamp" in first


# ============================================================================
# 9. Authentication Verification Tests
# ============================================================================

@pytest.mark.asyncio
async def test_auth_verify_success_and_failure(async_client: AsyncClient):
    """Verify PIN authentication against configured security PIN '2609'."""
    # Success with 2609
    res_ok = await async_client.post("/api/v1/auth/verify", json={"pin": "2609"})
    assert res_ok.status_code == 200
    body_ok = res_ok.json()
    assert body_ok["success"] is True
    assert body_ok["token"] is not None
    assert "granted" in body_ok["message"].lower()

    # Failure with incorrect PIN
    res_bad = await async_client.post("/api/v1/auth/verify", json={"pin": "0000"})
    assert res_bad.status_code == 401
    body_bad = res_bad.json()
    assert body_bad["success"] is False
    assert body_bad["token"] is None


# ============================================================================
# 10. Server-Sent Events (SSE) Stream Test
# ============================================================================

@pytest.mark.asyncio
async def test_sse_stream_initialization(async_client: AsyncClient):
    """Verify /api/v1/stream delivers SSE headers and immediate snapshot event."""
    response = await async_client.get("/api/v1/stream?max_events=1")
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    text = response.text
    assert "event: state" in text
    assert "data:" in text

    # Extract json payload
    found_data = False
    for line in text.splitlines():
        if line.startswith("data:"):
            payload_str = line[len("data:"):].strip()
            parsed = json.loads(payload_str)
            assert "agents" in parsed
            assert len(parsed["agents"]) == 11
            assert parsed["office_mode"] == "NORMAL"
            found_data = True
            break
    assert found_data, "SSE response did not contain data payload"


# ============================================================================
# 11. Static Frontend and Asset Delivery Tests
# ============================================================================

@pytest.mark.asyncio
async def test_frontend_static_serving(async_client: AsyncClient):
    """Verify static mount serves index.html on / and static assets."""
    res = await async_client.get("/")
    assert res.status_code == 200
    assert "<!DOCTYPE html>" in res.text
    assert "Yudiaz Virtual HQ" in res.text

    # Asset check
    res_asset = await async_client.get("/assets/yudiaz-logo-mark.svg")
    assert res_asset.status_code == 200
    assert "svg" in res_asset.headers["content-type"]


# ============================================================================
# 12. OfficeEngine Unit and Seat Allocation Tests
# ============================================================================

def test_office_engine_direct_methods():
    """Direct unit testing of OfficeEngine helper methods."""
    engine = OfficeEngine()
    state = engine.get_state()
    assert len(state.agents) == 11
    assert len(state.rooms) == 9

    # Test seat position distribution
    room = engine.rooms["room-dev"]
    pos0 = engine._calculate_seat_position(room, 0)
    pos1 = engine._calculate_seat_position(room, 1)
    # Positions should not overlap exactly
    assert (pos0.x, pos0.y) != (pos1.x, pos1.y)

    # Test direct activity addition and cap
    for i in range(120):
        engine.add_activity(
            agent_id="idris",
            action=f"BENCHMARK_TICK_{i}",
            details="Engine capacity stress test",
        )
    activities = engine.get_activities(limit=50)
    assert len(activities) == 50
    assert activities[0].action == "BENCHMARK_TICK_119"

    # Test direct trigger_recreation method
    rec_state = engine.trigger_recreation()
    assert rec_state.office_mode == OfficeMode.RECREATION
    assert len([r for r in rec_state.rooms if r.id == "room-concierge"][0].current_occupants) == 6
    assert len([r for r in rec_state.rooms if r.id == "room-ceo"][0].current_occupants) == 3
    assert len([r for r in rec_state.rooms if r.id == "room-atelier"][0].current_occupants) == 2


@pytest.mark.asyncio
async def test_simulation_tick_and_lifecycle():
    """Verify OfficeEngine simulation worker start and stop lifecycle."""
    engine = OfficeEngine()
    engine.settings.simulation_interval = 0.05
    await engine.start_simulation()
    assert engine._is_running is True
    assert engine._simulation_task is not None

    import asyncio
    await asyncio.sleep(0.15)
    assert engine._sim_ticks >= 1

    await engine.stop_simulation()
    assert engine._is_running is False
    assert engine._simulation_task is None
