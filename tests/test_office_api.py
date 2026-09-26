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
from backend.models import AgentStatus, MeetingMinutes, OfficeMode
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
    assert data_root["total_rooms"] == 11

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
    """Verify full spatial state contains 11 agents, 11 rooms, and valid telemetry."""
    res = await async_client.get("/api/v1/office/state")
    assert res.status_code == 200
    data = res.json()

    assert data["office_mode"] == OfficeMode.NORMAL.value
    assert len(data["agents"]) == 11
    assert len(data["rooms"]) == 11
    assert "server_telemetry" in data
    assert "uptime_seconds" in data["server_telemetry"]
    assert "cpu_load_percent" in data["server_telemetry"]
    assert len(data["recent_activities"]) >= 1


# ============================================================================
# 4. Rooms Tests
# ============================================================================

@pytest.mark.asyncio
async def test_list_and_get_rooms(async_client: AsyncClient):
    """Verify listing all 11 rooms and querying individual rooms by id."""
    res = await async_client.get("/api/v1/rooms")
    assert res.status_code == 200
    rooms = res.json()
    assert len(rooms) == 11

    expected_rooms = {
        "room-ceo": ("CEO Suite", "Executive", 6, [520.0, 340.0]),
        "room-cto": ("CTO Executive Suite", "Engineering Leadership", 4, [520.0, 1850.0]),
        "room-pa": ("Executive Assistant Office", "Executive Support", 4, [1300.0, 1850.0]),
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

    # Query room-cto
    res_cto = await async_client.get("/api/v1/rooms/room-cto")
    assert res_cto.status_code == 200
    cto_data = res_cto.json()
    assert cto_data["name"] == "CTO Executive Suite"
    assert cto_data["category"] == "Engineering Leadership"

    # Query room-pa
    res_pa = await async_client.get("/api/v1/rooms/room-pa")
    assert res_pa.status_code == 200
    pa_data = res_pa.json()
    assert pa_data["name"] == "Executive Assistant Office"
    assert pa_data["category"] == "Executive Support"

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
        ("raziel", "Raziel Hendrix", "CTO & Lead Orchestrator", "Engineering Leadership", "room-cto", "WORKING", "System Orchestration & Cloud Infrastructure"),
        ("kael", "Kael Ashford", "Lead Architect", "Architecture", "room-atelier", "RESEARCHING", "Distributed Multi-Agent Architecture Specs"),
        ("nara", "Nara Vasquez", "Lead Researcher", "R&D", "room-atelier", "RESEARCHING", "Telkom University Thesis Literature & arXiv Synthesis"),
        ("senna", "Senna Louviere", "Creative Director", "Design", "room-creative", "WORKING", "Cyber-Luxury Isometric Visual Tokens"),
        ("idris", "Idris Nakamura", "Senior Developer", "Engineering", "room-dev", "WORKING", "FastAPI Telemetry Engine & Socket Streamer"),
        ("mika", "Mika Stellan", "Frontend Engineer", "Engineering", "room-dev", "WORKING", "Isometric Canvas 2.5D Rendering Engine"),
        ("viktor", "Viktor Moreau", "Lead QA & Security Engineer", "QA & Sec", "room-server", "WORKING", "Automated E2E Suite & 4-Layer Defense Audit"),
        ("elara", "Elara Sinclair", "Personal Assistant to CEO", "Executive Support", "room-pa", "WORKING", "Executive Calendar, Briefing Prep & Priority Logistics"),
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
    """Verify initial baseline distribution of occupants across the 11 rooms."""
    res = await async_client.get("/api/v1/rooms")
    assert res.status_code == 200
    rooms = {r["id"]: r["current_occupants"] for r in res.json()}

    assert set(rooms["room-ceo"]) == {"dani", "daffa"}
    assert set(rooms["room-cto"]) == {"raziel"}
    assert set(rooms["room-pa"]) == {"elara"}
    assert set(rooms["room-atelier"]) == {"kael", "nara"}
    assert set(rooms["room-dev"]) == {"idris", "mika"}
    assert set(rooms["room-creative"]) == {"senna"}
    assert set(rooms["room-intel"]) == {"jovan"}
    assert set(rooms["room-server"]) == {"viktor"}
    assert len(rooms["room-concierge"]) == 0
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
    assert agent_map["daffa"]["position"]["room_id"] == "room-ceo"
    assert agent_map["raziel"]["position"]["room_id"] == "room-cto"
    assert agent_map["elara"]["position"]["room_id"] == "room-pa"
    assert agent_map["kael"]["position"]["room_id"] == "room-atelier"
    assert agent_map["nara"]["position"]["room_id"] == "room-atelier"
    assert agent_map["senna"]["position"]["room_id"] == "room-creative"
    assert agent_map["idris"]["position"]["room_id"] == "room-dev"
    assert agent_map["mika"]["position"]["room_id"] == "room-dev"
    assert agent_map["viktor"]["position"]["room_id"] == "room-server"
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
    assert agent_map["elara"]["current_task"] == "Espresso break & casual executive chat"

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
    assert len(state.rooms) == 11

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


# ============================================================================
# 13. Autonomous Studio Simulation Engine Tests
# ============================================================================

def test_autonomous_council_meeting_simulation():
    """Verify autonomous council convening in War Room with explicit leadership logs."""
    engine = OfficeEngine()
    engine.resume_deep_work()

    # 1. First council: CEO Daniandra leads
    engine._simulate_tick(force_event="council")
    assert engine._council_active is True
    assert len(engine.rooms["room-war"].current_occupants) == 11

    # Check leadership log explicitly mentions CEO Daniandra is leading the meeting
    council_logs = [a for a in engine.get_activities(limit=10) if a.action == "COUNCIL_CONVENED"]
    assert len(council_logs) >= 1
    first_log = council_logs[0]
    assert first_log.agent_id == "dani"
    assert "CEO Daniandra is leading the meeting" in first_log.details

    # Verify all agents are in meeting status
    for agent in engine.agents.values():
        assert agent.position.room_id == "room-war"
        assert agent.status == AgentStatus.MEETING

    # Advance ticks to complete meeting duration (4 ticks)
    for _ in range(4):
        engine._simulate_tick()

    # Verify employees naturally returned to their designated workstations
    assert engine._council_active is False
    assert len(engine.rooms["room-war"].current_occupants) == 0
    assert engine.agents["dani"].position.room_id == "room-ceo"
    assert engine.agents["daffa"].position.room_id == "room-ceo"
    assert engine.agents["raziel"].position.room_id == "room-cto"
    assert engine.agents["elara"].position.room_id == "room-pa"
    assert engine.agents["idris"].position.room_id == "room-dev"
    assert engine.agents["mika"].position.room_id == "room-dev"

    # Verify council conclusion audit log
    concluded_logs = [a for a in engine.get_activities(limit=10) if a.action == "COUNCIL_CONCLUDED"]
    assert len(concluded_logs) >= 1

    # 2. Second council: Daffa (CEO Office) leads
    engine._simulate_tick(force_event="council")
    assert engine._council_active is True
    daffa_logs = [a for a in engine.get_activities(limit=5) if a.action == "COUNCIL_CONVENED"]
    assert len(daffa_logs) >= 1
    assert daffa_logs[0].agent_id == "daffa"
    assert "Daffa (CEO Office) is leading the meeting" in daffa_logs[0].details


def test_autonomous_ping_pong_simulation():
    """Verify autonomous ping-pong breaks in room-concierge and natural return."""
    engine = OfficeEngine()
    engine.resume_deep_work()

    # Trigger ping-pong break
    engine._simulate_tick(force_event="ping_pong")

    # Check that 2 employees moved to room-concierge
    concierge_occupants = engine.rooms["room-concierge"].current_occupants
    assert len(concierge_occupants) == 2
    for p_id in concierge_occupants:
        agent = engine.agents[p_id]
        assert agent.position.room_id == "room-concierge"
        assert agent.status == AgentStatus.RESTING
        assert "ping-pong" in agent.current_task.lower()
        assert agent.active_tool == "Ping-Pong Paddle"

    # Check activity log emitted
    pingpong_logs = [a for a in engine.get_activities(limit=5) if a.action == "PING_PONG_MATCH"]
    assert len(pingpong_logs) >= 1
    assert "play ping-pong in room-concierge" in pingpong_logs[0].details

    # Advance ticks for duration (3 ticks)
    for _ in range(3):
        engine._simulate_tick()

    # Verify both returned to workstations for deep focus
    assert len(engine.rooms["room-concierge"].current_occupants) == 0
    return_logs = [a for a in engine.get_activities(limit=10) if a.action == "DEEP_WORK_RETURN"]
    assert len(return_logs) >= 2


def test_autonomous_coffee_and_pod_simulation():
    """Verify autonomous lounge coffee breaks and rest pod sleep/recovery cycles."""
    engine = OfficeEngine()
    engine.resume_deep_work()

    # 1. Coffee break in Lounge
    engine._simulate_tick(force_event="coffee")
    assert len(engine.rooms["room-concierge"].current_occupants) >= 1
    coffee_logs = [a for a in engine.get_activities(limit=5) if a.action == "COFFEE_BREAK"]
    assert len(coffee_logs) >= 1
    assert "get coffee" in coffee_logs[0].details

    # Advance 2 ticks to return from coffee
    for _ in range(2):
        engine._simulate_tick()
    assert len(engine.rooms["room-concierge"].current_occupants) == 0

    # 2. Rest pod sleep/recovery in room-pods
    engine._simulate_tick(force_event="pod")
    assert len(engine.rooms["room-pods"].current_occupants) >= 1
    pod_logs = [a for a in engine.get_activities(limit=5) if a.action == "POD_RECOVERY"]
    assert len(pod_logs) >= 1
    assert "sleep and" in pod_logs[0].details

    # Advance 3 ticks to return from pods
    for _ in range(3):
        engine._simulate_tick()
    assert len(engine.rooms["room-pods"].current_occupants) == 0


def test_autonomous_natural_schedule_progression():
    """Verify the autonomous simulation advances through natural organic schedule."""
    engine = OfficeEngine()
    engine.resume_deep_work()

    # Advance 40 simulation ticks and observe spontaneous events
    for _ in range(40):
        engine._simulate_tick()

    assert engine._sim_ticks == 40
    activities = engine.get_activities(limit=100)
    actions = {a.action for a in activities}

    # Should have triggered council, ping pong, coffee, pods, return to deep work, and ambient actions
    assert "COUNCIL_CONVENED" in actions
    assert "PING_PONG_MATCH" in actions
    assert "COFFEE_BREAK" in actions
    assert "POD_RECOVERY" in actions
    assert "DEEP_WORK_RETURN" in actions


def test_autonomous_sse_stream_broadcast():
    """Verify each simulation tick broadcasts updated office state over SSE queue."""
    engine = OfficeEngine()
    q = engine.subscribe()
    assert q.qsize() == 0

    engine._simulate_tick()
    assert q.qsize() >= 1

    event_payload = q.get_nowait()
    data = json.loads(event_payload)
    assert len(data["agents"]) == 11
    assert len(data["rooms"]) == 11
    assert data["server_telemetry"]["sim_ticks"] == 1

    engine.unsubscribe(q)


# ============================================================================
# 14. Meeting Minutes (MoM) & Deliberation Lifecycle Tests
# ============================================================================

@pytest.mark.asyncio
async def test_get_latest_meeting_endpoint(async_client: AsyncClient):
    """Verify GET /api/v1/meetings/latest returns rich Minutes of Meeting structure."""
    res = await async_client.get("/api/v1/meetings/latest")
    assert res.status_code == 200
    mom = res.json()

    # Core metadata
    assert mom is not None
    assert "meeting_id" in mom
    assert mom["title"] == "Evaluasi Infrastruktur Studio, Skripsi Telkom University & Autonomous Virtual HQ"
    assert mom["leader_name"] in ("Daniandra Prayudisty (CEO)", "Daffa (CEO Office)")
    assert mom["status"] in ("IN_PROGRESS", "COMPLETED")
    assert "started_at" in mom

    # Attendees: all 11 studio agents present
    assert len(mom["attendees"]) == 11
    expected_agents = ["Daniandra", "Daffa", "Raziel", "Kael", "Nara", "Senna", "Idris", "Mika", "Viktor", "Elara", "Jovan"]
    for agent_name in expected_agents:
        assert any(agent_name in att for att in mom["attendees"]), f"Missing {agent_name} in attendees"

    # Dialogues: rich sequenced statements from all divisions
    dialogues = mom["dialogues"]
    assert len(dialogues) >= 11
    speaker_ids = {d["speaker_id"] for d in dialogues}
    assert "dani" in speaker_ids
    assert "daffa" in speaker_ids
    assert "raziel" in speaker_ids
    assert "kael" in speaker_ids
    assert "nara" in speaker_ids
    assert "senna" in speaker_ids
    assert "idris" in speaker_ids
    assert "mika" in speaker_ids
    assert "viktor" in speaker_ids
    assert "elara" in speaker_ids
    assert "jovan" in speaker_ids

    # Each dialogue has speaker_id, speaker_name, role, text
    for d in dialogues:
        assert d["speaker_id"]
        assert d["speaker_name"]
        assert d["role"]
        assert len(d["text"]) > 10

    # Key decisions: strategic consensus items
    assert len(mom["key_decisions"]) >= 4
    assert any("Telkom University" in dec for dec in mom["key_decisions"])
    assert any("LaTeX" in dec or "Tectonic" in dec for dec in mom["key_decisions"])

    # Action items: commitments with PIC, task, and due date
    assert len(mom["action_items"]) >= 6
    for item in mom["action_items"]:
        assert item["pic"]
        assert item["task"]
        assert item["due"]


@pytest.mark.asyncio
async def test_meeting_minutes_in_office_state(async_client: AsyncClient):
    """Verify full office state snapshot includes latest_meeting."""
    res = await async_client.get("/api/v1/office/state")
    assert res.status_code == 200
    data = res.json()

    assert "latest_meeting" in data
    assert data["latest_meeting"] is not None
    assert data["latest_meeting"]["title"] == "Evaluasi Infrastruktur Studio, Skripsi Telkom University & Autonomous Virtual HQ"
    assert len(data["latest_meeting"]["dialogues"]) >= 11


@pytest.mark.asyncio
async def test_meeting_minutes_lifecycle_transitions(async_client: AsyncClient):
    """Verify meeting status transitions from IN_PROGRESS during War Room to COMPLETED on deep work return."""
    # Convene War Room
    res_war = await async_client.post("/api/v1/office/action", json={"action": "gather_war_room"})
    assert res_war.status_code == 200
    war_state = res_war.json()
    assert war_state["latest_meeting"]["status"] == "IN_PROGRESS"

    # Query latest meeting directly
    res_mom = await async_client.get("/api/v1/meetings/latest")
    assert res_mom.status_code == 200
    assert res_mom.json()["status"] == "IN_PROGRESS"

    # Resume Deep Work -> meeting marks as COMPLETED
    res_resume = await async_client.post("/api/v1/office/action", json={"action": "resume_deep_work"})
    assert res_resume.status_code == 200
    resumed_state = res_resume.json()
    assert resumed_state["latest_meeting"]["status"] == "COMPLETED"

    # Query endpoint after completion
    res_mom_after = await async_client.get("/api/v1/meetings/latest")
    assert res_mom_after.status_code == 200
    assert res_mom_after.json()["status"] == "COMPLETED"


def test_autonomous_council_meeting_minutes_generation():
    """Verify engine generates rich minutes for both CEO Daniandra and Daffa council sessions."""
    engine = OfficeEngine()
    engine.resume_deep_work()

    # 1. Convene CEO Daniandra session
    engine._simulate_tick(force_event="council")
    assert engine._council_active is True
    assert engine.latest_meeting is not None
    assert engine.latest_meeting.leader_name == "Daniandra Prayudisty (CEO)"
    assert engine.latest_meeting.status == "IN_PROGRESS"
    assert len(engine.latest_meeting.dialogues) == 12

    # Step through council deliberation
    for _ in range(4):
        engine._simulate_tick()

    assert engine._council_active is False
    assert engine.latest_meeting.status == "COMPLETED"

    # 2. Convene Daffa (CEO Office) session
    engine._simulate_tick(force_event="council")
    assert engine._council_active is True
    assert engine.latest_meeting.leader_name == "Daffa (CEO Office)"
    assert engine.latest_meeting.status == "IN_PROGRESS"
    assert len(engine.latest_meeting.dialogues) == 12

    for _ in range(4):
        engine._simulate_tick()

    assert engine._council_active is False
    assert engine.latest_meeting.status == "COMPLETED"
