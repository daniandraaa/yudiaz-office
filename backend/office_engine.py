"""Spatial simulation engine and real-time state machine for Yudiaz Virtual HQ.

Manages 10 autonomous agents across 9 cyber-luxury zones, coordinates spatial
positioning, mode switches (War Room, Deep Work, Rest Cycle), audit logging,
and live telemetry streaming.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import random
import time
from typing import Any, Optional
import uuid

from backend.config import get_settings
from backend.models import (
    ActivityLog,
    AgentInfo,
    AgentPosition,
    AgentStatus,
    OfficeMode,
    OfficeStateResponse,
    RoomInfo,
)


class OfficeEngine:
    """Stateful spatial multi-agent coordination engine."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._start_time = time.time()
        self._sim_ticks = 0
        self._office_mode = OfficeMode.NORMAL
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._simulation_task: Optional[asyncio.Task[None]] = None
        self._is_running = False

        # Master registries
        self.rooms: dict[str, RoomInfo] = {}
        self.agents: dict[str, AgentInfo] = {}
        self.activities: list[ActivityLog] = []

        # Baseline definitions for deep work restoration
        self._baseline_agents: dict[str, dict[str, Any]] = {}

        self._initialize_rooms()
        self._initialize_agents()
        self._seed_initial_activity()

    def _initialize_rooms(self) -> None:
        """Seed the 9 distinct physical architectural zones matching the 3D diorama building layout."""
        room_configs = [
            {
                "id": "room-ceo",
                "name": "CEO Suite",
                "category": "Executive",
                "capacity": 6,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (520.0, 340.0),
                "description": "CEO Suite - Private executive command chamber for high-level studio governance, strategic vision, and private executive lounge.",
                "status_accent": "#10B981",  # Cyber Emerald
            },
            {
                "id": "room-war",
                "name": "Conference Room",
                "category": "Deliberation",
                "capacity": 12,
                "floor": 1,
                "dimensions": (580.0, 310.0),
                "center_coord": (1300.0, 290.0),
                "description": "Conference Room - All-hands high-stakes strategy amphitheater with circular conference table and holographic 3D monogram projection.",
                "status_accent": "#F59E0B",  # Amber Deliberation
            },
            {
                "id": "room-creative",
                "name": "Creative Studio",
                "category": "Creative",
                "capacity": 6,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (2080.0, 340.0),
                "description": "Creative Studio - Visual prototyping suite for cyber-luxury design tokens, UI canvas assets, and 3D wireframe polyhedra.",
                "status_accent": "#A855F7",  # Neon Purple
            },
            {
                "id": "room-dev",
                "name": "Workstations",
                "category": "Engineering",
                "capacity": 6,
                "floor": 1,
                "dimensions": (560.0, 300.0),
                "center_coord": (470.0, 840.0),
                "description": "Workstations - High-density developer workstation cluster powering core microservices, neural pipelines, and streaming engines.",
                "status_accent": "#00F2FE",  # Matrix Cyan
            },
            {
                "id": "room-atelier",
                "name": "Research Library",
                "category": "R&D",
                "capacity": 8,
                "floor": 1,
                "dimensions": (580.0, 310.0),
                "center_coord": (1300.0, 820.0),
                "description": "Research Library - Architectural R&D atelier and quiet zone for multi-agent system design, academic literature synthesis, and patent exploration.",
                "status_accent": "#6366F1",  # Iris Synthesis
            },
            {
                "id": "room-intel",
                "name": "Radar NOC",
                "category": "Operations",
                "capacity": 6,
                "floor": 1,
                "dimensions": (560.0, 300.0),
                "center_coord": (2130.0, 840.0),
                "description": "Radar NOC - Network operations center monitoring academic pulses, campus radar, and competitive intel.",
                "status_accent": "#10B981",  # Radar Emerald
            },
            {
                "id": "room-concierge",
                "name": "Lounge & Ping-Pong",
                "category": "Hospitality",
                "capacity": 8,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (520.0, 1350.0),
                "description": "Lounge & Ping-Pong - Executive concierge, pantry coffee bar, recreation area with championship ping-pong table and lounge seating.",
                "status_accent": "#F59E0B",  # Warm Amber Glow
            },
            {
                "id": "room-pods",
                "name": "Bedroom & Rest Pods",
                "category": "Resting",
                "capacity": 8,
                "floor": 1,
                "dimensions": (580.0, 310.0),
                "center_coord": (1300.0, 1370.0),
                "description": "Bedroom & Rest Pods - Biometric rest quarters, sensory deprivation pods, and restorative bio-rhythm recharging stations.",
                "status_accent": "#38BDF8",  # Cyber Blue Rest
            },
            {
                "id": "room-server",
                "name": "Server Room",
                "category": "Infrastructure",
                "capacity": 4,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (2080.0, 1350.0),
                "description": "Server Room - Cold-aisle fortified data vault housing core servers, high-availability neural gateway, and secure HSMs.",
                "status_accent": "#EF4444",  # Crimson Vault
            },
        ]

        for config in room_configs:
            self.rooms[config["id"]] = RoomInfo(
                id=config["id"],
                name=config["name"],
                category=config["category"],
                capacity=config["capacity"],
                floor=config["floor"],
                dimensions=config["dimensions"],
                center_coord=config["center_coord"],
                description=config["description"],
                current_occupants=[],
                status_accent=config["status_accent"],
            )

    def _initialize_agents(self) -> None:
        """Seed the 10 Yudiaz virtual studio personnel."""
        agent_configs = [
            {
                "id": "dani",
                "name": "Daniandra Prayudisty",
                "role": "Founder & CEO",
                "department": "Executive",
                "room_id": "room-ceo",
                "status": AgentStatus.WORKING,
                "task": "Strategic Direction & Studio Vision",
                "avatar_color": "#FFD700",
                "tool": "Notion Strategic Roadmap",
                "context": "Yudiaz Creative Studio governance & executive roadmaps",
            },
            {
                "id": "raziel",
                "name": "Raziel Hendrix",
                "role": "CTO & Lead Orchestrator",
                "department": "Engineering Leadership",
                "room_id": "room-ceo",
                "status": AgentStatus.WORKING,
                "task": "System Orchestration & Cloud Infrastructure",
                "avatar_color": "#7928CA",
                "tool": "Hermes Agent Hub",
                "context": "Telemetry mesh, multi-agent synchronizer, high-availability cluster",
            },
            {
                "id": "kael",
                "name": "Kael Ashford",
                "role": "Lead Architect",
                "department": "Architecture",
                "room_id": "room-atelier",
                "status": AgentStatus.RESEARCHING,
                "task": "Distributed Multi-Agent Architecture Specs",
                "avatar_color": "#0070F3",
                "tool": "ADR & System Graph Generator",
                "context": "Modular service mesh, WebSocket/SSE streaming topology",
            },
            {
                "id": "nara",
                "name": "Nara Vasquez",
                "role": "Lead Researcher",
                "department": "R&D",
                "room_id": "room-atelier",
                "status": AgentStatus.RESEARCHING,
                "task": "Telkom University Thesis Literature & arXiv Synthesis",
                "avatar_color": "#00DFD8",
                "tool": "arXiv Explorer & Zotero",
                "context": "Autonomous agent coordination protocols and university research archive",
            },
            {
                "id": "senna",
                "name": "Senna Louviere",
                "role": "Creative Director",
                "department": "Design",
                "room_id": "room-creative",
                "status": AgentStatus.WORKING,
                "task": "Cyber-Luxury Isometric Visual Tokens",
                "avatar_color": "#FF0080",
                "tool": "Figma & Shader Visualizer",
                "context": "Cyber-luxury design system, gold-mesh trims, dark-glass aesthetics",
            },
            {
                "id": "idris",
                "name": "Idris Nakamura",
                "role": "Senior Developer",
                "department": "Engineering",
                "room_id": "room-dev",
                "status": AgentStatus.WORKING,
                "task": "FastAPI Telemetry Engine & Socket Streamer",
                "avatar_color": "#00FF66",
                "tool": "VS Code & Python REPL",
                "context": "Async state engine, SSE event broadcaster, Pydantic type contracts",
            },
            {
                "id": "mika",
                "name": "Mika Stellan",
                "role": "Frontend Engineer",
                "department": "Engineering",
                "room_id": "room-dev",
                "status": AgentStatus.WORKING,
                "task": "Isometric Canvas 2.5D Rendering Engine",
                "avatar_color": "#FF8800",
                "tool": "Chrome DevTools & CanvasProfiler",
                "context": "HTML5 2.5D Isometric viewport, dynamic agent avatars, status glow",
            },
            {
                "id": "viktor",
                "name": "Viktor Moreau",
                "role": "Lead QA & Security Engineer",
                "department": "QA & Sec",
                "room_id": "room-server",
                "status": AgentStatus.WORKING,
                "task": "Automated E2E Suite & 4-Layer Defense Audit",
                "avatar_color": "#FF3333",
                "tool": "Pytest Suite & Security Scanner",
                "context": "E2E endpoint verification, PIN auth brute-force gate, test coverage",
            },
            {
                "id": "elara",
                "name": "Elara Sinclair",
                "role": "Personal Assistant CEO",
                "department": "Executive Support",
                "room_id": "room-concierge",
                "status": AgentStatus.WORKING,
                "task": "Daily Executive Schedule & Meal Logistics",
                "avatar_color": "#E0AAFF",
                "tool": "Google Calendar & Pantry Inventory",
                "context": "Daniandra's itinerary, concierge dining order, calendar dispatch",
            },
            {
                "id": "jovan",
                "name": "Jovan Aritza",
                "role": "Intelligence Officer",
                "department": "Field Intelligence",
                "room_id": "room-intel",
                "status": AgentStatus.STANDBY,
                "task": "Telkom University Campus Event Radar",
                "avatar_color": "#39FF14",
                "tool": "Campus Radar NOC Feeds",
                "context": "Telkom University academic calendar, symposium alerts, student research feeds",
            },
            {
                "id": "daffa",
                "name": "Daffa",
                "role": "CEO Office",
                "department": "Executive Office",
                "room_id": "room-ceo",
                "status": AgentStatus.WORKING,
                "task": "Executive Operations & Strategic Alignment",
                "avatar_color": "#38BDF8",
                "tool": "Executive Dashboard & Notion",
                "context": "CEO Office operations, cross-department coordination, strategic follow-ups",
            },
        ]

        for cfg in agent_configs:
            self._baseline_agents[cfg["id"]] = dict(cfg)
            room = self.rooms[cfg["room_id"]]
            slot = len(room.current_occupants)
            room.current_occupants.append(cfg["id"])
            pos = self._calculate_seat_position(room, slot)

            self.agents[cfg["id"]] = AgentInfo(
                id=cfg["id"],
                name=cfg["name"],
                role=cfg["role"],
                department=cfg["department"],
                avatar_color=cfg["avatar_color"],
                status=cfg["status"],
                position=pos,
                current_task=cfg["task"],
                active_tool=cfg["tool"],
                memory_context=cfg["context"],
                cpu_footprint=round(random.uniform(15.0, 45.0), 1),
                ram_footprint=round(random.uniform(120.0, 350.0), 1),
                updated_at=datetime.now(timezone.utc).isoformat(),
            )

    def _seed_initial_activity(self) -> None:
        """Add welcoming bootstrap audit log entries."""
        self.add_activity(
            agent_id="raziel",
            action="SYSTEM_ONLINE",
            details="Yudiaz Virtual HQ Spatial Engine booted. 11 autonomous agents deployed across 9 zones.",
            severity="SYSTEM",
        )
        self.add_activity(
            agent_id="dani",
            action="OFFICE_INSPECTION",
            details="Daniandra Prayudisty initiated daily studio oversight from the Executive Suite.",
            severity="INFO",
        )

    def _calculate_seat_position(self, room: RoomInfo, slot_index: int) -> AgentPosition:
        """Calculate spatial coordinates for occupants distributed inside a room."""
        cx, cy = room.center_coord
        # Subtle offset grid around center
        col = (slot_index % 3) - 1  # -1, 0, 1
        row = (slot_index // 3) - 0.5  # -0.5, 0.5, etc.
        offset_x = col * 36.0
        offset_y = row * 32.0

        return AgentPosition(
            x=round(cx + offset_x, 1),
            y=round(cy + offset_y, 1),
            z=0.0,
            room_id=room.id,
        )

    def _reposition_room_occupants(self, room_id: str) -> None:
        """Update spatial positions for all agents occupying a given room."""
        room = self.rooms.get(room_id)
        if not room:
            return

        for idx, agent_id in enumerate(room.current_occupants):
            agent = self.agents.get(agent_id)
            if agent:
                agent.position = self._calculate_seat_position(room, idx)
                agent.position.room_id = room.id
                agent.updated_at = datetime.now(timezone.utc).isoformat()

    def get_state(self) -> OfficeStateResponse:
        """Compile a complete, fresh snapshot of virtual office state."""
        uptime = round(time.time() - self._start_time, 1)

        # Dynamic telemetry
        avg_cpu = round(
            sum(a.cpu_footprint for a in self.agents.values()) / max(len(self.agents), 1),
            1,
        )
        total_ram = round(sum(a.ram_footprint for a in self.agents.values()), 1)

        telemetry = {
            "uptime_seconds": uptime,
            "cpu_load_percent": avg_cpu,
            "memory_load_mb": total_ram,
            "active_agents": len(self.agents),
            "total_rooms": len(self.rooms),
            "sim_ticks": self._sim_ticks,
            "active_stream_clients": len(self._subscribers),
        }

        return OfficeStateResponse(
            timestamp=datetime.now(timezone.utc).isoformat(),
            office_mode=self._office_mode,
            agents=list(self.agents.values()),
            rooms=list(self.rooms.values()),
            recent_activities=list(reversed(self.activities[-50:])),
            server_telemetry=telemetry,
        )

    def get_agent(self, agent_id: str) -> Optional[AgentInfo]:
        """Fetch single agent state by identifier."""
        return self.agents.get(agent_id)

    def get_room(self, room_id: str) -> Optional[RoomInfo]:
        """Fetch single room state by identifier."""
        return self.rooms.get(room_id)

    def get_activities(self, limit: int = 50) -> list[ActivityLog]:
        """Fetch recent activity logs newest first."""
        limit = max(1, min(limit, 200))
        return list(reversed(self.activities[-limit:]))

    def add_activity(
        self,
        agent_id: str,
        action: str,
        details: str = "",
        severity: str = "INFO",
        room_id: Optional[str] = None,
    ) -> ActivityLog:
        """Record an activity log entry and broadcast to active streams."""
        agent = self.agents.get(agent_id)
        agent_name = agent.name if agent else agent_id
        target_room = room_id or (agent.position.room_id if agent else "room-server")

        log = ActivityLog(
            id=f"act-{uuid.uuid4().hex[:8]}",
            timestamp=datetime.now(timezone.utc).isoformat(),
            agent_id=agent_id,
            agent_name=agent_name,
            room_id=target_room,
            action=action,
            details=details,
            severity=severity,
        )

        self.activities.append(log)
        # Cap activity history
        if len(self.activities) > 300:
            self.activities = self.activities[-200:]

        self._broadcast_state()
        return log

    def move_agent(
        self,
        agent_id: str,
        target_room_id: str,
        new_status: Optional[AgentStatus] = None,
        new_task: Optional[str] = None,
        tool: Optional[str] = None,
    ) -> AgentInfo:
        """Relocate an agent to a new room with updated status and assignment."""
        agent = self.agents.get(agent_id)
        if not agent:
            raise KeyError(f"Agent '{agent_id}' does not exist.")

        target_room = self.rooms.get(target_room_id)
        if not target_room:
            raise KeyError(f"Room '{target_room_id}' does not exist.")

        old_room_id = agent.position.room_id
        if old_room_id != target_room_id:
            old_room = self.rooms.get(old_room_id)
            if old_room and agent_id in old_room.current_occupants:
                old_room.current_occupants.remove(agent_id)
                self._reposition_room_occupants(old_room_id)

            if agent_id not in target_room.current_occupants:
                target_room.current_occupants.append(agent_id)

        if new_status:
            agent.status = new_status
        if new_task:
            agent.current_task = new_task
        if tool is not None:
            agent.active_tool = tool

        self._reposition_room_occupants(target_room_id)
        agent.updated_at = datetime.now(timezone.utc).isoformat()

        self.add_activity(
            agent_id=agent.id,
            action="AGENT_RELOCATED",
            details=f"{agent.name} transitioned to {target_room.name}. Task: '{agent.current_task}'",
            severity="INFO",
            room_id=target_room_id,
        )

        return agent

    def gather_war_room(self) -> OfficeStateResponse:
        """Trigger War Room protocol: move all personnel to War Room Amphitheater."""
        self._office_mode = OfficeMode.WAR_ROOM
        war_room = self.rooms["room-war"]

        # Clear occupants across all rooms
        for room in self.rooms.values():
            room.current_occupants.clear()

        # Place all agents into War Room
        for agent_id, agent in self.agents.items():
            war_room.current_occupants.append(agent_id)
            agent.status = AgentStatus.MEETING
            if agent_id in ("dani", "raziel", "daffa"):
                agent.current_task = "Convening Studio Council & High-Priority Strategy Briefing"
            else:
                agent.current_task = "All-Hands Strategic Alignment & Studio Directives"
            agent.updated_at = datetime.now(timezone.utc).isoformat()

        self._reposition_room_occupants("room-war")

        self.add_activity(
            agent_id="dani",
            action="WAR_ROOM_CONVENED",
            details="Executive Command initiated War Room protocol. All 11 agents assembled in Strategy Amphitheater.",
            severity="ALERT",
            room_id="room-war",
        )

        return self.get_state()

    def resume_deep_work(self) -> OfficeStateResponse:
        """Disperse personnel back to core workstations and resume deep focus."""
        self._office_mode = OfficeMode.NORMAL

        # Clear occupants across all rooms
        for room in self.rooms.values():
            room.current_occupants.clear()

        for agent_id, agent in self.agents.items():
            base = self._baseline_agents[agent_id]
            target_room = self.rooms[base["room_id"]]
            target_room.current_occupants.append(agent_id)

            agent.status = base["status"]
            agent.current_task = base["task"]
            agent.active_tool = base["tool"]
            agent.memory_context = base["context"]
            agent.updated_at = datetime.now(timezone.utc).isoformat()

        # Reposition all rooms
        for room_id in self.rooms:
            self._reposition_room_occupants(room_id)

        self.add_activity(
            agent_id="raziel",
            action="DEEP_WORK_RESUMED",
            details="All personnel dispersed to departmental workstations. Deep focus mode engaged.",
            severity="INFO",
            room_id="room-ceo",
        )

        return self.get_state()

    def trigger_sleep_cycle(self) -> OfficeStateResponse:
        """Trigger headquarters-wide rest and regeneration cycle."""
        self._office_mode = OfficeMode.REST_CYCLE
        rest_room = self.rooms["room-pods"]

        for room in self.rooms.values():
            room.current_occupants.clear()

        for agent_id, agent in self.agents.items():
            rest_room.current_occupants.append(agent_id)
            if agent_id == "viktor":
                agent.status = AgentStatus.STANDBY
                agent.current_task = "Low-power Perimeter Daemon & Cryptographic Guard"
            elif agent_id == "jovan":
                agent.status = AgentStatus.STANDBY
                agent.current_task = "Passive Night Radar & Beacon Listener"
            else:
                agent.status = AgentStatus.RESTING
                agent.current_task = "Sensory Deprivation Pod Rest & Bio-Rhythm Recharging"

            agent.updated_at = datetime.now(timezone.utc).isoformat()

        self._reposition_room_occupants("room-pods")

        self.add_activity(
            agent_id="elara",
            action="REST_CYCLE_TRIGGERED",
            details="Headquarters rest cycle engaged. Ambient lighting dimmed to bioluminescent indigo.",
            severity="SYSTEM",
            room_id="room-pods",
        )

        return self.get_state()

    def trigger_recreation(self) -> OfficeStateResponse:
        """Trigger headquarters-wide break and recreation session.

        Sets office mode to RECREATION, assigns agents to recreational activities:
        - Idris & Mika: ping-pong table match in Lounge
        - Dani & Raziel: executive lounge sofa discussing vision
        - Elara: pantry coffee bar serving refreshments
        - Senna: lounge armchair sketching
        - Jovan: pantry snacks
        - Kael & Nara: research library discussion
        - Viktor: checking coffee machine / casual chat
        """
        self._office_mode = OfficeMode.RECREATION

        # Clear occupants across all rooms
        for room in self.rooms.values():
            room.current_occupants.clear()

        recreation_assignments = {
            "idris": {
                "room": "room-concierge",
                "task": "Ping-pong table match in Lounge",
                "tool": "Ping-Pong Paddle",
            },
            "mika": {
                "room": "room-concierge",
                "task": "Ping-pong table match in Lounge",
                "tool": "Ping-Pong Paddle",
            },
            "dani": {
                "room": "room-ceo",
                "task": "Executive lounge sofa discussing vision",
                "tool": "Vision Roadmap",
            },
            "raziel": {
                "room": "room-ceo",
                "task": "Executive lounge sofa discussing vision",
                "tool": "Vision Roadmap",
            },
            "daffa": {
                "room": "room-ceo",
                "task": "Executive lounge sofa discussing vision & strategy",
                "tool": "Executive iPad",
            },
            "elara": {
                "room": "room-concierge",
                "task": "Pantry coffee bar serving refreshments",
                "tool": "Italian Espresso Bar",
            },
            "senna": {
                "room": "room-concierge",
                "task": "Lounge armchair sketching",
                "tool": "Digital Sketchpad",
            },
            "jovan": {
                "room": "room-concierge",
                "task": "Pantry snacks",
                "tool": "Snack Inventory",
            },
            "kael": {
                "room": "room-atelier",
                "task": "Research library discussion",
                "tool": "Knowledge Archive",
            },
            "nara": {
                "room": "room-atelier",
                "task": "Research library discussion",
                "tool": "Knowledge Archive",
            },
            "viktor": {
                "room": "room-concierge",
                "task": "Checking coffee machine / casual chat",
                "tool": "Espresso Diagnostics",
            },
        }

        for agent_id, info in recreation_assignments.items():
            agent = self.agents.get(agent_id)
            if not agent:
                continue
            target_room = self.rooms[info["room"]]
            target_room.current_occupants.append(agent_id)

            agent.status = AgentStatus.RESTING
            agent.current_task = info["task"]
            agent.active_tool = info["tool"]
            agent.updated_at = datetime.now(timezone.utc).isoformat()

        # Reposition all occupants in rooms
        for room_id in self.rooms:
            self._reposition_room_occupants(room_id)

        self.add_activity(
            agent_id="dani",
            action="RECREATION_TRIGGERED",
            details="CEO triggered studio break & recreation session (ping-pong & lounge active)",
            severity="INFO",
            room_id="room-concierge",
        )

        return self.get_state()

    # --- Live SSE Subscription Management ---

    def subscribe(self) -> asyncio.Queue[str]:
        """Register a new SSE stream client."""
        queue: asyncio.Queue[str] = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        """Deregister an SSE stream client."""
        self._subscribers.discard(queue)

    def _broadcast_state(self) -> None:
        """Notify all active SSE listeners of a state modification."""
        if not self._subscribers:
            return
        state_json = self.get_state().model_dump_json()
        for q in list(self._subscribers):
            try:
                q.put_nowait(state_json)
            except asyncio.QueueFull:
                pass

    # --- Background Simulation Engine ---

    async def start_simulation(self) -> None:
        """Start the background autonomous simulation worker."""
        if self._is_running:
            return
        self._is_running = True
        self._simulation_task = asyncio.create_task(self._simulation_loop())

    async def stop_simulation(self) -> None:
        """Stop the background autonomous simulation worker gracefully."""
        self._is_running = False
        if self._simulation_task and not self._simulation_task.done():
            self._simulation_task.cancel()
            try:
                await self._simulation_task
            except asyncio.CancelledError:
                pass
        self._simulation_task = None

    async def _simulation_loop(self) -> None:
        """Autonomous background loop: fluctuates telemetry and produces ambient events."""
        autonomous_events = [
            (
                "viktor",
                "SECURITY_PROBE",
                "Viktor Moreau completed 4-layer security probe on port 9449: 0 vulnerabilities found.",
                "INFO",
            ),
            (
                "nara",
                "CITATIONS_INDEXED",
                "Nara Vasquez indexed 3 new arXiv papers on distributed multi-agent consensus protocols.",
                "INFO",
            ),
            (
                "idris",
                "SSE_POOL_OPTIMIZED",
                "Idris Nakamura optimized event broadcasting pool, dropping client latency to 1.2ms.",
                "INFO",
            ),
            (
                "mika",
                "SHADERS_COMPILED",
                "Mika Stellan compiled 2.5D cyber-luxury glass shaders for isometric canvas rendering.",
                "INFO",
            ),
            (
                "senna",
                "DESIGN_TOKENS_PUBLISHED",
                "Senna Louviere updated gold-mesh visual tokens in Figma component library.",
                "INFO",
            ),
            (
                "raziel",
                "HEARTBEAT_ACKNOWLEDGED",
                "Raziel Hendrix verified telemetry heartbeats across all 11 worker subagents.",
                "INFO",
            ),
            (
                "daffa",
                "EXECUTIVE_ALIGNMENT",
                "Daffa synchronized operational task queues between CEO Office and departmental leads.",
                "INFO",
            ),
            (
                "jovan",
                "CAMPUS_RADAR_PING",
                "Jovan Aritza detected Telkom University research symposium announcement.",
                "INFO",
            ),
            (
                "elara",
                "LOGISTICS_DISPATCH",
                "Elara Sinclair completed afternoon refreshment distribution & schedule alignment.",
                "INFO",
            ),
            (
                "kael",
                "ARCHITECTURE_SPEC_SYNC",
                "Kael Ashford committed system topology diagram for distributed office state engine.",
                "INFO",
            ),
            (
                "dani",
                "ROADMAP_MILESTONE",
                "Daniandra Prayudisty approved Q4 studio milestone for autonomous workspace.",
                "INFO",
            ),
        ]

        while self._is_running:
            try:
                await asyncio.sleep(self.settings.simulation_interval)
                self._sim_ticks += 1

                # Fluctuating light telemetry footprint
                for agent in self.agents.values():
                    if agent.status in (AgentStatus.WORKING, AgentStatus.RESEARCHING):
                        agent.cpu_footprint = round(
                            max(10.0, min(95.0, agent.cpu_footprint + random.uniform(-4.0, 4.0))),
                            1,
                        )
                        agent.ram_footprint = round(
                            max(100.0, min(550.0, agent.ram_footprint + random.uniform(-6.0, 6.0))),
                            1,
                        )
                    elif agent.status == AgentStatus.MEETING:
                        agent.cpu_footprint = round(
                            max(30.0, min(75.0, agent.cpu_footprint + random.uniform(-2.0, 3.0))),
                            1,
                        )
                    else:  # RESTING, SLEEPING, STANDBY
                        agent.cpu_footprint = round(
                            max(2.0, min(15.0, agent.cpu_footprint + random.uniform(-1.0, 1.0))),
                            1,
                        )
                        agent.ram_footprint = round(
                            max(80.0, min(180.0, agent.ram_footprint + random.uniform(-2.0, 2.0))),
                            1,
                        )

                # Periodic ambient micro-action (every ~3 ticks)
                if self._sim_ticks % 3 == 0 and self._office_mode == OfficeMode.NORMAL:
                    agent_id, action, details, sev = random.choice(autonomous_events)
                    self.add_activity(agent_id=agent_id, action=action, details=details, severity=sev)
                else:
                    self._broadcast_state()

            except asyncio.CancelledError:
                break
            except Exception:
                # Keep background simulation resilient
                pass


# Global singleton instance
office_engine = OfficeEngine()
