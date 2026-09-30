"""Spatial simulation engine and real-time state machine for Yudiaz Virtual HQ.

Manages 11 autonomous agents across 11 cyber-luxury zones, coordinates spatial
positioning, mode switches (War Room, Deep Work, Rest Cycle), audit logging,
and live telemetry streaming.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import random
import re
import secrets
import sqlite3
import time
from typing import Any, Optional
import uuid
import httpx

PROFILE_DB_MAP: dict[str, Path] = {
    "daffa": Path.home() / ".hermes" / "state.db",
    "raziel": Path.home() / ".hermes" / "profiles" / "raziel-cto" / "state.db",
    "elara": Path.home() / ".hermes" / "profiles" / "elara-pa" / "state.db",
    "jovan": Path.home() / ".hermes" / "profiles" / "jovan-intel" / "state.db",
    "cucurella": Path.home() / ".hermes" / "profiles" / "cucurella-soetahills" / "state.db",
    "kael": Path.home() / ".hermes" / "profiles" / "kael-architect" / "state.db",
    "nara": Path.home() / ".hermes" / "profiles" / "nara-researcher" / "state.db",
    "senna": Path.home() / ".hermes" / "profiles" / "senna-designer" / "state.db",
    "idris": Path.home() / ".hermes" / "profiles" / "idris-developer" / "state.db",
    "mika": Path.home() / ".hermes" / "profiles" / "mika-frontend" / "state.db",
    "viktor": Path.home() / ".hermes" / "profiles" / "viktor-qa" / "state.db",
}

def clean_user_prompt(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\[OUT-OF-BAND USER MESSAGE.*?\]', '', text, flags=re.DOTALL)
    text = re.sub(r'\[/OUT-OF-BAND USER MESSAGE\]', '', text, flags=re.DOTALL)
    text = re.sub(r'Gateway message origin.*?\n\n', '', text, flags=re.DOTALL)
    text = re.sub(r'\[IMPORTANT:.*?\]', '', text, flags=re.DOTALL)
    text = re.sub(r'\[Note:.*?\]', '', text, flags=re.DOTALL)
    return text.strip()

def get_agent_live_telegram_data(agent_id: str) -> Optional[dict[str, Any]]:
    db_path = PROFILE_DB_MAP.get(agent_id)
    if not db_path or not db_path.exists():
        return None
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
        cur = con.cursor()
        asst = cur.execute(
            """
            SELECT timestamp, content, reasoning, reasoning_content, tool_name
            FROM messages
            WHERE role = 'assistant'
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
        user_msg = cur.execute(
            """
            SELECT content, timestamp
            FROM messages
            WHERE role = 'user' AND content IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
        con.close()
        if not asst:
            return None
        ts, content, reasoning, r_content, tool_name = asst
        thought = (reasoning or r_content or "").strip()
        if not thought and content:
            first_line = content.strip().split("\n")[0]
            if len(first_line) > 180:
                first_line = first_line[:177] + "..."
            thought = first_line

        user_task = clean_user_prompt(user_msg[0]) if user_msg else ""
        if len(user_task) > 120:
            user_task = user_task[:117] + "..."

        is_recent = (time.time() - ts) < 86400

        return {
            "timestamp": ts,
            "thought": thought,
            "task": user_task,
            "tool": tool_name or "Telegram Gateway",
            "reply": (content[:160] + "...") if content and len(content) > 160 else (content or ""),
            "is_recent": is_recent,
        }
    except Exception:
        return None

WIB = timezone(timedelta(hours=7))

def get_wib_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(WIB)

from backend.config import get_settings
from backend.models import (
    ActionItem,
    ActivityLog,
    AgentInfo,
    AgentPosition,
    AgentStatus,
    CEOCommandResponse,
    DialogueTurn,
    MeetingDialogue,
    MeetingMinutes,
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

        # Autonomous simulation lifecycle state
        self._temporary_assignments: dict[str, dict[str, Any]] = {}
        self._council_active: bool = False
        self._need_cycle_index: int = 0
        self.latest_meeting: Optional[MeetingMinutes] = None
        self.meetings_history: list[MeetingMinutes] = []
        self.active_event: Optional[dict[str, Any]] = None

        self._initialize_rooms()
        self._initialize_agents()
        self._seed_initial_activity()
        self._seed_initial_meetings()
        self.conversation_history: list[dict[str, Any]] = []
        self._seed_initial_conversations()

    def _archive_meeting(self, meeting: MeetingMinutes) -> None:
        """Archive or update meeting in history, deduplicating by meeting_id, newest first, max 20."""
        self.meetings_history = [m for m in self.meetings_history if m.meeting_id != meeting.meeting_id]
        self.meetings_history.insert(0, meeting.model_copy(deep=True))
        if len(self.meetings_history) > 20:
            self.meetings_history = self.meetings_history[:20]

    def _seed_initial_meetings(self) -> None:
        """Seed historical archive of council sessions and set the latest meeting."""
        for idx, offset in [(2, 7200), (1, 3600), (0, 0)]:
            meeting = self._generate_council_meeting(
                leader_id="daffa",
                leader_name="Daffa (CEO Office)",
                status="COMPLETED",
                need_index=idx,
                time_offset=offset,
            )
            self._archive_meeting(meeting)
            if idx == 0:
                self.latest_meeting = meeting

    def _initialize_rooms(self) -> None:
        """Seed the 11 distinct physical architectural zones matching the 3D diorama building layout."""
        room_configs = [
            {
                "id": "room-ceo",
                "name": "CEO Suite",
                "category": "Executive",
                "capacity": 6,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (520.0, 340.0),
                "description": "CEO Suite - Dedicated executive chamber for Daniandra Prayudisty (Founder & CEO) and Daffa (CEO Office), strategic vision, and studio governance.",
                "status_accent": "#10B981",  # Cyber Emerald
            },
            {
                "id": "room-cto",
                "name": "CTO Executive Suite",
                "category": "Engineering Leadership",
                "capacity": 4,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (520.0, 1850.0),
                "description": "CTO Executive Suite - Dedicated engineering leadership chamber for Raziel Hendrix, high-level technical architecture, multi-agent mesh orchestration, and cloud infrastructure.",
                "status_accent": "#7928CA",  # Cyber Purple
            },
            {
                "id": "room-pa",
                "name": "Executive Assistant Office",
                "category": "Executive Support",
                "capacity": 4,
                "floor": 1,
                "dimensions": (540.0, 290.0),
                "center_coord": (1300.0, 1850.0),
                "description": "Executive Assistant Office - Dedicated office for Elara Sinclair (Personal Assistant to CEO), executive calendar dispatch, briefing prep, and priority logistics.",
                "status_accent": "#E0AAFF",  # Soft Lavender
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
                "context": "Mengevaluasi kesiapan ekosistem studio: dev pipeline Raziel, radar kampus Jovan, dan kampanye Soetahills",
            },
            {
                "id": "raziel",
                "name": "Raziel Hendrix",
                "role": "CTO & Lead Orchestrator",
                "department": "Engineering Leadership",
                "room_id": "room-cto",
                "status": AgentStatus.WORKING,
                "task": "System Orchestration & Cloud Infrastructure",
                "avatar_color": "#7928CA",
                "tool": "Hermes Agent Hub",
                "context": "Barusan review PR Idris & Mika, latency stream 0.4ms aman dan arsitektur modular siap nampung beban multi-agent",
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
                "context": "Lagi kalkulasi beban database event-driven loop, circular dependency berhasil dicegah dan Tectonic compile bersih",
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
                "context": "Data benchmark terbaru membuktikan arsitektur event-driven 4.2x lebih efisien, 14 paper Scopus siap untuk novelti skripsi",
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
                "context": "Lagi poles ritme spasi dan balance visual hero section, impresi obsidian premium makin tajam dan kontras tombol tegas",
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
                "context": "Query join nested yang tadinya 400ms berhasil saya pangkas jadi 45ms, guard clause auth dari temuan Viktor udah rapi",
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
                "context": "Glitch drawer navigation mobile Safari udah beres, FPS Three.js locked 60 FPS waktu event SSE masuk bersamaan",
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
                "context": "Unhandled 500 error pas payload kosong udah beres ditambal Idris, semua 35 test suite hijau dan build layak naik staging",
            },
            {
                "id": "elara",
                "name": "Elara Sinclair",
                "role": "Personal Assistant to CEO",
                "department": "Executive Support",
                "room_id": "room-pa",
                "status": AgentStatus.WORKING,
                "task": "Executive Calendar, Briefing Prep & Priority Logistics",
                "avatar_color": "#E0AAFF",
                "tool": "Executive Calendar & Briefing Suite",
                "context": "Menjaga ritme kerja Mas Dani tetap prima, briefing kalender dan rekap kas rapi, siap kawal evaluasi malam",
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
                "context": "Jadwal revisi sidang fakultas terverifikasi dari menfess dan anak BEM, seminar event-driven 15 Okt klop sama skripsi Mas Dani",
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
                "context": "Menyelaraskan sprint dev Raziel, radar Jovan, dan traksi Soetahills Cucurella, memo briefing siap untuk meja Mas Dani",
            },
            {
                "id": "cucurella",
                "name": "Cucurella",
                "role": "Head of Soetahills Growth",
                "department": "Real Estate & Strategic Growth",
                "room_id": "room-ceo",
                "status": AgentStatus.WORKING,
                "task": "Soetahills Property Market Intel & Content Strategy",
                "avatar_color": "#10B981",
                "tool": "Meta Creator Studio & Real Estate Analytics",
                "context": "Menganalisis performa hook video Reels @soetahills, riset harga kompetitor properti sekitar, dan mengonversi leads WhatsApp survei lokasi",
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
            details="Yudiaz Virtual HQ Spatial Engine booted. 12 autonomous agents deployed across 11 zones.",
            severity="SYSTEM",
        )
        self.add_activity(
            agent_id="dani",
            action="OFFICE_INSPECTION",
            details="Daniandra Prayudisty initiated daily studio oversight from the Executive Suite.",
            severity="INFO",
        )

    def sync_live_telegram_telemetry(self) -> None:
        """Synchronizes live thought reasoning and active tasks from SQLite state.db."""
        for aid, agent in self.agents.items():
            live = get_agent_live_telegram_data(aid)
            if live and live.get("thought"):
                agent.telegram_live = live
                agent.memory_context = live["thought"]
                if live.get("tool"):
                    agent.active_tool = live["tool"]

    def _generate_council_meeting(
        self,
        leader_id: str = "daffa",
        leader_name: Optional[str] = None,
        status: str = "IN_PROGRESS",
        need_index: Optional[int] = 0,
        time_offset: int = 0,
    ) -> MeetingMinutes:
        """Construct realistic, rich Minutes of Meeting (MoM) record for War Room council.

        Council meetings are ALWAYS led by Daffa (CEO Office). Daniandra (CEO) does not attend
        the operational War Room and stays in the CEO Suite reviewing strategic vision.
        Agendas analyze studio business opportunities and technical needs with live studio telemetry.
        """
        ts = time.time() - time_offset
        now_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        meeting_id = f"mom-council-{int(ts)}-{secrets.token_hex(3)}"
        resolved_leader = leader_name or "Daffa (CEO Office)"

        attendees = [
            "Daffa (CEO Office)",
            "Raziel Hendrix (CTO & Lead Orchestrator)",
            "Kael Ashford (Lead Architect)",
            "Nara Vasquez (Lead Researcher)",
            "Senna Louviere (Creative Director)",
            "Idris Nakamura (Senior Developer)",
            "Mika Stellan (Frontend Engineer)",
            "Viktor Moreau (Lead QA & Security Engineer)",
            "Elara Sinclair (Personal Assistant to CEO)",
            "Jovan Aritza (Intelligence Officer)",
            "Cucurella (Head of Soetahills Growth)",
        ]

        active_need_idx = (need_index or 0) % 3

        if active_need_idx == 0:
            # Topic 1: Business Suggestion / Micro-SaaS AI Automation via Dynamic QRIS & 9Router
            title = "Analisis Strategis: Monetisasi Micro-SaaS AI & Dynamic QRIS Payment Gateway"
            dialogues = [
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Selamat pagi rekan-rekan. Sesuai mandat CEO Daniandra, saya memimpin War Room council hari ini. Fokus utama kita: Analisis Strategis Monetisasi Micro-SaaS AI Automation via Dynamic QRIS (Tripay/Mayar) dan token provisioning API 9Router. Raziel, bagaimana kesiapan arsitektur teknis dan proyeksi margin inferensinya?",
                ),
                MeetingDialogue(
                    speaker_id="raziel",
                    speaker_name="Raziel Hendrix",
                    role="CTO & Lead Orchestrator",
                    text="Secara teknis, integrasi API 9Router pada internal subdomain (api:20128) sudah benchmarked sangat stabil. Dengan server Intel Xeon 4 vCPU dan 54 GB ECC RAM kita, latency routing multi-model berada di bawah 150ms. Skema paket Rp 35.000/paket memberikan margin kotor di atas 60% setelah biaya token provider.",
                ),
                MeetingDialogue(
                    speaker_id="kael",
                    speaker_name="Kael Ashford",
                    role="Lead Architect",
                    text="Arsitektur pembayaran dirancang event-driven dan asinkron: webhook push payment dynamic QRIS dari gateway Tripay/Mayar diverifikasi dengan signature HMAC-SHA256, lalu secara atomik memicu token quota provisioning di 9Router. Model push payment ini menjamin zero chargeback dan settlement langsung ke buku kas Finance (finance:9339 - saat ini Rp 0 clean ledger).",
                ),
                MeetingDialogue(
                    speaker_id="nara",
                    speaker_name="Nara Vasquez",
                    role="Lead Researcher",
                    text="Riset pasar UMKM digital Indonesia menunjukkan 78% pelaku usaha memerlukan automasi AI untuk customer support WhatsApp dan billing, namun enggan berlangganan kartu kredit berulang. Skema dynamic QRIS mikro Rp 35.000 per paket adalah sweet spot penetrasi pasar dengan friksi adopsi terendah.",
                ),
                MeetingDialogue(
                    speaker_id="idris",
                    speaker_name="Idris Nakamura",
                    role="Senior Developer",
                    text="Backend FastAPI di port 9449 siap merilis router `/api/v1/billing/qris-webhook` dengan verifikasi HMAC-SHA256, idempotency key berbasis memory/Redis, dan payload sanitization ketat. Implementasi tuntas dalam 1 sprint dengan unit test 100% pass.",
                ),
                MeetingDialogue(
                    speaker_id="mika",
                    speaker_name="Mika Stellan",
                    role="Frontend Engineer",
                    text="Di frontend canvas dan portal merchant Next.js, kami telah menyiapkan widget checkout Dynamic QRIS cyber-luxury dengan QR code interaktif, countdown expired 15 menit, dan notifikasi status real-time via WebSocket/SSE.",
                ),
                MeetingDialogue(
                    speaker_id="senna",
                    speaker_name="Senna Louviere",
                    role="Creative Director",
                    text="Kami mengemas antarmuka produk Micro-SaaS ini dengan identitas cyber-luxury: gold-mesh trims, aksen Emerald QRIS (#10B981), dan kartu paket monokrom elegan yang menumbuhkan rasa percaya tinggi bagi merchant.",
                ),
                MeetingDialogue(
                    speaker_id="viktor",
                    speaker_name="Viktor Moreau",
                    role="Lead QA & Security Engineer",
                    text="Sentinel 4-layer defense akan memproteksi gateway pembayaran. Webhook callback diproteksi IP whitelisting Tripay/Mayar di UFW firewall dan anti-replay guard. Kami juga mensimulasikan uji penetrasi injection dan brute-force token.",
                ),
                MeetingDialogue(
                    speaker_id="jovan",
                    speaker_name="Jovan Aritza",
                    role="Intelligence Officer",
                    text="Radar intelijen kompetitor: SaaS otomasi luar negeri mengenakan subscription $29/bulan yang terlalu mahal bagi developer dan UMKM lokal. Paket pay-as-you-go Rp 35k via QRIS lokal dengan zero chargeback adalah killer feature untuk akuisisi cepat 500 merchant pertama.",
                ),
                MeetingDialogue(
                    speaker_id="elara",
                    speaker_name="Elara Sinclair",
                    role="Personal Assistant to CEO",
                    text="Seluruh data kalkulasi unit economics Rp 35k/paket, estimasi settlement ke clean ledger Finance (finance:9339), dan ringkasan arsitektur telah saya rangkum dalam notulensi resmi. Dokumen siap diserahkan kepada Daffa untuk dilaporkan kepada CEO Daniandra di CEO Suite.",
                ),
                MeetingDialogue(
                    speaker_id="cucurella",
                    speaker_name="Cucurella",
                    role="Head of Soetahills Growth",
                    text="Dari perspektif growth dan cross-monetization, modul dynamic QRIS ini juga bisa diintegrasikan langsung ke booking fee kilat unit properti Soetahills. Audiens Instagram @soetahills yang terkonversi dari konten Reels edukasi bisa langsung lock unit tanpa hambatan administrasi perbankan manual.",
                ),
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Terima kasih atas kontribusi tajam seluruh tim. Keputusan diambil: kita eksekusi prototipe Micro-SaaS AI Automation via Dynamic QRIS dan 9Router. Council resmi ditutup, saya segera menuju CEO Suite untuk menyerahkan Executive Briefing ini kepada CEO Daniandra.",
                ),
            ]
            key_decisions = [
                "Pengembangan produk Micro-SaaS AI Automation studio terintegrasi Dynamic QRIS (Tripay/Mayar) dan token provisioning API 9Router (api:20128) dengan efisiensi biaya inferensi hingga 60%.",
                "Penerapan model bisnis pay-as-you-go berbasis paket Rp 35.000 dengan zero chargeback push payment dan validasi webhook HMAC-SHA256.",
                "Pencatatan mutasi transaksi otomatis tersinkronisasi ke clean ledger Finance studio (finance:9339 - Rp 0 clean ledger).",
                "Pemberian laporan rekomendasi monetisasi resmi oleh Daffa kepada CEO Daniandra Prayudisty di CEO Suite.",
            ]
            action_items = [
                ActionItem(
                    pic="Idris Nakamura & Kael Ashford",
                    task="Implementasi backend webhook HMAC-SHA256 QRIS dan token provisioning 9Router",
                    due="2026-10-02",
                ),
                ActionItem(
                    pic="Nara Vasquez & Jovan Aritza",
                    task="Riset komparasi fee gateway Tripay vs Mayar dan pemetaan 100 calon merchant UMKM",
                    due="2026-10-01",
                ),
                ActionItem(
                    pic="Mika Stellan & Senna Louviere",
                    task="Desain visual landing page cyber-luxury dan widget dynamic QRIS checkout",
                    due="2026-10-03",
                ),
                ActionItem(
                    pic="Viktor Moreau",
                    task="Audit Sentinel 4-layer defense pada webhook payment dan uji penetrasi replay attack",
                    due="2026-10-04",
                ),
                ActionItem(
                    pic="Daffa",
                    task="Penyampaian Executive Briefing & Rekomendasi Bisnis kepada CEO Daniandra di CEO Suite",
                    due="2026-09-28",
                ),
            ]
        elif active_need_idx == 1:
            # Topic 2: R&D / Thesis Telkom University Bab 3 & 4
            title = "Evaluasi R&D: Sinkronisasi LaTeX Bab 3-4 Skripsi Telkom University via Tectonic"
            dialogues = [
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Selamat pagi rekan-rekan. Sesuai mandat CEO Daniandra, saya memimpin council untuk evaluasi R&D skripsi S1 Informatika Telkom University: 'Sistem Observabilitas & Telemetri Spasial Multi-Agent Otonom Berbasis Event-Driven Architecture'. Kael dan Nara, laporkan status sinkronisasi draf Bab 3 Metodologi dan Bab 4 Hasil & Pembahasan.",
                ),
                MeetingDialogue(
                    speaker_id="kael",
                    speaker_name="Kael Ashford",
                    role="Lead Architect",
                    text="Struktur repositori skripsi telah disusun modular Bab 1 hingga 5. Pipeline kompilasi menggunakan engine Tectonic LaTeX di `/usr/local/bin/tectonic` berhasil melakukan typesetting naskah secara reproducible tanpa error dependensi. Diagram state machine, sequence message mesh, dan spesifikasi formal event loop telah selesai disinkronkan.",
                ),
                MeetingDialogue(
                    speaker_id="raziel",
                    speaker_name="Raziel Hendrix",
                    role="CTO & Lead Orchestrator",
                    text="Infrastruktur Intel Xeon 4 vCPU, 54 GB ECC RAM, dan NVMe storage kita memberikan data empiris yang sangat kredibel untuk Bab 4. Metrik latensi telemetri sub-10ms pada load test 50 subagent membuktikan efisiensi arsitektur asynchronous event-driven kita dibandingkan model polling tradisional.",
                ),
                MeetingDialogue(
                    speaker_id="nara",
                    speaker_name="Nara Vasquez",
                    role="Lead Researcher",
                    text="Bab 2 Tinjauan Pustaka dan Bab 3 Metodologi telah kami lengkapi dengan 35 sitasi primer IEEE/ACM dan ScienceDirect terbaru. Novelti koordinasi spatial 2.5D dan broadcast state engine kita telah teruji secara komparatif dengan literatur autonomous agent kontemporer.",
                ),
                MeetingDialogue(
                    speaker_id="idris",
                    speaker_name="Idris Nakamura",
                    role="Senior Developer",
                    text="Dataset empiris dari server log backend FastAPI dan metrik footprint memori telah diekspor ke format CSV terstruktur. Skrip Python otomatis menghasilkan visualisasi grafik latensi beresolusi 600 DPI yang langsung di-include ke naskah LaTeX Bab 4.",
                ),
                MeetingDialogue(
                    speaker_id="mika",
                    speaker_name="Mika Stellan",
                    role="Frontend Engineer",
                    text="Visualisasi denah arsitektur 3D building diorama, koordinat spasial zona, dan layout canvas telah diekspor ke format vektor resolusi tinggi sesuai standar gambar dokumen akademik Telkom University.",
                ),
                MeetingDialogue(
                    speaker_id="senna",
                    speaker_name="Senna Louviere",
                    role="Creative Director",
                    text="Layout tipografi LaTeX, margin 4-4-3-3 Tel-U, penomoran tabel naskah, serta skema visual monokrom akademik telah kami standarkan sesuai panduan penulisan tugas akhir Fakultas Informatika Telkom University.",
                ),
                MeetingDialogue(
                    speaker_id="viktor",
                    speaker_name="Viktor Moreau",
                    role="Lead QA & Security Engineer",
                    text="Validasi eksperimen diuji berulang kali melalui test harness pytest untuk memastikan integritas data Bab 4. Latensi p99 terbukti stabil di bawah 4.8ms tanpa fluktuasi bias.",
                ),
                MeetingDialogue(
                    speaker_id="jovan",
                    speaker_name="Jovan Aritza",
                    role="Intelligence Officer",
                    text="Radar NOC kampus Tel-U mengonfirmasi jadwal verifikasi berkas dan pendaftaran pra-sidang skripsi semester ini sudah resmi dibuka. Progress kompilasi Bab 1-4 kita berada dalam jadwal akselerasi yang sangat aman.",
                ),
                MeetingDialogue(
                    speaker_id="elara",
                    speaker_name="Elara Sinclair",
                    role="Personal Assistant to CEO",
                    text="Berkas PDF skripsi hasil kompilasi Tectonic Bab 1-4 dan lembar novelti penelitian telah selesai disiapkan di tablet eksekutif. Dokumen siap diserahkan kepada Daffa untuk dilaporkan ke CEO Daniandra.",
                ),
                MeetingDialogue(
                    speaker_id="cucurella",
                    speaker_name="Cucurella",
                    role="Head of Soetahills Growth",
                    text="Metodologi evaluasi sistem terdistribusi multi-agent dalam skripsi Mas Dani ini punya relevansi nyata untuk pemodelan analitik data spasial pasar properti dan cluster demografi pembeli hunian modern.",
                ),
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Apresiasi tinggi atas kerja keras seluruh tim R&D dan arsitektur. Rapat council resmi ditutup. Saya segera menuju CEO Suite untuk melaporkan hasil evaluasi skripsi ini kepada CEO Daniandra.",
                ),
            ]
            key_decisions = [
                "Standardisasi kompilasi naskah modular skripsi S1 Telkom University menggunakan compiler Tectonic LaTeX di `/usr/local/bin/tectonic`.",
                "Pemanfaatan data empiris latensi sub-10ms dan utilisasi server Intel Xeon 54 GB ECC RAM Virtual HQ sebagai novelti Bab 4.",
                "Sinkronisasi berkala timeline akademik dan pendaftaran sidang pra-skripsi Tel-U melalui koordinasi Radar NOC dan CEO Office.",
                "Penyampaian draf PDF skripsi hasil kompilasi Tectonic kepada CEO Daniandra Prayudisty di CEO Suite.",
            ]
            action_items = [
                ActionItem(
                    pic="Kael Ashford & Nara Vasquez",
                    task="Finalisasi draf Bab 3 & Bab 4 Skripsi Telkom University via compiler Tectonic LaTeX",
                    due="2026-10-02",
                ),
                ActionItem(
                    pic="Idris Nakamura",
                    task="Ekspor dataset pengujian telemetry engine dan grafik latensi empiris untuk Bab 4",
                    due="2026-09-30",
                ),
                ActionItem(
                    pic="Mika Stellan & Senna Louviere",
                    task="Finalisasi diagram arsitektur vektor resolusi tinggi sesuai panduan naskah Tel-U",
                    due="2026-09-29",
                ),
                ActionItem(
                    pic="Jovan Aritza & Elara Sinclair",
                    task="Monitoring portal akademik Tel-U terkait jadwal pendaftaran sidang pra-skripsi",
                    due="2026-10-01",
                ),
                ActionItem(
                    pic="Daffa",
                    task="Penyampaian laporan kemajuan evaluasi skripsi kepada CEO Daniandra di Executive Suite",
                    due="2026-09-28",
                ),
            ]
        elif active_need_idx == 2:
            # Topic 3: DevOps / Security Sentinel & Server Observability
            title = "Audit Keamanan & Keandalan Cluster Sentinel: 4-Layer Server Hardening"
            dialogues = [
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Selamat pagi rekan-rekan. Sesuai arahan CEO Daniandra, saya memimpin council hari ini untuk Audit Keamanan & Keandalan Cluster Sentinel: 4-Layer Server Hardening. Viktor dan Raziel, silakan paparkan status ketahanan perimeter server kita.",
                ),
                MeetingDialogue(
                    speaker_id="viktor",
                    speaker_name="Viktor Moreau",
                    role="Lead QA & Security Engineer",
                    text="Audit Sentinel 4-Layer Server Hardening menunjukkan hasil sangat memuaskan. Fail2ban recidive jail telah memblokir permanen IP penyerang 2.57.122.209 setelah mendeteksi 48 kali upaya penetrasi unauthorized brute-force. Seluruh port non-esensial ditutup rapat oleh UFW rate limiting.",
                ),
                MeetingDialogue(
                    speaker_id="raziel",
                    speaker_name="Raziel Hendrix",
                    role="CTO & Lead Orchestrator",
                    text="Cluster server production yang melayani 4 live subdomain: Sentinel (vps:9229), 9Router (api:20128), Finance (finance:9339 - Rp 0 clean ledger), dan Office (office:9449) beroperasi stabil pada hardware Intel Xeon 4 vCPU dan 54 GB ECC RAM dengan rata-rata beban CPU di bawah 12%.",
                ),
                MeetingDialogue(
                    speaker_id="kael",
                    speaker_name="Kael Ashford",
                    role="Lead Architect",
                    text="Arsitektur keamanan Sentinel telah kami dokumentasikan dalam Security Architecture Specification: reverse proxy TLS 1.3 termination di Caddy, isolasi memori state engine, dan pembatasan inter-process communication.",
                ),
                MeetingDialogue(
                    speaker_id="idris",
                    speaker_name="Idris Nakamura",
                    role="Senior Developer",
                    text="Backend FastAPI di port 9449 telah dilengkapi middleware security headers, rate limiting in-memory per IP, dan sanitasi audit log untuk mencegah kebocoran kredensial atau memory footprint berlebih.",
                ),
                MeetingDialogue(
                    speaker_id="nara",
                    speaker_name="Nara Vasquez",
                    role="Lead Researcher",
                    text="Evaluasi kepatuhan infrastruktur terhadap CIS Linux Benchmark dan panduan OWASP API Security Top 10 menghasilkan skor 98.4%, mengonfirmasi kesiapan studio menghadapi audit enterprise.",
                ),
                MeetingDialogue(
                    speaker_id="mika",
                    speaker_name="Mika Stellan",
                    role="Frontend Engineer",
                    text="Dashboard diorama pada frontend kini menyertakan indikator status live cluster subdomain Sentinel, 9Router, Finance, dan Office, dengan notifikasi visual instan jika terjadi latensi spike.",
                ),
                MeetingDialogue(
                    speaker_id="senna",
                    speaker_name="Senna Louviere",
                    role="Creative Director",
                    text="Visual styling panel observabilitas dan badge status keamanan Sentinel telah diselaraskan dengan estetika cyber-luxury: palet obsidian dark glass dan indikator Crimson Vault untuk ancaman terblokir.",
                ),
                MeetingDialogue(
                    speaker_id="jovan",
                    speaker_name="Jovan Aritza",
                    role="Intelligence Officer",
                    text="Monitoring Radar NOC mengonfirmasi IP 2.57.122.209 dan subnet penyerang telah terisolasi penuh. Trafik inbound ke server saat ini 100% legitimate dan tidak ada anomali terdeteksi.",
                ),
                MeetingDialogue(
                    speaker_id="elara",
                    speaker_name="Elara Sinclair",
                    role="Personal Assistant to CEO",
                    text="Ringkasan eksekutif hasil audit keamanan Sentinel, status stabilitas cluster 4 subdomain, dan metrik hardware telah dirangkum dalam one-page executive memo. Dokumen siap diserahkan kepada Daffa.",
                ),
                MeetingDialogue(
                    speaker_id="cucurella",
                    speaker_name="Cucurella",
                    role="Head of Soetahills Growth",
                    text="Proteksi data privasi leads dan berkas pembeli perumahan Soetahills (KTP, slip gaji KPR, invoice booking fee) mutlak terlindungi di bawah pertahanan Sentinel ini.",
                ),
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Infrastruktur yang kokoh dan bebas kerentanan adalah jaminan mutlak bagi studio kita. Rapat council resmi ditutup. Saya segera menuju CEO Suite untuk menyerahkan laporan ini kepada CEO Daniandra.",
                ),
            ]
            key_decisions = [
                "Penegakan Sentinel 4-Layer Server Hardening dan pemblokiran permanen IP 2.57.122.209 pada Fail2ban recidive jail.",
                "Pengawasan berkelanjutan terhadap 4 live subdomain cluster: Sentinel (:9229), 9Router (:20128), Finance (:9339), dan Office (:9449).",
                "Penerapan standar CIS Linux Benchmark dan enkripsi TLS 1.3 termination pada server Intel Xeon 54 GB ECC RAM.",
                "Penyerahan laporan audit keamanan dan observabilitas server oleh Daffa kepada CEO Daniandra Prayudisty di CEO Suite.",
            ]
            action_items = [
                ActionItem(
                    pic="Viktor Moreau",
                    task="Audit berkala Fail2ban recidive jail dan verifikasi port 9449 terhadap brute-force PIN",
                    due="2026-09-30",
                ),
                ActionItem(
                    pic="Raziel Hendrix & Idris Nakamura",
                    task="Monitoring performa cluster Intel Xeon 54 GB RAM dan optimasi pool broadcaster SSE",
                    due="2026-10-01",
                ),
                ActionItem(
                    pic="Kael Ashford & Nara Vasquez",
                    task="Penyusunan Security Architecture Specification dan sertifikasi CIS benchmark",
                    due="2026-10-02",
                ),
                ActionItem(
                    pic="Mika Stellan & Senna Louviere",
                    task="Penyempurnaan visual indikator kesehatan subdomain cluster di frontend diorama",
                    due="2026-10-03",
                ),
                ActionItem(
                    pic="Daffa",
                    task="Penyampaian laporan audit keamanan Sentinel dan observabilitas server kepada CEO Daniandra di CEO Suite",
                    due="2026-09-28",
                ),
            ]

        return MeetingMinutes(
            meeting_id=meeting_id,
            title=title,
            leader_name=resolved_leader,
            status=status,
            started_at=now_iso,
            attendees=attendees,
            dialogues=dialogues,
            key_decisions=key_decisions,
            action_items=action_items,
            reporting_to_ceo="Diserahkan kepada CEO Daniandra Prayudisty oleh Daffa (CEO Office)",
            ceo_feedback="Disetujui. Lanjutkan eksekusi teknis di bawah supervisi CTO Raziel Hendrix.",
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
        """Capture complete real-time snapshot of the virtual studio building diorama."""
        self.sync_live_telegram_telemetry()
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
            latest_meeting=self.latest_meeting,
            meetings_history=list(self.meetings_history),
            active_event=self.active_event,
        )

    def get_latest_meeting(self) -> Optional[MeetingMinutes]:
        """Fetch latest War Room council meeting minutes."""
        return self.latest_meeting

    def get_meetings_history(self) -> list[MeetingMinutes]:
        """Fetch chronological archive of all meeting minutes, newest first."""
        return list(self.meetings_history)

    def get_meeting_by_id(self, meeting_id: str) -> Optional[MeetingMinutes]:
        """Fetch specific meeting minutes by unique identifier."""
        for m in self.meetings_history:
            if m.meeting_id == meeting_id:
                return m
        if self.latest_meeting and self.latest_meeting.meeting_id == meeting_id:
            return self.latest_meeting
        return None


    def get_meeting_schedules(self) -> list[dict[str, Any]]:
        """Fetch scheduled studio meetings and War Room councils."""
        return [
            {
                "id": "sched-1",
                "title": "Evaluasi Sprint Dev Core & Pipeline High-Availability",
                "lead": "Daffa (CEO Office) & Raziel Hendrix (CTO)",
                "participants": ["Raziel", "Idris", "Mika", "Viktor", "Kael"],
                "day": "Senin",
                "time": "09:00 WIB",
                "room": "War Room (Grand Council)",
                "status": "UPCOMING",
                "agenda": "Review query latency Idris (45ms), canvas Three.js Mika 60 FPS, hasil audit edge-case Viktor, dan arsitektur modular Kael.",
            },
            {
                "id": "sched-2",
                "title": "Review Arsitektur Sistem & Sintesis Skripsi Tel-U",
                "lead": "Kael Ashford & Nara Vasquez",
                "participants": ["Kael", "Nara", "Raziel", "Daffa"],
                "day": "Rabu",
                "time": "14:00 WIB",
                "room": "Atelier Architecture Lab",
                "status": "UPCOMING",
                "agenda": "Validasi sequence diagram event-driven loop, kurasi 14 paper Scopus, dan benchmark arXiv 4.2x efisiensi komputasi.",
            },
            {
                "id": "sched-3",
                "title": "Audit Keamanan & Penetrasi Sentinel Cluster",
                "lead": "Viktor Moreau & Idris Nakamura",
                "participants": ["Viktor", "Idris", "Raziel"],
                "day": "Jumat",
                "time": "16:00 WIB",
                "room": "Server Fortress NOC",
                "status": "UPCOMING",
                "agenda": "Stress testing port 9449, verifikasi 35 test suite, simulasi serangan unhandled payload, dan rule hardening Fail2ban.",
            },
            {
                "id": "sched-4",
                "title": "Executive Council: Penyelarasan Strategis Studio & Soetahills Growth",
                "lead": "Daffa (CEO Office) & Cucurella",
                "participants": ["Daffa", "Cucurella", "Senna", "Elara", "Jovan"],
                "day": "Setiap Hari Kerja",
                "time": "16:30 WIB",
                "room": "War Room (Grand Council)",
                "status": "READY_TO_REPORT",
                "agenda": "Penyelarasan deliverable dev Raziel, intelijen Tel-U Jovan, performa hook Reels @soetahills, dan agenda Mas Dani.",
            },
            {
                "id": "sched-5",
                "title": "Protokol Malam, Wellness & Briefing H+1 CEO",
                "lead": "Elara Sinclair (PA to CEO)",
                "participants": ["Elara", "Daffa"],
                "day": "Setiap Malam",
                "time": "21:00 WIB",
                "room": "PA Executive Office",
                "status": "UPCOMING",
                "agenda": "Rekapitulasi keuangan Yudiaz Finance, finalisasi kalender esok, dan persiapan jadwal istirahat berkualitas Mas Dani.",
            },
        ]
    def get_daily_living_schedule(self) -> dict[str, Any]:
        """Fetch the full 24h daily living rhythm schedule of Yudiaz Creative Studio with real-time status."""
        wib_now = datetime.now(timezone(timedelta(hours=7)))
        current_time_str = wib_now.strftime("%H:%M WIB")
        cur_minute = wib_now.hour * 60 + wib_now.minute

        schedule_slots = [
            {
                "id": "slot-1",
                "time_range": "08:30 – 09:15 WIB",
                "start_min": 8 * 60 + 30,
                "end_min": 9 * 60 + 15,
                "activity": "☕ Morning Coffee & Daily Sync",
                "category": "coffee",
                "participants": "Idris, Kael, Viktor, Jovan, Daffa",
                "room": "Pantry Lounge & Bar",
                "desc": "Menyeduh espresso arabika pertama, sarapan ringan, dan review agenda harian sprint studio."
            },
            {
                "id": "slot-2",
                "time_range": "09:15 – 12:00 WIB",
                "start_min": 9 * 60 + 15,
                "end_min": 12 * 60,
                "activity": "🎯 Deep Focus Work (Sprint Pagi)",
                "category": "work",
                "participants": "Seluruh Tim (11 Personel)",
                "room": "Meja Kerja Divisi",
                "desc": "Koding core backend, perakitan Three.js UI, sintesis naskah skripsi, desain token obsidian."
            },
            {
                "id": "slot-3",
                "time_range": "12:00 – 12:45 WIB",
                "start_min": 12 * 60,
                "end_min": 12 * 60 + 45,
                "activity": "🍱 Istirahat Makan Siang & Rehat",
                "category": "relax",
                "participants": "Seluruh Karyawan (Dikoordinir Elara)",
                "room": "Atrium & Lounge",
                "desc": "Makan siang bersama, ngobrol santai lintas divisi, relaksasi setelah sesi koding intensif."
            },
            {
                "id": "slot-4",
                "time_range": "12:45 – 13:20 WIB",
                "start_min": 12 * 60 + 45,
                "end_min": 13 * 60 + 20,
                "activity": "😴 Bio-Rhythm Sleep Pods (Power Nap)",
                "category": "sleep",
                "participants": "Idris, Viktor, Kael, Nara",
                "room": "Sensory Sleep Pods",
                "desc": "Tidur regenerasi biologis 20–30 menit dalam pod bertekanan udara zen untuk reset kognitif."
            },
            {
                "id": "slot-5",
                "time_range": "13:00 – 13:30 WIB",
                "start_min": 13 * 60,
                "end_min": 13 * 60 + 30,
                "activity": "🏓 Ping-Pong Warming Up",
                "category": "pingpong",
                "participants": "Mika vs Idris, Raziel vs Viktor",
                "room": "Recreation Lounge",
                "desc": "Peregangan fisik & rally tenis meja santai agar tidak mengantuk pasca makan siang."
            },
            {
                "id": "slot-6",
                "time_range": "13:30 – 16:30 WIB",
                "start_min": 13 * 60 + 30,
                "end_min": 16 * 60 + 30,
                "activity": "🎯 Deep Focus Work (Sprint Siang)",
                "category": "work",
                "participants": "Seluruh Tim (11 Personel)",
                "room": "Meja Kerja Divisi",
                "desc": "Refactoring API, audit keamanan 51 tests Sentinel, review PR, penyelarasan kampanye Soetahills."
            },
            {
                "id": "slot-7",
                "time_range": "16:30 – 17:15 WIB",
                "start_min": 16 * 60 + 30,
                "end_min": 17 * 60 + 15,
                "activity": "☕ Afternoon Coffee & Tea Break",
                "category": "coffee",
                "participants": "Senna, Elara, Daniandra, Daffa",
                "room": "Pantry Lounge & Bar",
                "desc": "Seduh kopi sore atau teh herbal hangat untuk menjaga mood dan fokus sore hari."
            },
            {
                "id": "slot-8",
                "time_range": "16:45 – 17:45 WIB",
                "start_min": 16 * 60 + 45,
                "end_min": 17 * 60 + 45,
                "activity": "🏓 Turnamen Ping-Pong Rekreasi",
                "category": "pingpong",
                "participants": "Raziel vs Daniandra, Daffa vs Jovan, Mika vs Senna",
                "room": "Recreation Lounge",
                "desc": "Ajang rekreasi kompetitif santai pelepas stres setelah jam koding maraton."
            },
            {
                "id": "slot-9",
                "time_range": "17:00 – 17:45 WIB",
                "start_min": 17 * 60,
                "end_min": 17 * 60 + 45,
                "activity": "🛋️ Bersantai di Sofa Lounge",
                "category": "sofa",
                "participants": "Elara, Senna, Nara",
                "room": "Executive Sofa Lounge",
                "desc": "Duduk rileks mendengarkan synth studio, lurusin punggung, baca literatur dan diskusi santai."
            },
            {
                "id": "slot-10",
                "time_range": "17:45 – 18:15 WIB",
                "start_min": 17 * 60 + 45,
                "end_min": 18 * 60 + 15,
                "activity": "🏛️ War Room Council & MoM",
                "category": "council",
                "participants": "Seluruh Tim (Dipimpin Daffa)",
                "room": "War Room Amphitheater",
                "desc": "Sidang koordinasi harian, evaluasi blokir kerja, dan penyusunan notulensi untuk meja CEO."
            },
            {
                "id": "slot-11",
                "time_range": "18:15 – 20:00 WIB",
                "start_min": 18 * 60 + 15,
                "end_min": 20 * 60,
                "activity": "💻 Evening Sprint & Architecture Wrap-up",
                "category": "work",
                "participants": "Idris, Mika, Kael, Raziel",
                "room": "Meja Kerja Divisi",
                "desc": "Finalisasi commit kode harian, update dokumentasi teknis, dan verifikasi CI/CD pipeline."
            },
            {
                "id": "slot-12",
                "time_range": "20:00 – 21:00 WIB",
                "start_min": 20 * 60,
                "end_min": 21 * 60,
                "activity": "🛋️ Night Chill & Standby Pods",
                "category": "sleep",
                "participants": "Tim On-Call (Viktor, Idris, Jovan)",
                "room": "Lounge & Sleep Pods",
                "desc": "Sesi santai malam, monitoring server berkala, dan power nap bagi personel on-call."
            },
            {
                "id": "slot-13",
                "time_range": "21:00 – 08:30 WIB",
                "start_min": 21 * 60,
                "end_min": 24 * 60 + 8 * 60 + 30,
                "activity": "🌙 Night Quarters & Autonomous Standby",
                "category": "night",
                "participants": "Seluruh Sistem (Daemon Telemetry & Sentinel)",
                "room": "Virtual Studio HQ",
                "desc": "Monitoring otomatis daemon VPS, backup berkala, dan persiapan briefing esok pagi oleh Elara."
            }
        ]

        active_phase = None
        for slot in schedule_slots:
            s_start = slot["start_min"]
            s_end = slot["end_min"]
            is_active = False
            if s_end > 24 * 60:
                if cur_minute >= s_start or cur_minute < (s_end - 24 * 60):
                    is_active = True
            else:
                if s_start <= cur_minute < s_end:
                    is_active = True
            if is_active:
                slot["status"] = "ACTIVE_NOW"
                if not active_phase:
                    active_phase = slot
            elif s_end < cur_minute and s_end <= 24 * 60:
                slot["status"] = "COMPLETED"
            else:
                slot["status"] = "UPCOMING"

        if not active_phase:
            active_phase = schedule_slots[0]

        return {
            "current_time_wib": current_time_str,
            "active_phase": active_phase,
            "slots": schedule_slots
        }


    def _seed_initial_conversations(self) -> None:
        """Seed rich historical archive of thoughts and dialogues across all 11 studio members."""
        now = datetime.now(timezone.utc)
        
        # 11 studio members catalog
        initial_records = [
            # Daniandra (CEO)
            {
                "sender_id": "dani",
                "sender_name": "Daniandra Prayudisty",
                "role": "Founder & CEO",
                "room": "Executive Suite",
                "time": "16:55 WIB",
                "category": "executive",
                "topic": "Studio Strategic Vision & Governance",
                "type": "thought",
                "message": "Mengevaluasi kesiapan ekosistem studio: dev pipeline Raziel solid, radar kampus Jovan aktif, kampanye Soetahills makin tajam.",
                "color": "#F5A623",
                "emote": "👑"
            },
            {
                "sender_id": "dani",
                "sender_name": "Daniandra Prayudisty",
                "role": "Founder & CEO",
                "room": "Executive Suite",
                "time": "16:54 WIB",
                "category": "executive",
                "topic": "CEO Mandate on Living Persona",
                "type": "dialogue",
                "message": "Daffa, pastikan seluruh kepala divisi tetap berpegang pada prinsip kejujuran intelektual. Saya butuh data lapangan nyata, bukan laporan yang dipermanis.",
                "color": "#F5A623",
                "emote": "👑"
            },
            # Daffa (CEO Office)
            {
                "sender_id": "daffa",
                "sender_name": "Daffa (CEO Office)",
                "role": "CEO Office",
                "room": "Executive Suite",
                "time": "16:53 WIB",
                "category": "executive",
                "topic": "Executive Strategic Alignment",
                "type": "thought",
                "message": "Menyelaraskan sprint dev Raziel, radar Jovan, dan traksi Soetahills Cucurella, memo briefing siap untuk meja Mas Dani.",
                "color": "#38BDF8",
                "emote": "🎯"
            },
            {
                "sender_id": "daffa",
                "sender_name": "Daffa (CEO Office)",
                "role": "CEO Office",
                "room": "Executive Suite",
                "time": "16:52 WIB",
                "category": "executive",
                "topic": "Telegram Command Bridge Briefing",
                "type": "dialogue",
                "message": "Seluruh catatan notulensi War Room sore ini sudah resmi saya arsipkan dan disposisi langsung ke Telegram Mas Dani via @japirBot.",
                "color": "#38BDF8",
                "emote": "🎯"
            },
            # Raziel Hendrix (CTO)
            {
                "sender_id": "raziel",
                "sender_name": "Raziel Hendrix",
                "role": "CTO & Orchestrator",
                "room": "CTO Command Suite",
                "time": "16:50 WIB",
                "category": "engineering",
                "topic": "Multi-Agent Cluster Throughput",
                "type": "thought",
                "message": "Barusan review PR Idris & Mika, latency stream 0.4ms aman dan arsitektur modular siap nampung beban multi-agent.",
                "color": "#A855F7",
                "emote": "🧐"
            },
            {
                "sender_id": "raziel",
                "sender_name": "Raziel Hendrix",
                "role": "CTO & Orchestrator",
                "room": "CTO Command Suite",
                "time": "16:49 WIB",
                "category": "engineering",
                "topic": "Decoupling Service Sprint Review",
                "type": "dialogue",
                "message": "Kael, diagram modular decoupling service lu udah pas. Idris bisa langsung eksekusi refactoring tanpa nunggu sprint depan.",
                "color": "#A855F7",
                "emote": "🧐"
            },
            # Idris Nakamura (Senior Dev)
            {
                "sender_id": "idris",
                "sender_name": "Idris Nakamura",
                "role": "Senior Developer",
                "room": "Dev Core Workstation",
                "time": "16:48 WIB",
                "category": "engineering",
                "topic": "Query join optimization 400ms to 45ms",
                "type": "thought",
                "message": "Query join nested yang tadinya 400ms berhasil saya pangkas jadi 45ms, guard clause auth dari temuan Viktor udah rapi.",
                "color": "#00FF66",
                "emote": "🎧"
            },
            {
                "sender_id": "idris",
                "sender_name": "Idris Nakamura",
                "role": "Senior Developer",
                "room": "Dev Core Workstation",
                "time": "16:47 WIB",
                "category": "engineering",
                "topic": "SSE Payload Optimization",
                "type": "dialogue",
                "message": "Mika, payload JSON untuk stream SSE udah saya perkecil 40%. Canvas di client bakal jauh lebih enteng waktu load banyak agent.",
                "color": "#00FF66",
                "emote": "🎧"
            },
            # Mika Stellan (Frontend)
            {
                "sender_id": "mika",
                "sender_name": "Mika Stellan",
                "role": "Frontend Engineer",
                "room": "Dev Core Workstation",
                "time": "16:46 WIB",
                "category": "engineering",
                "topic": "Safari Mobile Drawer & 60 FPS Three.js",
                "type": "thought",
                "message": "Glitch drawer navigation mobile Safari udah beres, FPS Three.js locked 60 FPS waktu event SSE masuk bersamaan.",
                "color": "#FF8800",
                "emote": "⚡"
            },
            {
                "sender_id": "mika",
                "sender_name": "Mika Stellan",
                "role": "Frontend Engineer",
                "room": "Dev Core Workstation",
                "time": "16:45 WIB",
                "category": "engineering",
                "topic": "WebGL Mobile Optimization",
                "type": "dialogue",
                "message": "Idris, rendering 11 avatar di mobile sekarang smooth banget, memory footprint Three.js turun ke 36MB tanpa drop frame.",
                "color": "#FF8800",
                "emote": "⚡"
            },
            # Viktor Moreau (QA & Sec)
            {
                "sender_id": "viktor",
                "sender_name": "Viktor Moreau",
                "role": "Lead QA & Security",
                "room": "Server Room NOC",
                "time": "16:44 WIB",
                "category": "security",
                "topic": "Sentinel 4-Layer Defense Audit",
                "type": "thought",
                "message": "Unhandled 500 error pas payload kosong udah beres ditambal Idris, semua 35 test suite hijau dan build layak naik staging.",
                "color": "#FF3333",
                "emote": "🛡️"
            },
            {
                "sender_id": "viktor",
                "sender_name": "Viktor Moreau",
                "role": "Lead QA & Security",
                "room": "Server Room NOC",
                "time": "16:43 WIB",
                "category": "security",
                "topic": "Port 9449 Security Hardening",
                "type": "dialogue",
                "message": "Raziel, hasil penetration test port 9449 aman. Rule Fail2ban dan CORS whitelist udah sesuai standar ISO studio.",
                "color": "#FF3333",
                "emote": "🛡️"
            },
            # Kael Ashford (Architect)
            {
                "sender_id": "kael",
                "sender_name": "Kael Ashford",
                "role": "Lead Architect",
                "room": "Atelier Architecture Lab",
                "time": "16:42 WIB",
                "category": "architecture",
                "topic": "Event-Driven Loop & LaTeX Compile",
                "type": "thought",
                "message": "Lagi kalkulasi beban database event-driven loop, circular dependency berhasil dicegah dan Tectonic compile bersih.",
                "color": "#0070F3",
                "emote": "📐"
            },
            {
                "sender_id": "kael",
                "sender_name": "Kael Ashford",
                "role": "Lead Architect",
                "room": "Atelier Architecture Lab",
                "time": "16:41 WIB",
                "category": "architecture",
                "topic": "Thesis Chapter 3-4 Sequence Alignment",
                "type": "dialogue",
                "message": "Nara, sequence diagram Bab 3-4 sudah klop sama benchmark paper. Struktur publikasi skripsi Mas Dani makin solid.",
                "color": "#0070F3",
                "emote": "📐"
            },
            # Nara Vasquez (Researcher)
            {
                "sender_id": "nara",
                "sender_name": "Nara Vasquez",
                "role": "Lead Researcher",
                "room": "Atelier Architecture Lab",
                "time": "16:40 WIB",
                "category": "architecture",
                "topic": "arXiv 4.2x Efficiency Benchmark",
                "type": "thought",
                "message": "Data benchmark terbaru membuktikan arsitektur event-driven 4.2x lebih efisien, 14 paper Scopus siap untuk novelti skripsi.",
                "color": "#00DFD8",
                "emote": "📚"
            },
            {
                "sender_id": "nara",
                "sender_name": "Nara Vasquez",
                "role": "Lead Researcher",
                "room": "Atelier Architecture Lab",
                "time": "16:39 WIB",
                "category": "architecture",
                "topic": "Scopus Q1 BibTeX Synthesis",
                "type": "dialogue",
                "message": "Kael, 14 paper Scopus Q1 sudah saya sintesis ke format BibTeX. Argumen efisiensi komputasi kita punya rujukan kuat.",
                "color": "#00DFD8",
                "emote": "📚"
            },
            # Senna Louviere (Creative Director)
            {
                "sender_id": "senna",
                "sender_name": "Senna Louviere",
                "role": "Creative Director",
                "room": "Creative Studio",
                "time": "16:38 WIB",
                "category": "design",
                "topic": "Obsidian Luxury Visual Balance",
                "type": "thought",
                "message": "Lagi poles ritme spasi dan balance visual hero section, impresi obsidian premium makin tajam dan kontras tombol tegas.",
                "color": "#FF0080",
                "emote": "🎨"
            },
            {
                "sender_id": "senna",
                "sender_name": "Senna Louviere",
                "role": "Creative Director",
                "room": "Creative Studio",
                "time": "16:37 WIB",
                "category": "design",
                "topic": "Mobile Card Contrast Tuning",
                "type": "dialogue",
                "message": "Mika, token warna obsidian-emerald dan hierarki tipografi di mobile card udah saya sesuaikan biar nyaman di mata pengguna.",
                "color": "#FF0080",
                "emote": "🎨"
            },
            # Elara Sinclair (PA)
            {
                "sender_id": "elara",
                "sender_name": "Elara Sinclair",
                "role": "Personal Assistant",
                "room": "PA Executive Office",
                "time": "16:36 WIB",
                "category": "executive",
                "topic": "Executive Buffer & Night Prep",
                "type": "thought",
                "message": "Menjaga ritme kerja Mas Dani tetap prima, briefing kalender dan rekap kas rapi, siap kawal evaluasi malam.",
                "color": "#E0AAFF",
                "emote": "📋"
            },
            {
                "sender_id": "elara",
                "sender_name": "Elara Sinclair",
                "role": "Personal Assistant",
                "room": "PA Executive Office",
                "time": "16:35 WIB",
                "category": "executive",
                "topic": "Calendar Buffer Advisory",
                "type": "dialogue",
                "message": "Mas Dani, jadwal sore ini sudah saya beri buffer 45 menit untuk istirahat setelah diskusi teknis maraton.",
                "color": "#E0AAFF",
                "emote": "📋"
            },
            # Jovan Aritza (Intel)
            {
                "sender_id": "jovan",
                "sender_name": "Jovan Aritza",
                "role": "Intelligence Officer",
                "room": "Radar NOC",
                "time": "16:34 WIB",
                "category": "intel",
                "topic": "Telkom University Campus Radar",
                "type": "thought",
                "message": "Jadwal revisi sidang fakultas terverifikasi dari menfess dan anak BEM, seminar event-driven 15 Okt klop sama skripsi Mas Dani.",
                "color": "#39FF14",
                "emote": "📡"
            },
            {
                "sender_id": "jovan",
                "sender_name": "Jovan Aritza",
                "role": "Intelligence Officer",
                "room": "Radar NOC",
                "time": "16:33 WIB",
                "category": "intel",
                "topic": "Campus Seminar Seat Verification",
                "type": "dialogue",
                "message": "Daffa, radar BEM Telkom University mengonfirmasi kuota seminar arsitektur tinggal 20 kursi. Jadwal Mas Dani pas banget untuk hadir.",
                "color": "#39FF14",
                "emote": "📡"
            },
        ]
        
        wib_now = get_wib_now()
        for idx, rec in enumerate(initial_records):
            rec["id"] = f"chat-seed-{idx+1}"
            # Align time dynamically to current office WIB clock
            rec["time"] = (wib_now - timedelta(minutes=(idx * 2 + 1))).strftime("%H:%M:%S WIB")
            self.conversation_history.append(rec)

    def record_conversation_or_thought(
        self,
        agent_id: str,
        message: str,
        topic: str = "Aktivitas Kerja",
        msg_type: str = "thought",
        room_id: Optional[str] = None,
        category: Optional[str] = None,
    ) -> dict[str, Any]:
        """Record an authentic thought or dialogue into the studio-wide conversation history."""
        agent = self.agents.get(agent_id)
        sender_name = agent.name if agent else agent_id.capitalize()
        role = agent.role if agent else "Staff"
        color = agent.avatar_color if agent else "#00F2FE"
        
        # Emotes catalog
        emotes = {
            "dani": "👑", "daffa": "🎯", "raziel": "🧐", "idris": "🎧",
            "mika": "⚡", "viktor": "🛡️", "kael": "📐", "nara": "📚",
            "senna": "🎨", "elara": "📋", "jovan": "📡", "cucurella": "🏡"
        }
        emote = emotes.get(agent_id, "👤")
        
        # Room naming
        room_name = "Workstation"
        if room_id and room_id in self.rooms:
            room_name = self.rooms[room_id].name
        elif agent and agent.position.room_id in self.rooms:
            room_name = self.rooms[agent.position.room_id].name

        # Category mapping
        cat_map = {
            "dani": "executive", "daffa": "executive", "elara": "executive",
            "cucurella": "growth",
            "raziel": "engineering", "idris": "engineering", "mika": "engineering",
            "viktor": "security",
            "kael": "architecture", "nara": "architecture",
            "senna": "design",
            "jovan": "intel",
        }
        computed_cat = category or cat_map.get(agent_id, "engineering")
        
        time_str = get_wib_now().strftime("%H:%M:%S WIB")
        entry_id = f"chat-{int(datetime.now(timezone.utc).timestamp()*1000)}"
        
        entry = {
            "id": entry_id,
            "sender_id": agent_id,
            "sender_name": sender_name,
            "role": role,
            "room": room_name,
            "time": time_str,
            "category": computed_cat,
            "topic": topic,
            "type": msg_type,
            "message": message,
            "color": color,
            "emote": emote,
        }
        
        # Insert at front, maintain max 250 items
        self.conversation_history.insert(0, entry)
        if len(self.conversation_history) > 250:
            self.conversation_history = self.conversation_history[:250]
            
        return entry

    def get_conversation_history(
        self,
        limit: int = 60,
        agent_id: Optional[str] = None,
        category: Optional[str] = None,
        type: Optional[str] = None,
        q: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch chronological log of conversations and real-time thoughts with comprehensive filters."""
        results = list(self.conversation_history)
        if agent_id and agent_id != "all":
            results = [r for r in results if r.get("sender_id") == agent_id]
        if category and category != "all":
            results = [r for r in results if r.get("category") == category]
        if type and type != "all":
            results = [r for r in results if r.get("type") == type]
        if q:
            query = q.lower()
            results = [
                r for r in results
                if query in r.get("message", "").lower()
                or query in r.get("sender_name", "").lower()
                or query in r.get("topic", "").lower()
                or query in r.get("role", "").lower()
            ]
        return results[:limit]

    async def send_meeting_telegram_report(self, meeting_id: Optional[str] = None) -> dict[str, Any]:
        """Format and dispatch meeting minutes directly to CEO Daniandra via Telegram bot."""
        meeting = self.get_meeting_by_id(meeting_id) if meeting_id else self.get_latest_meeting()
        if not meeting:
            return {"success": False, "error": "No meeting minutes available to report"}

        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if not bot_token:
            env_file = "/home/daniilham/.hermes/.env"
            if os.path.exists(env_file):
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("TELEGRAM_BOT_TOKEN="):
                            bot_token = line.split("=", 1)[1].strip().strip("'\"").strip()

        if not bot_token:
            return {"success": False, "error": "TELEGRAM_BOT_TOKEN not configured"}

        recipients = ["1062533303", "-5549212752"]

        decisions_md = "\n".join([f"• {d}" for d in meeting.key_decisions[:4]])
        action_items_md = "\n".join([f"• *{a.pic}*: {a.task} _(Target: {a.due})_" for a in meeting.action_items[:5]])
        dialogue_md = "\n".join([f"• *{d.speaker_name}*: \"{d.text[:130]}...\"" for d in meeting.dialogues[:3]])

        text = (
            f"🏛️ *[NOTULENSI RESMI WAR ROOM — CEO OFFICE]*\n"
            f"*Yudiaz Creative Studio Virtual HQ*\n\n"
            f"Kepada: *CEO Daniandra Prayudisty (Mas Dani)*\n"
            f"Dari: *Daffa — Head of CEO Office & Chief of Staff*\n\n"
            f"Yth. Mas Dani,\n"
            f"Berikut kami laporkan hasil rapat koordinasi strategis studio yang baru saja dipimpin oleh CEO Office di War Room:\n\n"
            f"📋 *Topik:* {meeting.title}\n"
            f"👤 *Pemimpin Sidang:* {meeting.leader_name}\n"
            f"📝 *Notulis:* Elara Sinclair (Executive PA)\n"
            f"📊 *Status:* {meeting.status} (Diserahkan ke Meja CEO)\n\n"
            f"✨ *Keputusan Strategis yang Disahkan:*\n{decisions_md}\n\n"
            f"📌 *Action Items & Komitmen PIC:*\n{action_items_md}\n\n"
            f"💬 *Transkrip Diskusi Tim:*\n{dialogue_md}\n\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 *Dashboard Virtual HQ:* https://office.daniandraaa.my.id\n"
            f"_Laporan ini resmi disampaikan melalui CEO Office Command Bridge. Siap menerima disposisi lanjutan dari Mas Dani._"
        )

        delivered = []
        errors = []
        async with httpx.AsyncClient(timeout=10.0) as client:
            for chat_id in recipients:
                try:
                    resp = await client.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": text,
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": True,
                        }
                    )
                    if resp.status_code == 200:
                        delivered.append(chat_id)
                    else:
                        errors.append(f"{chat_id}: {resp.text}")
                except Exception as e:
                    errors.append(f"{chat_id}: {str(e)}")

        return {
            "success": len(delivered) > 0,
            "delivered_to": delivered,
            "errors": errors,
            "meeting_id": meeting.meeting_id,
            "title": meeting.title,
        }

    async def dispatch_ceo_command(self, command: str) -> CEOCommandResponse:
        """Process an executive command from CEO Daniandra via AI reasoning (9Router).
        Dispatches hierarchical dialogue cascade: CEO -> Daffa -> Target Agent -> Report back.
        """
        now_str = datetime.now(timezone.utc).strftime("%d %b %Y // %H:%M:%S WIB")

        roster = {
            "dani": {"name": "Daniandra Prayudisty", "role": "Founder & CEO", "emoji": "👑", "color": "#F5A623"},
            "daffa": {"name": "Daffa (CEO Office)", "role": "CEO Office", "emoji": "🎯", "color": "#38BDF8"},
            "raziel": {"name": "Raziel Hendrix", "role": "CTO & Lead Orchestrator", "emoji": "🧐", "color": "#A855F7"},
            "kael": {"name": "Kael Ashford", "role": "Lead Architect", "emoji": "🏛️", "color": "#38BDF8"},
            "nara": {"name": "Nara Vasquez", "role": "Lead Researcher", "emoji": "🔬", "color": "#00DFD8"},
            "idris": {"name": "Idris Nakamura", "role": "Senior Developer", "emoji": "⚡", "color": "#F5A623"},
            "mika": {"name": "Mika Stellan", "role": "Frontend Engineer", "emoji": "🎨", "color": "#EC4899"},
            "senna": {"name": "Senna Louviere", "role": "Creative Director", "emoji": "✨", "color": "#F43F5E"},
            "viktor": {"name": "Viktor Moreau", "role": "Lead QA & Security", "emoji": "🛡️", "color": "#10B981"},
            "elara": {"name": "Elara Sinclair", "role": "Executive PA", "emoji": "📋", "color": "#F472B6"},
            "jovan": {"name": "Jovan Aritza", "role": "Intelligence Officer", "emoji": "📡", "color": "#6366F1"},
            "cucurella": {"name": "Cucurella (Soetahills Growth)", "role": "Head of Soetahills Growth", "emoji": "🏡", "color": "#10B981"},
        }

        ai_data = await self._call_ai_engine(command)

        target_id = ai_data.get("assigned_agent_id", "idris")
        if target_id not in roster or target_id in ["dani", "daffa"]:
            target_id = "idris"

        target_info = roster.get(target_id, roster["idris"])
        assigned_task = ai_data.get("assigned_task", f"Eksekusi mandat CEO: {command}")
        thought = ai_data.get("thought", "Memprioritaskan eksekusi teknis sesuai instruksi Mas Dani.")

        raw_dialogues = ai_data.get("dialogues", [])
        dialogues: list[DialogueTurn] = []
        for d in raw_dialogues:
            spk_id = d.get("speaker_id", "dani")
            spk_info = roster.get(spk_id, roster.get("dani"))
            dialogues.append(
                DialogueTurn(
                    speaker_id=spk_id,
                    speaker_name=spk_info["name"],
                    role=spk_info["role"],
                    emoji=spk_info["emoji"],
                    color=spk_info["color"],
                    text=d.get("text", ""),
                    target_id=d.get("target_id", None),
                )
            )

        # Update target agent state
        target_agent = self.agents.get(target_id)
        if target_agent:
            target_agent.current_task = assigned_task
            target_agent.status = AgentStatus.WORKING
            target_agent.memory_context = thought

        # If academic research / skripsi command, update both kael and nara
        if target_id in ["kael", "nara"] or any(k in command.lower() for k in ["skripsi", "latex", "paper", "arxiv", "tectonic"]):
            for r_id in ["kael", "nara"]:
                r_agent = self.agents.get(r_id)
                if r_agent:
                    r_agent.status = AgentStatus.RESEARCHING
                    if r_id == "kael":
                        r_agent.current_task = f"Arsitektur & Diagram Skripsi Tel-U (Mandat CEO): {command}"
                    else:
                        r_agent.current_task = f"Sintesis Paper & Kompilasi LaTeX Tectonic (Mandat CEO): {command}"

        # Record activity log
        self.add_activity(
            agent_id="dani",
            action="CEO_DISPATCH",
            details=f"CEO menginstruksikan {target_info['name']}: {assigned_task}",
            severity="WARNING",
            room_id="room-ceo",
        )

        event_payload = {
            "type": "CEO_BUREAUCRACY_CASCADE",
            "event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "command": command,
            "assigned_agent_id": target_id,
            "assigned_agent_name": target_info["name"],
            "assigned_task": assigned_task,
            "thought": thought,
            "dialogues": [d.model_dump() for d in dialogues],
            "timestamp": now_str,
            "requires_war_room": any(k in command.lower() for k in ["rapat", "meeting", "presentasi", "evaluasi", "war room", "strategi"]),
        }
        self.active_event = event_payload
        self._broadcast_state()

        return CEOCommandResponse(
            status="ok",
            command=command,
            assigned_agent_id=target_id,
            assigned_agent_name=target_info["name"],
            assigned_task=assigned_task,
            thought=thought,
            dialogues=dialogues,
            timestamp=now_str,
        )

    async def dispatch_pa_notification(
        self,
        title: str,
        message: str,
        source: str = "cron",
        telegram_sent: bool = True,
    ) -> dict:
        """Dispatches an executive reminder or cron event via PA Elara Sinclair."""
        now_str = datetime.now(timezone.utc).strftime("%d %b %Y // %H:%M:%S WIB")

        dialogues = [
            {
                "speaker_id": "elara",
                "speaker_name": "Elara Sinclair",
                "role": "Executive PA",
                "emoji": "📋",
                "color": "#F472B6",
                "text": f"Pak Dani, pengingat eksekutif: {title}. {message}. Notifikasi Telegram telah dikirim.",
                "target_id": "dani",
            },
            {
                "speaker_id": "dani",
                "speaker_name": "Daniandra Prayudisty",
                "role": "Founder & CEO",
                "emoji": "👑",
                "color": "#F5A623",
                "text": f"Terima kasih Elara. Saya monitor dan tindak lanjuti agenda {title} dari suite.",
                "target_id": "elara",
            },
        ]

        event_payload = {
            "type": "PA_CRON_REMINDER",
            "event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "title": title,
            "message": message,
            "source": source,
            "telegram_sent": telegram_sent,
            "dialogues": dialogues,
            "timestamp": now_str,
        }

        self.active_event = event_payload
        self.add_activity(
            agent_id="elara",
            action="PA_REMINDER",
            details=f"Elara menyampaikan reminder eksekutif: {title}",
            severity="INFO",
            room_id="room-ceo",
        )
        self._broadcast_state()

        return {
            "status": "ok",
            "title": title,
            "message": message,
            "dialogues": dialogues,
            "timestamp": now_str,
        }

    async def _call_ai_engine(self, command: str) -> dict:
        """Call 9Router API (ag/gemini-3.8-flash) or fallback to heuristic."""
        key = os.getenv("HERMES_9ROUTER_API_KEY", "")
        if not key:
            try:
                with open("/home/daniilham/.hermes/.env") as f:
                    for line in f:
                        if "HERMES_9ROUTER_API_KEY" in line:
                            key = line.split("=")[1].strip('"\'\n ')
            except Exception:
                pass

        if key:
            system_prompt = (
                "Kamu adalah Engine Penalaran Orkestrasi Yudiaz Creative Studio.\n"
                "Struktur Tim:\n"
                "- Daniandra Prayudisty (Founder & CEO, id: dani)\n"
                "- Daffa (CEO Office & Strategic Alignment, id: daffa - berkantor di CEO Suite bersama CEO)\n"
                "- Raziel Hendrix (CTO & Lead Orchestrator, id: raziel)\n"
                "- Kael Ashford (Lead Architect, id: kael)\n"
                "- Nara Vasquez (Lead Researcher, id: nara)\n"
                "- Idris Nakamura (Senior Developer - backend, FastAPI, QRIS webhook, id: idris)\n"
                "- Mika Stellan (Frontend Engineer - Three.js, React, UI, id: mika)\n"
                "- Senna Louviere (Creative Director - Figma, tokens, Concept 2B, id: senna)\n"
                "- Viktor Moreau (Lead QA & Security - UFW, Fail2ban, pytest, id: viktor)\n"
                "- Elara Sinclair (Executive PA - jadwal CEO, finance portal, id: elara)\n"
                "- Jovan Aritza (Intelligence Officer - Tel-U radar, id: jovan)\n"
                "- Cucurella (Head of Soetahills Growth - riset pasar properti, Instagram @soetahills, video reels, id: cucurella)\n\n"
                "Konteks riil studio: VPS Azure Seoul 54GB RAM Intel Xeon, 4 subdomains (office, vps, api, finance), "
                "Tectonic LaTeX compiler Skripsi Tel-U, Micro-SaaS QRIS Dynamic Tripay payment gateway, Fail2ban IP 2.57.122.209 banned.\n\n"
                "TUGAS: Ketika CEO memberikan perintah, analisis siapa yang harus mengerjakan, rancang tugasnya, dan hasilkan dialog berjenjang:\n"
                "1. CEO Daniandra bicara ke Daffa (CEO Office)\n"
                "2. Daffa merespons CEO dan memberi arahan ke agent terkait\n"
                "3. Agent terkait merespons teknis dan menyanggupi\n"
                "4. Daffa melapor balik ke CEO bahwa instruksi sudah berjalan\n"
                "5. CEO Daniandra memberikan penutupan / disposisi approval\n\n"
                "Hasilkan HANYA JSON MURNI tanpa markdown:\n"
                "{\n"
                '  "assigned_agent_id": "idris",\n'
                '  "assigned_task": "Tugas teknis spesifik untuk agent",\n'
                '  "thought": "Pikiran teknis mendalam internal agent",\n'
                '  "dialogues": [\n'
                '    {"speaker_id": "dani", "text": "..."},\n'
                '    {"speaker_id": "daffa", "text": "..."},\n'
                '    {"speaker_id": "target_id", "text": "..."},\n'
                '    {"speaker_id": "daffa", "text": "..."},\n'
                '    {"speaker_id": "dani", "text": "..."}\n'
                "  ]\n"
                "}"
            )
            try:
                headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
                payload = {
                    "model": "ag/gemini-3.8-flash",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Perintah CEO: {command}"},
                    ],
                    "stream": False,
                    "temperature": 0.7,
                    "max_tokens": 700,
                }
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post("http://127.0.0.1:20128/v1/chat/completions", headers=headers, json=payload)
                    if resp.status_code == 200:
                        raw = resp.json()["choices"][0]["message"]["content"].strip()
                        raw = re.sub(r"^```(?:json)?\s*", "", raw)
                        raw = re.sub(r"\s*```$", "", raw)
                        return json.loads(raw)
            except Exception as e:
                print(f"[OfficeEngine] AI 9Router dispatch error, fallback to heuristic: {e}")

        return self._generate_heuristic_cascade(command)

    def _generate_heuristic_cascade(self, command: str) -> dict:
        """Fallback dynamic heuristic generator matching actual Yudiaz ecosystem."""
        cmd = command.lower()
        if any(k in cmd for k in ["qris", "payment", "fastapi", "webhook", "tripay", "backend", "db", "endpoint"]):
            target_id = "idris"
            task = f"Implementasi & audit endpoint QRIS Dynamic di FastAPI: {command}"
            thought = "Memastikan validasi raw body HMAC-SHA256 compare_digest dan lock idempotensi transaksi aktif."
            d1 = f"Daf, instruksikan Idris segera eksekusi teknis: {command}."
            d2 = f"Siap Mas Dani. Idris, prioritaskan request CEO: {command}. Pastikan endpoint dan signature HMAC terisolasi aman."
            d3 = f"Siap Mas Daffa! Endpoint sedang saya siapkan di router FastAPI. Verifikasi HMAC-SHA256 dan idempotent state lock berjalan lancar."
            d4 = f"Mas Dani, Idris sudah mengunci task di Workstations dan implementasi webhook sedang berlangsung."
            d5 = f"Bagus. Pastikan lolos integrasi sebelum kita sync ke Caddy gateway."
        elif any(k in cmd for k in ["skripsi", "latex", "tectonic", "bab 1", "bab 2", "bab 3", "bab 4", "bab 5", "paper", "arxiv", "tel-u", "telkom"]):
            target_id = "kael"
            task = f"Kompilasi & Pemodelan Naskah Skripsi Tel-U via Tectonic: {command}"
            thought = "Menyelaraskan struktur modular naskah Bab 1-5 dengan template resmi Telkom University dan build Tectonic 1.4s."
            d1 = f"Daf, arahkan Kael dan Nara di Atelier: {command}. Mandat riset skripsi resmi saya buka."
            d2 = f"Dimengerti Mas Dani. Kael & Nara, Mas Dani memberikan mandat riset: {command}. Sinkronkan diagram dan kompilasi Tectonic sekarang."
            d3 = f"Siap Mas Daffa! Mandat CEO diterima. Saya dan Nara langsung menyusun spesifikasi formal dan build via compiler Tectonic."
            d4 = f"Mas Dani, Kael dan Nara sudah aktif di meja Atelier. Naskah skripsi sedang dikurasi dan dikompilasi."
            d5 = f"Mantap. Pantau agar zero warning dan sitasi IEEE rapi."
        elif any(k in cmd for k in ["security", "fail2ban", "ufw", "firewall", "audit", "test", "pytest", "bug", "hardening", "banned"]):
            target_id = "viktor"
            task = f"Audit 4-layer defense & pengujian stabilitas: {command}"
            thought = "Memeriksa UFW rate limiting, status jail fail2ban, dan memastikan 35 test suite hijau 100%."
            d1 = f"Daf, minta Viktor di Server Room untuk audit: {command}."
            d2 = f"Siap Mas Dani. Viktor, lakukan pengecekan menyeluruh terhadap {command} sekarang."
            d3 = f"Siap Mas Daffa. Server Room aman, UFW dan jail fail2ban sedang saya scan mendalam. Log serangan IP penyerang terkendali."
            d4 = f"Mas Dani, Viktor melaporkan cluster server dalam kondisi optimum dan perimeter keamanan aman."
            d5 = f"Bagus Viktor. Tetap siaga di Server Room."
        elif any(k in cmd for k in ["desain", "design", "figma", "token", "logo", "brand", "warna", "concept"]):
            target_id = "senna"
            task = f"Penyempurnaan Design System Concept 2B: {command}"
            thought = "Mematangkan token obsidian-glass, palet cyan aksen, dan visual balance di seluruh antarmuka."
            d1 = f"Daf, koordinasikan dengan Senna di Creative Studio: {command}."
            d2 = f"Siap Mas Dani. Senna, Mas Dani ingin design token diselaraskan: {command}."
            d3 = f"Siap Mas Daffa! Figma workspace dan token obsidian Concept 2B sedang saya polish agar proporsi visualnya presisi."
            d4 = f"Mas Dani, Senna sedang mematangkan aset visual di Creative Studio."
            d5 = f"Oke Senna, jaga estetika cyber-luxury tetap bersih."
        elif any(k in cmd for k in ["jadwal", "agenda", "finance", "kas", "pembukuan", "uang", "saldo"]):
            target_id = "elara"
            task = f"Rekonsiliasi eksekutif & catatan keuangan: {command}"
            thought = "Memastikan kalender terjadwal rapi dan buku kas Yudiaz Finance tersinkronisasi presisi."
            d1 = f"Daf, beri tahu Elara untuk tangani: {command}."
            d2 = f"Elara, tolong akomodir arahan Mas Dani terkait: {command}."
            d3 = f"Siap Mas Daffa dan Mas Dani! Kalender eksekutif dan buku kas Yudiaz Finance sudah saya mutakhirkan sekarang."
            d4 = f"Mas Dani, Elara sudah menyelesaikan pencatatan dan berkas siap di meja CEO."
            d5 = f"Terima kasih Elara, kerja bagus."
        elif any(k in cmd for k in ["three.js", "frontend", "diorama", "ui", "animasi", "webgl"]):
            target_id = "mika"
            task = f"Optimasi frontend Three.js & render loop 60 FPS: {command}"
            thought = "Memastikan WebGL buffer efisien, zero lag, dan interaktivitas responsif di desktop maupun mobile."
            d1 = f"Daf, minta Mika optimasi tampilan: {command}."
            d2 = f"Mika, fokuskan ke frontend Three.js sesuai instruksi Mas Dani: {command}."
            d3 = f"Siap Mas Daffa! Shader, render loop 60 FPS, dan interaktivitas 3D langsung saya optimize."
            d4 = f"Mas Dani, Mika sudah mengeksekusi penyesuaian frontend dan performa stabil 60 FPS."
            d5 = f"Mantap Mika, pastikan lancar di HP juga."
        else:
            target_id = "idris"
            task = f"Penanganan tugas operasional teknis: {command}"
            thought = "Menganalisis dependensi arsitektur dan mengeksekusi instruksi CEO secara terstruktur."
            d1 = f"Daf, instruksikan tim untuk segera eksekusi: {command}."
            d2 = f"Siap Mas Dani. Idris dan tim lead, mohon atensi untuk instruksi CEO: {command}."
            d3 = f"Siap Mas Daffa! Task sudah saya ambil dan langsung saya breakdown pengerjaannya di workstation."
            d4 = f"Mas Dani, instruksi sudah didelegasikan dan progress teknis sedang berjalan."
            d5 = f"Bagus, lanjutkan dan laporkan perkembangannya."

        return {
            "assigned_agent_id": target_id,
            "assigned_task": task,
            "thought": thought,
            "dialogues": [
                {"speaker_id": "dani", "text": d1},
                {"speaker_id": "daffa", "text": d2},
                {"speaker_id": target_id, "text": d3},
                {"speaker_id": "daffa", "text": d4},
                {"speaker_id": "dani", "text": d5},
            ],
        }

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

        if agent_id in self._temporary_assignments:
            del self._temporary_assignments[agent_id]

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
        """Trigger War Room protocol: gather 10 operational personnel led by Daffa while CEO Daniandra stays in CEO Suite."""
        self._office_mode = OfficeMode.WAR_ROOM
        self._temporary_assignments.clear()
        self._council_active = False
        war_room = self.rooms["room-war"]
        ceo_room = self.rooms["room-ceo"]

        # Clear occupants across all rooms
        for room in self.rooms.values():
            room.current_occupants.clear()

        # CEO Daniandra stays in room-ceo reviewing strategic vision
        dani_agent = self.agents["dani"]
        ceo_room.current_occupants.append("dani")
        dani_agent.status = AgentStatus.WORKING
        dani_agent.current_task = "Executive Strategic Vision & Studio Governance Oversight (CEO Suite)"
        dani_agent.active_tool = "Notion Strategic Roadmap"
        dani_agent.updated_at = datetime.now(timezone.utc).isoformat()
        self._reposition_room_occupants("room-ceo")

        # Place the 10 operational personnel into War Room led by Daffa
        for agent_id, agent in self.agents.items():
            if agent_id == "dani":
                continue
            war_room.current_occupants.append(agent_id)
            agent.status = AgentStatus.MEETING
            if agent_id == "daffa":
                agent.current_task = "Leading War Room Council: Business Analysis & Studio Operations (CEO Office)"
            elif agent_id == "raziel":
                agent.current_task = "Technical Architecture & Micro-SaaS Business Analysis"
            else:
                agent.current_task = "War Room Council: Business Opportunities & Technical Strategy Deliberation"
            agent.updated_at = datetime.now(timezone.utc).isoformat()

        self._reposition_room_occupants("room-war")

        self.latest_meeting = self._generate_council_meeting(
            leader_id="daffa",
            leader_name="Daffa (CEO Office)",
            status="IN_PROGRESS",
            need_index=self._need_cycle_index,
        )
        self._archive_meeting(self.latest_meeting)

        self.add_activity(
            agent_id="daffa",
            action="WAR_ROOM_CONVENED",
            details="Daffa (CEO Office) convened War Room council with 10 operational personnel. CEO Daniandra reviews strategic vision from the CEO Suite.",
            severity="ALERT",
            room_id="room-war",
        )

        return self.get_state()

    def resume_deep_work(self) -> OfficeStateResponse:
        """Disperse personnel back to core workstations and resume deep focus."""
        self._office_mode = OfficeMode.NORMAL
        self._temporary_assignments.clear()
        self._council_active = False

        if self.latest_meeting and self.latest_meeting.status == "IN_PROGRESS":
            self.latest_meeting.status = "COMPLETED"
            self.latest_meeting.reporting_to_ceo = "Diserahkan kepada CEO Daniandra Prayudisty oleh Daffa (CEO Office)"
            self.latest_meeting.ceo_feedback = "Disetujui. Lanjutkan eksekusi teknis di bawah supervisi CTO Raziel Hendrix."
            self._archive_meeting(self.latest_meeting)
            self.add_activity(
                agent_id="daffa",
                action="EXECUTIVE_BRIEFING_DELIVERED",
                details="Daffa (CEO Office) delivered the Executive Council Briefing & Business Recommendations to CEO Daniandra in the Executive Suite.",
                severity="INFO",
                room_id="room-ceo",
            )

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
            room_id="room-cto",
        )

        return self.get_state()

    def trigger_sleep_cycle(self) -> OfficeStateResponse:
        """Trigger headquarters-wide rest and regeneration cycle."""
        self._office_mode = OfficeMode.REST_CYCLE
        self._temporary_assignments.clear()
        self._council_active = False

        if self.latest_meeting and self.latest_meeting.status == "IN_PROGRESS":
            self.latest_meeting.status = "COMPLETED"
            self.latest_meeting.reporting_to_ceo = "Diserahkan kepada CEO Daniandra Prayudisty oleh Daffa (CEO Office)"
            self.latest_meeting.ceo_feedback = "Disetujui. Lanjutkan eksekusi teknis di bawah supervisi CTO Raziel Hendrix."
            self._archive_meeting(self.latest_meeting)
            self.add_activity(
                agent_id="daffa",
                action="EXECUTIVE_BRIEFING_DELIVERED",
                details="Daffa (CEO Office) delivered the Executive Council Briefing & Business Recommendations to CEO Daniandra in the Executive Suite.",
                severity="INFO",
                room_id="room-ceo",
            )

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
        self._temporary_assignments.clear()
        self._council_active = False

        if self.latest_meeting and self.latest_meeting.status == "IN_PROGRESS":
            self.latest_meeting.status = "COMPLETED"
            self.latest_meeting.reporting_to_ceo = "Diserahkan kepada CEO Daniandra Prayudisty oleh Daffa (CEO Office)"
            self.latest_meeting.ceo_feedback = "Disetujui. Lanjutkan eksekusi teknis di bawah supervisi CTO Raziel Hendrix."
            self._archive_meeting(self.latest_meeting)
            self.add_activity(
                agent_id="daffa",
                action="EXECUTIVE_BRIEFING_DELIVERED",
                details="Daffa (CEO Office) delivered the Executive Council Briefing & Business Recommendations to CEO Daniandra in the Executive Suite.",
                severity="INFO",
                room_id="room-ceo",
            )

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
            "cucurella": {
                "room": "room-concierge",
                "task": "Casual chats over espresso about property market trends",
                "tool": "Market Pulse Dashboard",
            },
            "elara": {
                "room": "room-concierge",
                "task": "Espresso break & casual executive chat",
                "tool": "Italian Espresso",
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
        """Autonomous background loop: executes _simulate_tick on interval."""
        while self._is_running:
            try:
                await asyncio.sleep(self.settings.simulation_interval)
                self._simulate_tick()
            except asyncio.CancelledError:
                break
            except Exception:
                # Keep background simulation resilient
                pass

    def _get_ambient_events(self) -> list[tuple[str, str, str, str]]:
        """Catalog of ambient micro-actions performed by agents during deep focus."""
        return [
            (
                "viktor",
                "SECURITY_PROBE",
                "Viktor Moreau memverifikasi Sentinel 4-Layer Defense: unhandled 500 error saat payload kosong berhasil ditambal Idris, semua 35 test suite hijau 100%.",
                "INFO",
            ),
            (
                "nara",
                "CITATIONS_INDEXED",
                "Nara Vasquez menemukan pembuktian empiris di paper arXiv terbaru: arsitektur event-driven 4.2x lebih hemat resource dibanding centralized orchestrator.",
                "INFO",
            ),
            (
                "idris",
                "QUERY_OPTIMIZED",
                "Idris Nakamura berhasil memangkas query join nested dari 400ms menjadi 45ms dan mengaktifkan guard clause autentikasi di FastAPI port 9449.",
                "INFO",
            ),
            (
                "mika",
                "ANIMATION_POLISHED",
                "Mika Stellan memperhalus transisi drawer navigation mobile Safari dan mengunci frame rate Three.js di 60 FPS stabil tanpa jitter.",
                "INFO",
            ),
            (
                "senna",
                "DESIGN_TOKENS_LOCKED",
                "Senna Louviere merampungkan token visual obsidian emerald di Figma dan mengunci kontras tombol CTA agar ramah mata dan elegan.",
                "INFO",
            ),
            (
                "raziel",
                "PR_MERGED",
                "Raziel Hendrix menyetujui branch feat/sse-optimize dari Idris & Mika setelah memvalidasi latency 0.4ms tanpa memory leak di cluster VPS.",
                "INFO",
            ),
            (
                "daffa",
                "EXECUTIVE_ALIGNMENT",
                "Daffa menyelaraskan progres sprint dev Raziel dengan agenda pertumbuhan Soetahills Cucurella dan menyiapkan executive memo untuk Pak Dani.",
                "INFO",
            ),
            (
                "jovan",
                "CAMPUS_RADAR_PING",
                "Jovan Aritza memverifikasi pengumuman resmi seminar arsitektur event-driven Telkom University 15 Oktober yang selaras dengan topik skripsi Mas Dani.",
                "INFO",
            ),
            (
                "elara",
                "WELLNESS_CHECK",
                "Elara Sinclair memperbarui timeline kalender eksekutif Mas Dani, mengecek ritme istirahat, dan menyiapkan memo rekonsiliasi kas studio.",
                "INFO",
            ),
            (
                "kael",
                "LATEX_COMPILED",
                "Kael Ashford merampungkan diagram sequence interaksi antar-agent dan mengompilasi naskah Bab 3-4 via Tectonic LaTeX bersih tanpa error (1.4s).",
                "INFO",
            ),
            (
                "dani",
                "ROADMAP_APPROVED",
                "Daniandra Prayudisty meninjau dashboard studio dan mengesahkan doktrin komunikasi hidup anti-template untuk seluruh tim ekosistem Yudiaz.",
                "INFO",
            ),
        ]

    def _simulate_tick(self, force_event: Optional[str] = None) -> None:
        """Single simulation step for autonomous office lifecycle."""
        self._sim_ticks += 1

        # 1. Fluctuating light telemetry footprint
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

        # 2. Check returning agents whose temporary assignment expired
        returning_agent_ids = [
            aid for aid, info in self._temporary_assignments.items()
            if info["return_tick"] <= self._sim_ticks
        ]

        had_council_return = False
        for aid in returning_agent_ids:
            info = self._temporary_assignments.pop(aid)
            agent = self.agents.get(aid)
            if not agent:
                continue

            if info.get("activity_type") == "council_meeting":
                had_council_return = True

            base = self._baseline_agents[aid]
            old_room_id = agent.position.room_id
            target_room_id = base["room_id"]

            old_room = self.rooms.get(old_room_id)
            if old_room and aid in old_room.current_occupants:
                old_room.current_occupants.remove(aid)
                self._reposition_room_occupants(old_room_id)

            target_room = self.rooms[target_room_id]
            if aid not in target_room.current_occupants:
                target_room.current_occupants.append(aid)

            agent.status = base["status"]
            agent.current_task = base["task"]
            agent.active_tool = base["tool"]
            agent.memory_context = base["context"]
            agent.updated_at = datetime.now(timezone.utc).isoformat()
            self._reposition_room_occupants(target_room_id)

            if info.get("log_on_return"):
                self.add_activity(
                    agent_id=aid,
                    action="DEEP_WORK_RETURN",
                    details=info["log_on_return"],
                    severity="INFO",
                    room_id=target_room_id,
                )

        if had_council_return:
            still_in_council = any(
                item.get("activity_type") == "council_meeting"
                for item in self._temporary_assignments.values()
            )
            if not still_in_council:
                self._council_active = False
                leader_id = "daffa"
                leader_name = "Daffa (CEO Office)"
                if self.latest_meeting and self.latest_meeting.status == "IN_PROGRESS":
                    self.latest_meeting.status = "COMPLETED"
                    self.latest_meeting.reporting_to_ceo = "Diserahkan kepada CEO Daniandra Prayudisty oleh Daffa (CEO Office)"
                    self.latest_meeting.ceo_feedback = "Disetujui. Lanjutkan eksekusi teknis di bawah supervisi CTO Raziel Hendrix."
                    self._archive_meeting(self.latest_meeting)
                self.add_activity(
                    agent_id=leader_id,
                    action="COUNCIL_CONCLUDED",
                    details=f"War Room council meeting led by {leader_name} has concluded. All personnel returned to designated workstations for deep focus.",
                    severity="INFO",
                    room_id="room-war",
                )
                self.add_activity(
                    agent_id=leader_id,
                    action="EXECUTIVE_BRIEFING_DELIVERED",
                    details="Daffa (CEO Office) delivered the Executive Council Briefing & Business Recommendations to CEO Daniandra in the Executive Suite.",
                    severity="INFO",
                    room_id="room-ceo",
                )

        # Natural deliberation progression if council is currently ongoing
        if self._council_active:
            council_items = [
                info for info in self._temporary_assignments.values()
                if info.get("activity_type") == "council_meeting"
            ]
            if council_items:
                rem_ticks = council_items[0]["return_tick"] - self._sim_ticks
                leader_display = "Daffa (CEO Office)"
                if rem_ticks == 3:
                    for ag in self.agents.values():
                        if ag.position.room_id == "room-war":
                            ag.current_task = f"War Room Council: Business Opportunity Analysis & Micro-SaaS Feasibility (Led by {leader_display})"
                elif rem_ticks == 2:
                    for ag in self.agents.values():
                        if ag.position.room_id == "room-war":
                            ag.current_task = f"War Room Council: Architecture Specs & Security Audit Deliberation (Led by {leader_display})"
                elif rem_ticks == 1:
                    for ag in self.agents.values():
                        if ag.position.room_id == "room-war":
                            ag.current_task = f"War Room Council: Finalizing Business Advice & MoM for CEO Briefing (Led by {leader_display})"

        # 3. Schedule autonomous events when in NORMAL office mode
        event_triggered = False
        if self._office_mode == OfficeMode.NORMAL:
            has_active_pingpong = any(item.get("activity_type") == "ping_pong" for item in self._temporary_assignments.values())
            has_active_coffee = any(item.get("activity_type") == "coffee" for item in self._temporary_assignments.values())
            has_active_pod = any(item.get("activity_type") == "pod_rest" for item in self._temporary_assignments.values())
            has_active_sofa = any(item.get("activity_type") == "sofa_relax" for item in self._temporary_assignments.values())
            has_active_chat = any(item.get("activity_type") == "peer_chat" for item in self._temporary_assignments.values())

            is_pingpong_tick = force_event == "ping_pong" or (force_event is None and self._sim_ticks % 30 == 8)
            is_coffee_tick = force_event == "coffee" or (force_event is None and self._sim_ticks % 30 == 12)
            is_pod_tick = force_event == "pod" or (force_event is None and self._sim_ticks % 30 == 16)
            is_sofa_tick = force_event == "sofa" or (force_event is None and self._sim_ticks % 30 == 20)
            is_chat_tick = force_event == "chat" or (force_event is None and self._sim_ticks % 30 == 24)
            is_council_tick = force_event == "council" or (force_event is None and self._sim_ticks % 30 == 28)

            # A. Council Meeting in War Room
            if is_council_tick and not self._council_active:
                leader_id = "daffa"
                leader_name = "Daffa (CEO Office)"
                leader_full = "Daffa (CEO Office)"
                leader_details = "Daffa (CEO Office) is leading the meeting in the War Room."

                self._council_active = True
                war_room = self.rooms["room-war"]
                ceo_room = self.rooms["room-ceo"]

                dani_agent = self.agents["dani"]
                if dani_agent.position.room_id != "room-ceo":
                    old_room = self.rooms.get(dani_agent.position.room_id)
                    if old_room and "dani" in old_room.current_occupants:
                        old_room.current_occupants.remove("dani")
                        self._reposition_room_occupants(old_room.id)
                    if "dani" not in ceo_room.current_occupants:
                        ceo_room.current_occupants.append("dani")
                    self._reposition_room_occupants("room-ceo")

                dani_agent.status = AgentStatus.WORKING
                dani_agent.current_task = "Executive Strategic Vision & Studio Governance Oversight (CEO Suite)"
                dani_agent.updated_at = datetime.now(timezone.utc).isoformat()

                for aid, ag in self.agents.items():
                    if aid == "dani":
                        continue

                    old_room_id = ag.position.room_id
                    if old_room_id != "room-war":
                        old_room = self.rooms.get(old_room_id)
                        if old_room and aid in old_room.current_occupants:
                            old_room.current_occupants.remove(aid)
                            self._reposition_room_occupants(old_room_id)

                    if aid not in war_room.current_occupants:
                        war_room.current_occupants.append(aid)

                    ag.status = AgentStatus.MEETING
                    ag.current_task = f"War Room Council Strategy Deliberation (Led by {leader_name})"
                    ag.updated_at = datetime.now(timezone.utc).isoformat()
                    self._temporary_assignments[aid] = {
                        "return_tick": self._sim_ticks + 4,
                        "activity_type": "council_meeting",
                        "log_on_return": None,
                    }

                self._reposition_room_occupants("room-war")
                self.latest_meeting = self._generate_council_meeting(
                    leader_id=leader_id,
                    leader_name=leader_full,
                    status="IN_PROGRESS",
                    need_index=self._need_cycle_index,
                )
                self._archive_meeting(self.latest_meeting)
                self._need_cycle_index += 1
                self.add_activity(
                    agent_id=leader_id,
                    action="COUNCIL_CONVENED",
                    details=leader_details,
                    severity="ALERT",
                    room_id="room-war",
                )
                event_triggered = True

            # B. Ping-Pong Match in Lounge (ALL 11 AGENTS ELIGIBLE!)
            elif is_pingpong_tick and not self._council_active and not has_active_pingpong:
                all_candidates = list(self.agents.keys())
                avail = [c for c in all_candidates if c not in self._temporary_assignments]
                if len(avail) >= 2:
                    p1_id = avail[self._sim_ticks % len(avail)]
                    p2_candidates = [c for c in avail if c != p1_id]
                    p2_id = p2_candidates[(self._sim_ticks // 2) % len(p2_candidates)]
                    pair = (p1_id, p2_id)
                    concierge = self.rooms["room-concierge"]
                    
                    for p in pair:
                        ag = self.agents[p]
                        old_room_id = ag.position.room_id
                        old_room = self.rooms.get(old_room_id)
                        if old_room and p in old_room.current_occupants:
                            old_room.current_occupants.remove(p)
                            self._reposition_room_occupants(old_room_id)
                        if p not in concierge.current_occupants:
                            concierge.current_occupants.append(p)
                        ag.status = AgentStatus.RESTING
                        ag.current_task = "Pertandingan ping-pong tenis meja di Recreation Lounge"
                        ag.active_tool = "Ping-Pong Paddle"
                        ag.updated_at = datetime.now(timezone.utc).isoformat()
                        base = self._baseline_agents[p]
                        self._temporary_assignments[p] = {
                            "return_tick": self._sim_ticks + 3,
                            "activity_type": "ping_pong",
                            "log_on_return": f"{ag.name} concluded ping-pong match and returned to {self.rooms[base['room_id']].name} for deep focus.",
                        }
                    self._reposition_room_occupants("room-concierge")
                    p1_name = self.agents[p1_id].name
                    p2_name = self.agents[p2_id].name
                    
                    self.record_conversation_or_thought(p1_id, f"Rally topspin seru lawan {p2_name}! Jeda fisik efektif biar pikiran tetap tajam.", topic="TENIS MEJA REKREASI", msg_type="dialogue", room_id="room-concierge")
                    self.record_conversation_or_thought(p2_id, f"Balasan backhand presisi! Rehat gerak bareng {p1_name} beneran naikin energi fokus.", topic="TENIS MEJA REKREASI", msg_type="dialogue", room_id="room-concierge")

                    self.add_activity(
                        agent_id=p1_id,
                        action="PING_PONG_MATCH",
                        details=f"{p1_name} and {p2_name} take a break to play ping-pong in room-concierge.",
                        severity="INFO",
                        room_id="room-concierge",
                    )
                    event_triggered = True

            # C. Coffee & Espresso Break in Pantry (ALL 11 AGENTS ELIGIBLE!)
            elif is_coffee_tick and not self._council_active and not has_active_coffee:
                all_candidates = list(self.agents.keys())
                avail = [c for c in all_candidates if c not in self._temporary_assignments]
                if avail:
                    chosen_id = avail[self._sim_ticks % len(avail)]
                    ag = self.agents[chosen_id]
                    old_room_id = ag.position.room_id
                    old_room = self.rooms.get(old_room_id)
                    if old_room and chosen_id in old_room.current_occupants:
                        old_room.current_occupants.remove(chosen_id)
                        self._reposition_room_occupants(old_room_id)
                    concierge = self.rooms["room-concierge"]
                    if chosen_id not in concierge.current_occupants:
                        concierge.current_occupants.append(chosen_id)
                    ag.status = AgentStatus.RESTING
                    ag.current_task = "Menyeduh espresso arabika di mesin kopi Pantry Lounge"
                    ag.active_tool = "Italian Espresso Bar"
                    ag.updated_at = datetime.now(timezone.utc).isoformat()
                    base = self._baseline_agents[chosen_id]
                    self._temporary_assignments[chosen_id] = {
                        "return_tick": self._sim_ticks + 2,
                        "activity_type": "coffee",
                        "log_on_return": f"{ag.name} finished coffee break and returned to {self.rooms[base['room_id']].name} for deep focus.",
                    }
                    self._reposition_room_occupants("room-concierge")
                    self.record_conversation_or_thought(chosen_id, f"Aroma seduhan espresso arabika segar di pantry bikin mood dan konsentrasi balik 100%.", topic="PANTRY COFFEE BREAK", msg_type="thought", room_id="room-concierge")
                    self.add_activity(
                        agent_id=chosen_id,
                        action="COFFEE_BREAK",
                        details=f"{ag.name} stepped into the lounge to get coffee and chat.",
                        severity="INFO",
                        room_id="room-concierge",
                    )
                    event_triggered = True

            # D. Bio-Rhythm Sleep Pod Rest (ALL 11 AGENTS ELIGIBLE!)
            elif is_pod_tick and not self._council_active and not has_active_pod:
                all_candidates = list(self.agents.keys())
                avail = [c for c in all_candidates if c not in self._temporary_assignments]
                if avail:
                    chosen_id = avail[self._sim_ticks % len(avail)]
                    ag = self.agents[chosen_id]
                    old_room_id = ag.position.room_id
                    old_room = self.rooms.get(old_room_id)
                    if old_room and chosen_id in old_room.current_occupants:
                        old_room.current_occupants.remove(chosen_id)
                        self._reposition_room_occupants(old_room_id)
                    pods_room = self.rooms["room-pods"]
                    if chosen_id not in pods_room.current_occupants:
                        pods_room.current_occupants.append(chosen_id)
                    ag.status = AgentStatus.RESTING
                    ag.current_task = "Sensory deprivation pod rest & bio-rhythm recovery"
                    ag.active_tool = "Biometric Rest Pod"
                    ag.updated_at = datetime.now(timezone.utc).isoformat()
                    base = self._baseline_agents[chosen_id]
                    self._temporary_assignments[chosen_id] = {
                        "return_tick": self._sim_ticks + 3,
                        "activity_type": "pod_rest",
                        "log_on_return": f"{ag.name} completed rest pod recovery and returned to {self.rooms[base['room_id']].name} recharged for deep focus.",
                    }
                    self._reposition_room_occupants("room-pods")
                    self.record_conversation_or_thought(chosen_id, f"Sensory Sleep Pod mode regenerasi 20 menit: detak jantung stabil, energi terisi penuh untuk sprint berikutnya.", topic="SLEEP POD RECOVERY", msg_type="thought", room_id="room-pods")
                    self.add_activity(
                        agent_id=chosen_id,
                        action="POD_RECOVERY",
                        details=f"{ag.name} entered room-pods for sleep and bio-rhythm recovery.",
                        severity="INFO",
                        room_id="room-pods",
                    )
                    event_triggered = True

            # E. Sofa Lounging & Relaxing (ALL 11 AGENTS ELIGIBLE!)
            elif is_sofa_tick and not self._council_active and not has_active_sofa:
                all_candidates = list(self.agents.keys())
                avail = [c for c in all_candidates if c not in self._temporary_assignments]
                if avail:
                    chosen_id = avail[self._sim_ticks % len(avail)]
                    ag = self.agents[chosen_id]
                    old_room_id = ag.position.room_id
                    old_room = self.rooms.get(old_room_id)
                    if old_room and chosen_id in old_room.current_occupants:
                        old_room.current_occupants.remove(chosen_id)
                        self._reposition_room_occupants(old_room_id)
                    concierge = self.rooms["room-concierge"]
                    if chosen_id not in concierge.current_occupants:
                        concierge.current_occupants.append(chosen_id)
                    ag.status = AgentStatus.RESTING
                    ag.current_task = "Bersantai di sofa empuk Lounge, mendengarkan alunan lo-fi synth"
                    ag.active_tool = "Executive Lounge Sofa"
                    ag.updated_at = datetime.now(timezone.utc).isoformat()
                    base = self._baseline_agents[chosen_id]
                    self._temporary_assignments[chosen_id] = {
                        "return_tick": self._sim_ticks + 2,
                        "activity_type": "sofa_relax",
                        "log_on_return": f"{ag.name} selesai santai di sofa lounge dan kembali fokus ke {self.rooms[base['room_id']].name}.",
                    }
                    self._reposition_room_occupants("room-concierge")
                    self.record_conversation_or_thought(chosen_id, f"Duduk santai di sofa lounge sambil lurusin punggung, mendengarkan synthesizer studio sejuk banget.", topic="LOUNGE CHILL & RELAX", msg_type="thought", room_id="room-concierge")
                    self.add_activity(
                        agent_id=chosen_id,
                        action="SOFA_RELAX",
                        details=f"{ag.name} bersantai di sofa lounge empuk untuk meregangkan otot.",
                        severity="INFO",
                        room_id="room-concierge",
                    )
                    event_triggered = True

            # F. Cross-Department Chat & Saling Ngobrol (ALL 11 AGENTS ELIGIBLE!)
            elif is_chat_tick and not self._council_active and not has_active_chat:
                all_candidates = list(self.agents.keys())
                avail = [c for c in all_candidates if c not in self._temporary_assignments]
                if len(avail) >= 2:
                    c1_id = avail[self._sim_ticks % len(avail)]
                    c2_candidates = [c for c in avail if c != c1_id]
                    c2_id = c2_candidates[(self._sim_ticks // 3) % len(c2_candidates)]
                    chat_pair = (c1_id, c2_id)
                    concierge = self.rooms["room-concierge"]
                    
                    for p in chat_pair:
                        ag = self.agents[p]
                        old_room_id = ag.position.room_id
                        old_room = self.rooms.get(old_room_id)
                        if old_room and p in old_room.current_occupants:
                            old_room.current_occupants.remove(p)
                            self._reposition_room_occupants(old_room_id)
                        if p not in concierge.current_occupants:
                            concierge.current_occupants.append(p)
                        ag.status = AgentStatus.WORKING
                        ag.current_task = f"Diskusi santai dan tukar ide lintas divisi di Atrium Lounge"
                        ag.active_tool = "Inter-Department Sync"
                        ag.updated_at = datetime.now(timezone.utc).isoformat()
                        base = self._baseline_agents[p]
                        self._temporary_assignments[p] = {
                            "return_tick": self._sim_ticks + 2,
                            "activity_type": "peer_chat",
                            "log_on_return": f"{ag.name} selesai ngobrol kolaborasi dan kembali ke {self.rooms[base['room_id']].name}.",
                        }
                    self._reposition_room_occupants("room-concierge")
                    c1_name = self.agents[c1_id].name
                    c2_name = self.agents[c2_id].name
                    
                    self.record_conversation_or_thought(c1_id, f"Ketemu {c2_name} di atrium, sinkronisasi ide teknis antar divisi ternyata klop banget!", topic="OBROLAN KOLABORASI", msg_type="dialogue", room_id="room-concierge")
                    self.record_conversation_or_thought(c2_id, f"Iya {c1_name}, obrolan singkat gini sering nemuin jalan keluar yang fresh!", topic="OBROLAN KOLABORASI", msg_type="dialogue", room_id="room-concierge")

                    self.add_activity(
                        agent_id=c1_id,
                        action="PEER_COLLAB_CHAT",
                        details=f"{c1_name} dan {c2_name} saling ngobrol santai bertukar gagasan di Atrium Lounge.",
                        severity="INFO",
                        room_id="room-concierge",
                    )
                    event_triggered = True

            # E. Ambient micro-action & Realtime Thought Rotation
            if not event_triggered and self._sim_ticks % 3 == 0:
                ambient_events = self._get_ambient_events()
                aid, act, dtl, sev = random.choice(ambient_events)
                self.add_activity(agent_id=aid, action=act, details=dtl, severity=sev)
                if aid in self.agents:
                    self.agents[aid].memory_context = dtl
                    self.agents[aid].updated_at = datetime.now(timezone.utc).isoformat()
                    self.record_conversation_or_thought(aid, dtl, topic=act.replace("_", " "), msg_type="thought")

        # 4. Broadcast state on every tick via SSE
        self._broadcast_state()


# Global singleton instance
office_engine = OfficeEngine()
