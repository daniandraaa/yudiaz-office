"""Spatial simulation engine and real-time state machine for Yudiaz Virtual HQ.

Manages 11 autonomous agents across 11 cyber-luxury zones, coordinates spatial
positioning, mode switches (War Room, Deep Work, Rest Cycle), audit logging,
and live telemetry streaming.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import random
import secrets
import time
from typing import Any, Optional
import uuid

from backend.config import get_settings
from backend.models import (
    ActionItem,
    ActivityLog,
    AgentInfo,
    AgentPosition,
    AgentStatus,
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

        self._initialize_rooms()
        self._initialize_agents()
        self._seed_initial_activity()
        self.latest_meeting = self._generate_council_meeting(
            leader_id="daffa",
            leader_name="Daffa (CEO Office)",
            status="COMPLETED",
            need_index=0,
        )

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
                "context": "Yudiaz Creative Studio governance & executive roadmaps",
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
                "role": "Personal Assistant to CEO",
                "department": "Executive Support",
                "room_id": "room-pa",
                "status": AgentStatus.WORKING,
                "task": "Executive Calendar, Briefing Prep & Priority Logistics",
                "avatar_color": "#E0AAFF",
                "tool": "Executive Calendar & Briefing Suite",
                "context": "Daniandra's itinerary, executive briefings, priority logistics, and VIP communications",
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
            details="Yudiaz Virtual HQ Spatial Engine booted. 11 autonomous agents deployed across 11 zones.",
            severity="SYSTEM",
        )
        self.add_activity(
            agent_id="dani",
            action="OFFICE_INSPECTION",
            details="Daniandra Prayudisty initiated daily studio oversight from the Executive Suite.",
            severity="INFO",
        )

    def _generate_council_meeting(
        self,
        leader_id: str = "daffa",
        leader_name: Optional[str] = None,
        status: str = "IN_PROGRESS",
        need_index: Optional[int] = 0,
    ) -> MeetingMinutes:
        """Construct realistic, rich Minutes of Meeting (MoM) record for War Room council.

        Council meetings are ALWAYS led by Daffa (CEO Office). Daniandra (CEO) does not attend
        the operational War Room and stays in the CEO Suite reviewing strategic vision.
        Agendas analyze studio business opportunities and technical needs.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        meeting_id = f"mom-council-{int(time.time())}-{secrets.token_hex(3)}"
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
        ]

        active_need_idx = (need_index or 0) % 3

        if active_need_idx == 0:
            # Need 1: Business Suggestion / Micro-SaaS AI Automation
            title = "Analisis Peluang Bisnis Micro-SaaS AI Automation via Dynamic QRIS & API 9Router untuk UMKM/Devs"
            dialogues = [
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Selamat pagi rekan-rekan. Sesuai arahan CEO Daniandra, saya memimpin council operasional hari ini. Fokus utama kita: Analisis Peluang Bisnis Micro-SaaS AI Automation via Dynamic QRIS & API 9Router untuk pasar UMKM dan pengembang software. Raziel, bagaimana kelayakan teknis dan arsitektur solusinya?",
                ),
                MeetingDialogue(
                    speaker_id="raziel",
                    speaker_name="Raziel Hendrix",
                    role="CTO & Lead Orchestrator",
                    text="Dari evaluasi teknis, integrasi API 9Router memungkinkan dynamic multi-model routing berlatensi rendah dengan efisiensi biaya inferensi hingga 60%. Settlement otomatis via Dynamic QRIS memberikan konversi instan tanpa friksi manual.",
                ),
                MeetingDialogue(
                    speaker_id="kael",
                    speaker_name="Kael Ashford",
                    role="Lead Architect",
                    text="Arsitektur micro-SaaS ini dirancang modular: webhook mutasi QRIS asinkron, proxy gateway inferensi 9Router, dan token quota limiter. Desain event-driven ini sekaligus memvalidasi arsitektur tugas akhir yang scalable.",
                ),
                MeetingDialogue(
                    speaker_id="nara",
                    speaker_name="Nara Vasquez",
                    role="Lead Researcher",
                    text="Riset pasar menunjukkan lebih dari 78% UMKM digital di Indonesia mencari solusi AI customer support dan billing WhatsApp yang terjangkau. Analisis data empiris membuktikan ruang penetrasi pasar kita sangat luas.",
                ),
                MeetingDialogue(
                    speaker_id="idris",
                    speaker_name="Idris Nakamura",
                    role="Senior Developer",
                    text="Backend FastAPI siap mengeksekusi integrasi ini. Endpoint webhook verifikasi signature QRIS dan router 9Router client dapat kita selesaikan dalam 1 sprint dengan strictly-typed Pydantic contracts.",
                ),
                MeetingDialogue(
                    speaker_id="mika",
                    speaker_name="Mika Stellan",
                    role="Frontend Engineer",
                    text="Kami siapkan antarmuka merchant portal cyber-luxury yang ringkas dan intuitif untuk setup API key dan pemantauan transaksi QRIS secara real-time di Next.js canvas.",
                ),
                MeetingDialogue(
                    speaker_id="senna",
                    speaker_name="Senna Louviere",
                    role="Creative Director",
                    text="Landing page produk micro-SaaS dirancang dengan cyber-luxury tokens, tipografi tajam, dan aksen emerald QRIS untuk menumbuhkan rasa percaya enterprise-grade bagi merchant UMKM.",
                ),
                MeetingDialogue(
                    speaker_id="viktor",
                    speaker_name="Viktor Moreau",
                    role="Lead QA & Security Engineer",
                    text="Keamanan transaksi moneter dan data pelanggan terproteksi Sentinel 4-layer defense. Validasi HMAC-SHA256 pada payload QRIS dan rate-limiting ketat memastikan zero-leakage.",
                ),
                MeetingDialogue(
                    speaker_id="jovan",
                    speaker_name="Jovan Aritza",
                    role="Intelligence Officer",
                    text="Monitoring kompetitor: biaya langganan AI bot di pasar saat ini relatif mahal dan kaku. Dengan 9Router dan pembayaran QRIS langsung, skema harga pay-per-use kita akan sangat atraktif bagi UMKM.",
                ),
                MeetingDialogue(
                    speaker_id="elara",
                    speaker_name="Elara Sinclair",
                    role="Personal Assistant to CEO",
                    text="Seluruh notulensi analisis peluang bisnis, estimasi proyeksi revenue, dan diagram teknis telah terangkum rapi. Berkas siap diserahkan kepada Daffa untuk dilaporkan langsung ke CEO Daniandra di ruang CEO.",
                ),
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Terima kasih seluruh tim atas analisis bisnis dan kesiapan teknis yang solid. Rapat council resmi ditutup. Saya akan segera menuju CEO Suite untuk menyerahkan Executive Council Briefing & Rekomendasi Bisnis ini kepada CEO Daniandra.",
                ),
            ]
            key_decisions = [
                "Pengembangan produk Micro-SaaS AI Automation studio terintegrasi Dynamic QRIS dan API 9Router untuk efisiensi biaya routing LLM hingga 60%.",
                "Penerapan model bisnis pay-per-use berbasis API token yang kompetitif dan ramah bagi pelaku UMKM serta pengembang software.",
                "Hardening endpoint payment webhook QRIS menggunakan validasi HMAC-SHA256 berarsitektur event-driven asinkron.",
                "Penyusunan rekomendasi bisnis resmi oleh CEO Office untuk persetujuan eksekutif CEO Daniandra Prayudisty.",
            ]
            action_items = [
                ActionItem(
                    pic="Idris Nakamura & Kael Ashford",
                    task="Implementasi prototipe backend Micro-SaaS AI automation dan integrasi client 9Router",
                    due="2026-10-02",
                ),
                ActionItem(
                    pic="Nara Vasquez & Jovan Aritza",
                    task="Riset pasar adopsi otomasi UMKM dan benchmark pricing SaaS kompetitor",
                    due="2026-10-01",
                ),
                ActionItem(
                    pic="Mika Stellan & Senna Louviere",
                    task="Desain visual landing page cyber-luxury dan UI portal merchant QRIS",
                    due="2026-10-03",
                ),
                ActionItem(
                    pic="Viktor Moreau",
                    task="Pengujian penetrasi keamanan webhook payment gateway dan automated test suite",
                    due="2026-10-04",
                ),
                ActionItem(
                    pic="Daffa",
                    task="Penyampaian Executive Council Briefing & Rekomendasi Bisnis kepada CEO Daniandra di CEO Suite",
                    due="2026-09-28",
                ),
            ]
        elif active_need_idx == 1:
            # Need 2: R&D / Thesis Architecture Telkom University Bab 3 & 4
            title = "Evaluasi Arsitektur Skripsi Telkom University Bab 3 & 4 (Event-Driven Autonomous Multi-Agent Systems)"
            dialogues = [
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Selamat pagi rekan-rekan. Mewakili CEO Office, saya memimpin council hari ini untuk Evaluasi Arsitektur Skripsi Telkom University Bab 3 & 4 mengenai Event-Driven Autonomous Multi-Agent Systems. Raziel dan Kael, bagaimana evaluasi pemodelan sistem dan noveltinya?",
                ),
                MeetingDialogue(
                    speaker_id="raziel",
                    speaker_name="Raziel Hendrix",
                    role="CTO & Lead Orchestrator",
                    text="Evaluasi arsitektur membuktikan novelti kita pada koordinasi spatial dan asynchronous event loop sangat kokoh. Benchmark throughput telemetry dan latency broadcast di Virtual HQ menjadi data empiris valid untuk Bab 4.",
                ),
                MeetingDialogue(
                    speaker_id="kael",
                    speaker_name="Kael Ashford",
                    role="Lead Architect",
                    text="Bab 3 Metodologi dan Bab 4 Hasil & Pembahasan telah disinkronkan dengan template LaTeX resmi Telkom University. Diagram state machine, sequence diagram, dan spesifikasi formal koordinasi agen tuntas dikompilasi dengan Tectonic.",
                ),
                MeetingDialogue(
                    speaker_id="nara",
                    speaker_name="Nara Vasquez",
                    role="Lead Researcher",
                    text="Literature review Bab 2 dan Bab 3 telah diperkaya dengan 30 sitasi IEEE/ACM terbaru. Evaluasi komparatif antara hierarchical dan emergent agent coordination membuktikan keunggulan efisiensi arsitektur kita.",
                ),
                MeetingDialogue(
                    speaker_id="idris",
                    speaker_name="Idris Nakamura",
                    role="Senior Developer",
                    text="Test bed eksperimen pada endpoint API dan modul simulasi backend telah menghasilkan dataset log metrik yang siap diproyeksikan ke dalam grafik Bab 4. Seluruh kode pengujian lulus 100% di pytest.",
                ),
                MeetingDialogue(
                    speaker_id="mika",
                    speaker_name="Mika Stellan",
                    role="Frontend Engineer",
                    text="Visualisasi diagram arsitektur dan hasil simulasi interaksi agen di canvas 2.5D telah diekspor dalam format vektor resolusi tinggi sesuai standar publikasi ilmiah Tel-U.",
                ),
                MeetingDialogue(
                    speaker_id="senna",
                    speaker_name="Senna Louviere",
                    role="Creative Director",
                    text="Layout tipografi LaTeX, penomoran tabel, grafik performa, dan skema visual monokrom akademik sudah dipoles sesuai panduan naskah tugas akhir Informatika Tel-U.",
                ),
                MeetingDialogue(
                    speaker_id="viktor",
                    speaker_name="Viktor Moreau",
                    role="Lead QA & Security Engineer",
                    text="Validasi eksperimen diuji dengan skenario beban tinggi untuk memastikan tidak ada fluktuasi bias pada data Bab 4. Metrik latensi p99 terbukti stabil di bawah 5 milidetik.",
                ),
                MeetingDialogue(
                    speaker_id="jovan",
                    speaker_name="Jovan Aritza",
                    role="Intelligence Officer",
                    text="Pantauan radar Telkom University: jadwal pendaftaran sidang pra-skripsi dan verifikasi berkas sudah dibuka. Kita berada di timeline yang sangat aman untuk submit draf Bab 1-4 pekan ini.",
                ),
                MeetingDialogue(
                    speaker_id="elara",
                    speaker_name="Elara Sinclair",
                    role="Personal Assistant to CEO",
                    text="Executive brief evaluasi skripsi dan draf kompilasi PDF Bab 3 & 4 sudah saya cetak dan siapkan di tablet eksekutif. Dokumen siap diteruskan ke CEO Daniandra untuk arahan strategis final.",
                ),
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Terima kasih atas kerja keras seluruh tim R&D dan arsitektur. Rapat council selesai. Saya akan langsung membawa berkas evaluasi skripsi ini dan melapor ke CEO Daniandra di ruang CEO.",
                ),
            ]
            key_decisions = [
                "Standarisasi naskah skripsi Telkom University Bab 3 & 4 menggunakan format template LaTeX resmi dengan compiler Tectonic.",
                "Pemanfaatan data empiris latensi dan throughput Virtual HQ sebagai novelti arsitektur event-driven autonomous multi-agent systems.",
                "Sinkronisasi berkala timeline akademik dan pendaftaran sidang pra-skripsi Tel-U melalui koordinasi CEO Office dan PA.",
            ]
            action_items = [
                ActionItem(
                    pic="Kael Ashford & Nara Vasquez",
                    task="Finalisasi draf Bab 3 & Bab 4 Skripsi Telkom University dalam format LaTeX Tectonic",
                    due="2026-10-02",
                ),
                ActionItem(
                    pic="Idris Nakamura",
                    task="Ekspor dataset pengujian telemetry engine untuk validasi grafik Bab 4",
                    due="2026-09-30",
                ),
                ActionItem(
                    pic="Mika Stellan & Senna Louviere",
                    task="Visualisasi diagram arsitektur resolusi tinggi untuk naskah skripsi",
                    due="2026-09-29",
                ),
                ActionItem(
                    pic="Jovan Aritza & Elara Sinclair",
                    task="Monitoring portal akademik Tel-U terkait jadwal pendaftaran sidang pra-skripsi",
                    due="2026-10-01",
                ),
                ActionItem(
                    pic="Daffa",
                    task="Laporan kemajuan evaluasi skripsi kepada CEO Daniandra di Executive Suite",
                    due="2026-09-28",
                ),
            ]
        else:
            # Need 3: DevOps / Security Sentinel & Server Observability
            title = "Audit Keamanan Sentinel & Peningkatan Kapasitas Observabilitas Server Studio"
            dialogues = [
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Selamat pagi rekan-rekan. Rapat War Room council pagi ini kita dedikasikan untuk Audit Keamanan Sentinel & Peningkatan Kapasitas Observabilitas Server Studio. Viktor dan Raziel, silakan laporkan status audit ketahanan infrastruktur kita.",
                ),
                MeetingDialogue(
                    speaker_id="viktor",
                    speaker_name="Viktor Moreau",
                    role="Lead QA & Security Engineer",
                    text="Audit Sentinel 4-layer defense selesai dijalankan: scanning port 9449, rate-limiting brute-force guard pada PIN auth, dan audit dependensi bebas CVE critical. Seluruh parameter berstatus zero vulnerability.",
                ),
                MeetingDialogue(
                    speaker_id="raziel",
                    speaker_name="Raziel Hendrix",
                    role="CTO & Lead Orchestrator",
                    text="Dari sisi cloud infrastructure, pipeline SSE broadcaster dan telemetry mesh beroperasi dengan CPU di bawah 15%. Kapasitas observabilitas server telah diperkuat dengan structured logging dan metric export.",
                ),
                MeetingDialogue(
                    speaker_id="kael",
                    speaker_name="Kael Ashford",
                    role="Lead Architect",
                    text="Arsitektur keamanan Sentinel telah kami formalisasi dalam Security Architecture Specification: TLS 1.3 termination di reverse proxy Caddy, strict CORS, dan memory state isolation di simulation loop.",
                ),
                MeetingDialogue(
                    speaker_id="idris",
                    speaker_name="Idris Nakamura",
                    role="Senior Developer",
                    text="Backend telah dilengkapi rate-limiting middleware per endpoint, validasi input Pydantic yang ketat, serta sanitasi log audit untuk mencegah kebocoran data sensitif.",
                ),
                MeetingDialogue(
                    speaker_id="nara",
                    speaker_name="Nara Vasquez",
                    role="Lead Researcher",
                    text="Rujukan standar keamanan CIS Benchmark dan OWASP API Security Top 10 telah kami jadikan panduan pengamanan berkelanjutan seluruh service studio.",
                ),
                MeetingDialogue(
                    speaker_id="mika",
                    speaker_name="Mika Stellan",
                    role="Frontend Engineer",
                    text="Dashboard telemetry pada frontend diorama kini dilengkapi visual warning alert dan indikator latency real-time jika terjadi fluktuasi beban server.",
                ),
                MeetingDialogue(
                    speaker_id="senna",
                    speaker_name="Senna Louviere",
                    role="Creative Director",
                    text="Desain visual status Sentinel audit, badge enkripsi, dan visual panel telemetry telah diselaraskan dengan aksen cyber-emerald obsidian.",
                ),
                MeetingDialogue(
                    speaker_id="jovan",
                    speaker_name="Jovan Aritza",
                    role="Intelligence Officer",
                    text="Monitoring perimeter NOC mengonfirmasi tidak ada aktivitas scan atau anomali jaringan mencurigakan yang mendekati server studio.",
                ),
                MeetingDialogue(
                    speaker_id="elara",
                    speaker_name="Elara Sinclair",
                    role="Personal Assistant to CEO",
                    text="Laporan ringkas hasil audit keamanan Sentinel dan observabilitas server telah dirangkum dalam one-page executive summary untuk briefing pimpinan.",
                ),
                MeetingDialogue(
                    speaker_id="daffa",
                    speaker_name="Daffa",
                    role="CEO Office",
                    text="Sempurna, infrastruktur yang aman dan teruji adalah pondasi utama ekspansi produk kita. Council selesai. Saya segera menghadap CEO Daniandra di CEO Suite untuk menyerahkan laporan ini.",
                ),
            ]
            key_decisions = [
                "Penerapan Sentinel 4-layer defense dan zero-vulnerability security baseline pada seluruh endpoint API.",
                "Peningkatan kapasitas observabilitas server studio dengan structured audit logging dan metrik telemetri real-time.",
                "Penerapan automated security scanning terintegrasi pada pipeline deployment studio.",
            ]
            action_items = [
                ActionItem(
                    pic="Viktor Moreau",
                    task="Audit keamanan Sentinel pada port 9449 dan verifikasi proteksi brute-force PIN",
                    due="2026-09-30",
                ),
                ActionItem(
                    pic="Raziel Hendrix & Idris Nakamura",
                    task="Peningkatan kapasitas observabilitas server dan optimasi pool broadcaster SSE",
                    due="2026-10-01",
                ),
                ActionItem(
                    pic="Kael Ashford & Nara Vasquez",
                    task="Penyusunan Security Architecture Specification dan kepatuhan CIS Benchmark",
                    due="2026-10-02",
                ),
                ActionItem(
                    pic="Daffa",
                    task="Penyampaian laporan audit keamanan dan observabilitas server ke CEO Daniandra",
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
            latest_meeting=self.latest_meeting,
        )

    def get_latest_meeting(self) -> Optional[MeetingMinutes]:
        """Fetch latest War Room council meeting minutes."""
        return self.latest_meeting

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
                "Raziel Hendrix verified telemetry heartbeats across all 11 worker subagents from the CTO Executive Suite.",
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
                "Elara Sinclair completed executive briefing prep & priority calendar logistics.",
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

            is_council_tick = force_event == "council" or (force_event is None and self._sim_ticks % 20 == 18)
            is_pingpong_tick = force_event == "ping_pong" or (force_event is None and self._sim_ticks % 20 == 9)
            is_coffee_tick = force_event == "coffee" or (force_event is None and self._sim_ticks % 20 == 12)
            is_pod_tick = force_event == "pod" or (force_event is None and self._sim_ticks % 20 == 15)

            # A. Council Meeting in War Room
            if is_council_tick and not self._council_active:
                leader_id = "daffa"
                leader_name = "Daffa (CEO Office)"
                leader_full = "Daffa (CEO Office)"
                leader_details = "Daffa (CEO Office) is leading the meeting in the War Room."

                self._council_active = True
                war_room = self.rooms["room-war"]
                ceo_room = self.rooms["room-ceo"]

                # CEO Daniandra stays in room-ceo reviewing strategic vision
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

                # Move 10 operational personnel to War Room
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
                self._need_cycle_index += 1
                self.add_activity(
                    agent_id=leader_id,
                    action="COUNCIL_CONVENED",
                    details=leader_details,
                    severity="ALERT",
                    room_id="room-war",
                )
                event_triggered = True

            # B. Ping-Pong Break in Lounge
            elif is_pingpong_tick and not self._council_active and not has_active_pingpong:
                pair_idx = (self._sim_ticks // 16) % 2
                pairs = [("idris", "mika"), ("jovan", "viktor")]
                pair = pairs[pair_idx]
                if all(p not in self._temporary_assignments for p in pair):
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
                        ag.current_task = "Ping-pong table match in Lounge"
                        ag.active_tool = "Ping-Pong Paddle"
                        ag.updated_at = datetime.now(timezone.utc).isoformat()
                        base = self._baseline_agents[p]
                        self._temporary_assignments[p] = {
                            "return_tick": self._sim_ticks + 3,
                            "activity_type": "ping_pong",
                            "log_on_return": f"{ag.name} concluded ping-pong match and returned to {self.rooms[base['room_id']].name} for deep focus.",
                        }
                    self._reposition_room_occupants("room-concierge")
                    p1_name = self.agents[pair[0]].name
                    p2_name = self.agents[pair[1]].name
                    self.add_activity(
                        agent_id=pair[0],
                        action="PING_PONG_MATCH",
                        details=f"{p1_name} and {p2_name} take a break to play ping-pong in room-concierge.",
                        severity="INFO",
                        room_id="room-concierge",
                    )
                    event_triggered = True

            # C. Coffee / Lounge Chat
            elif is_coffee_tick and not self._council_active and not has_active_coffee:
                candidates = ["elara", "senna", "kael", "nara", "raziel"]
                available = [c for c in candidates if c not in self._temporary_assignments]
                if available:
                    chosen_id = available[self._sim_ticks % len(available)]
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
                    ag.current_task = "Coffee break & casual executive chat"
                    ag.active_tool = "Italian Espresso Bar"
                    ag.updated_at = datetime.now(timezone.utc).isoformat()
                    base = self._baseline_agents[chosen_id]
                    self._temporary_assignments[chosen_id] = {
                        "return_tick": self._sim_ticks + 2,
                        "activity_type": "coffee",
                        "log_on_return": f"{ag.name} finished coffee break and returned to {self.rooms[base['room_id']].name} for deep focus.",
                    }
                    self._reposition_room_occupants("room-concierge")
                    self.add_activity(
                        agent_id=chosen_id,
                        action="COFFEE_BREAK",
                        details=f"{ag.name} stepped into the lounge to get coffee and chat.",
                        severity="INFO",
                        room_id="room-concierge",
                    )
                    event_triggered = True

            # D. Rest Pod Sleep/Recovery
            elif is_pod_tick and not self._council_active and not has_active_pod:
                candidates = ["viktor", "jovan", "nara", "kael", "idris"]
                available = [c for c in candidates if c not in self._temporary_assignments]
                if available:
                    chosen_id = available[self._sim_ticks % len(available)]
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
                    self.add_activity(
                        agent_id=chosen_id,
                        action="POD_RECOVERY",
                        details=f"{ag.name} entered room-pods for sleep and bio-rhythm recovery.",
                        severity="INFO",
                        room_id="room-pods",
                    )
                    event_triggered = True

            # E. Ambient micro-action
            if not event_triggered and self._sim_ticks % 3 == 0:
                ambient_events = self._get_ambient_events()
                aid, act, dtl, sev = random.choice(ambient_events)
                self.add_activity(agent_id=aid, action=act, details=dtl, severity=sev)

        # 4. Broadcast state on every tick via SSE
        self._broadcast_state()


# Global singleton instance
office_engine = OfficeEngine()
