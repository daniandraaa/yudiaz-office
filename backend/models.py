"""Data models and schemas for Yudiaz Virtual HQ 3D Building Diorama.

Defines Pydantic representations for agents, 3D building diorama rooms
(CEO Suite, CTO Executive Suite, Executive Assistant Office, Conference Room,
Workstations, Research Library, Creative Studio, Radar NOC, Lounge & Ping-Pong,
Bedroom & Rest Pods, Server Room),
activities, telemetry, operating modes, and API request/response contracts.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator


class AgentStatus(str, Enum):
    """Execution state of an autonomous agent."""

    WORKING = "WORKING"
    MEETING = "MEETING"
    RESTING = "RESTING"
    SLEEPING = "SLEEPING"
    STANDBY = "STANDBY"
    RESEARCHING = "RESEARCHING"


class OfficeMode(str, Enum):
    """High-level operating mode of the headquarters."""

    NORMAL = "NORMAL"
    WAR_ROOM = "WAR_ROOM"
    REST_CYCLE = "REST_CYCLE"
    RECREATION = "RECREATION"


class AgentPosition(BaseModel):
    """Spatial coordinate and room assignment for an agent."""

    x: float = Field(..., description="X-coordinate in virtual 2.5D space")
    y: float = Field(..., description="Y-coordinate in virtual 2.5D space")
    z: float = Field(default=0.0, description="Z-coordinate or elevation level")
    room_id: str = Field(..., description="Identifier of the room currently occupied")


class AgentInfo(BaseModel):
    """Complete profile, status, telemetry, and spatial state of an agent."""

    id: str = Field(..., description="Unique agent identifier (e.g. 'dani', 'raziel')")
    name: str = Field(..., description="Full name of personnel / agent")
    role: str = Field(..., description="Organizational title and specialty")
    department: str = Field(..., description="Department or studio division")
    avatar_color: str = Field(default="#00FFCC", description="Hex accent color code")
    status: AgentStatus = Field(default=AgentStatus.STANDBY, description="Current status")
    position: AgentPosition = Field(..., description="Current spatial location")
    current_task: str = Field(..., description="Active task or objective description")
    active_tool: Optional[str] = Field(default=None, description="Currently invoked tool or system")
    memory_context: Optional[str] = Field(
        default=None, description="Short summary of active context / memory stream"
    )
    cpu_footprint: float = Field(default=0.0, description="Simulated compute footprint in %")
    ram_footprint: float = Field(default=0.0, description="Simulated memory footprint in MB")
    updated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp of last state change",
    )


class RoomInfo(BaseModel):
    """Physical zone or chamber definition in the virtual studio 3D building diorama.

    Supports building diorama rooms: CEO Suite, CTO Executive Suite,
    Executive Assistant Office, Conference Room, Workstations,
    Research Library, Creative Studio, Radar NOC, Lounge & Ping-Pong,
    Bedroom & Rest Pods, Server Room.
    """

    id: str = Field(..., description="Unique room identifier (e.g. 'room-ceo')")
    name: str = Field(
        ...,
        description="Display name of the room (e.g. 'CEO Suite', 'Conference Room', 'Workstations', 'Research Library', 'Creative Studio', 'Radar NOC', 'Lounge & Ping-Pong', 'Bedroom & Rest Pods', 'Server Room')",
    )
    category: str = Field(..., description="Room classification (e.g. 'Executive', 'Engineering')")
    capacity: int = Field(default=4, description="Maximum agent seating capacity")
    floor: int = Field(default=1, description="Floor level in 3D building diorama")
    dimensions: tuple[float, float] = Field(
        default=(540.0, 290.0),
        description="Physical dimensions matching 3D building diorama layout as (width, height)",
    )
    center_coord: tuple[float, float] = Field(
        ...,
        description="Anchor center coordinate (x, y) in 3D building diorama layout",
    )
    description: str = Field(default="", description="Atmospheric and operational description of the diorama room")
    current_occupants: list[str] = Field(
        default_factory=list,
        description="List of agent IDs currently in this room",
    )
    status_accent: str = Field(
        default="#00FFCC",
        description="Cyber-luxury neon accent color for room borders and holograms",
    )

    @field_validator("dimensions", mode="before")
    @classmethod
    def parse_dimensions(cls, v: Any) -> tuple[float, float]:
        """Allow tuple, list, or dict format for dimensions."""
        if isinstance(v, dict):
            return (float(v.get("width", 240.0)), float(v.get("height", 220.0)))
        if isinstance(v, (list, tuple)) and len(v) >= 2:
            return (float(v[0]), float(v[1]))
        return v

    @field_validator("center_coord", mode="before")
    @classmethod
    def parse_center(cls, v: Any) -> tuple[float, float]:
        """Allow tuple, list, or dict format for center coordinate."""
        if isinstance(v, dict):
            return (float(v.get("x", 0.0)), float(v.get("y", 0.0)))
        if isinstance(v, (list, tuple)) and len(v) >= 2:
            return (float(v[0]), float(v[1]))
        return v

    @property
    def width(self) -> float:
        """Width convenience property."""
        return self.dimensions[0]

    @property
    def height(self) -> float:
        """Height convenience property."""
        return self.dimensions[1]


class ActivityLog(BaseModel):
    """Audit and operational activity entry."""

    id: str = Field(..., description="Unique event identifier")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp of activity",
    )
    agent_id: str = Field(..., description="Agent triggering or subject to the event")
    agent_name: str = Field(..., description="Display name of the agent")
    room_id: str = Field(..., description="Room where event took place")
    action: str = Field(..., description="Action title or code")
    details: str = Field(default="", description="Detailed narrative or payload")
    severity: str = Field(
        default="INFO",
        description="Event severity (INFO, WARNING, ALERT, SYSTEM)",
    )


class ServerTelemetry(BaseModel):
    """Runtime engine performance and system metrics."""

    uptime_seconds: float = Field(default=0.0)
    cpu_load_percent: float = Field(default=0.0)
    memory_load_mb: float = Field(default=0.0)
    active_agents: int = Field(default=11)
    total_rooms: int = Field(default=11)
    sim_ticks: int = Field(default=0)
    active_stream_clients: int = Field(default=0)


class MeetingDialogue(BaseModel):
    """Dialogue exchange item within meeting minutes."""

    speaker_id: str = Field(..., description="Agent identifier of the speaker")
    speaker_name: str = Field(..., description="Full display name of the speaker")
    role: str = Field(..., description="Role or organizational title of speaker")
    text: str = Field(..., description="Spoken dialogue or statement")


class ActionItem(BaseModel):
    """Actionable commitment assigned during executive meeting."""

    pic: str = Field(..., description="Person in charge / assignee name or identifier")
    task: str = Field(..., description="Action item description")
    due: str = Field(..., description="Target due date or milestone horizon")


class MeetingMinutes(BaseModel):
    """Official Minutes of Meeting (MoM) record for executive War Room sessions."""

    meeting_id: str = Field(..., description="Unique meeting session identifier")
    title: str = Field(..., description="Executive agenda / meeting title")
    leader_name: str = Field(..., description="Leader convening the council session")
    status: str = Field(
        ...,
        description="Meeting status ('IN_PROGRESS' | 'COMPLETED')",
    )
    started_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 start timestamp",
    )
    attendees: list[str] = Field(
        default_factory=list,
        description="List of participant names or IDs",
    )
    dialogues: list[MeetingDialogue] = Field(
        default_factory=list,
        description="Chronological dialogues during the session",
    )
    key_decisions: list[str] = Field(
        default_factory=list,
        description="Key decisions agreed upon during session",
    )
    action_items: list[ActionItem] = Field(
        default_factory=list,
        description="Assigned action items with PIC and due date",
    )
    reporting_to_ceo: str = Field(
        default="Diserahkan kepada CEO Daniandra Prayudisty oleh Daffa (CEO Office)",
        description="Executive briefing submission record delivered to CEO",
    )
    ceo_feedback: str = Field(
        default="Disetujui. Lanjutkan eksekusi teknis di bawah supervisi CTO Raziel Hendrix.",
        description="Executive directive and approval feedback from CEO",
    )


class OfficeStateResponse(BaseModel):
    """Complete snapshot of the virtual office state."""

    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Snapshot timestamp",
    )
    office_mode: OfficeMode = Field(
        default=OfficeMode.NORMAL,
        description="Active operating mode ('NORMAL', 'WAR_ROOM', 'REST_CYCLE', 'RECREATION')",
    )
    agents: list[AgentInfo] = Field(default_factory=list)
    rooms: list[RoomInfo] = Field(default_factory=list)
    recent_activities: list[ActivityLog] = Field(default_factory=list)
    server_telemetry: dict[str, Any] = Field(default_factory=dict)
    latest_meeting: Optional[MeetingMinutes] = Field(
        default=None,
        description="Latest War Room Council meeting minutes",
    )
    meetings_history: list[MeetingMinutes] = Field(
        default_factory=list,
        description="Historical archive of past and current meeting minutes",
    )


class AuthVerifyRequest(BaseModel):
    """Authentication PIN verification request."""

    pin: str = Field(..., description="Security PIN code")


class AuthVerifyResponse(BaseModel):
    """Authentication verification response."""

    success: bool = Field(..., description="Whether PIN is valid")
    message: str = Field(..., description="Status message")
    token: Optional[str] = Field(default=None, description="Session authorization token")


class AgentActionRequest(BaseModel):
    """Direct command or state modification for a specific agent."""

    action: str = Field(
        default="move",
        description="Command action ('move', 'set_task', 'set_status', 'use_tool')",
    )
    target_room_id: Optional[str] = Field(default=None, description="Destination room ID")
    new_status: Optional[AgentStatus] = Field(default=None, description="New agent status")
    new_task: Optional[str] = Field(default=None, description="Updated task description")
    tool: Optional[str] = Field(default=None, description="Tool invoked or updated")


class OfficeActionRequest(BaseModel):
    """Headquarters-wide operational command."""

    action: str = Field(
        ...,
        description="Office action ('gather_war_room', 'resume_deep_work', 'trigger_sleep_cycle', 'trigger_recreation')",
    )
    details: Optional[str] = Field(default=None, description="Optional command context")
