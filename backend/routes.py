"""API Route endpoints for Yudiaz Virtual HQ.

Exposes REST contracts and SSE streaming under prefix /api/v1.
"""

import asyncio
from datetime import datetime, timezone
import secrets
from typing import Any, AsyncGenerator, Optional

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from backend.config import get_settings
from backend.models import (
    ActivityLog,
    AgentActionRequest,
    AgentInfo,
    AuthVerifyRequest,
    AuthVerifyResponse,
    OfficeActionRequest,
    OfficeStateResponse,
    RoomInfo,
)
from backend.office_engine import office_engine

router = APIRouter(prefix="/api/v1", tags=["Office"])
settings = get_settings()


@router.get("/health", response_model=dict[str, Any], summary="API Health and Status Probe")
async def health_check() -> dict[str, Any]:
    """Retrieve service health status, engine mode, and active agent count."""
    state = office_engine.get_state()
    return {
        "status": "ok",
        "service": settings.title,
        "version": settings.version,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "office_mode": state.office_mode,
        "active_agents": len(state.agents),
        "total_rooms": len(state.rooms),
        "uptime_seconds": state.server_telemetry.get("uptime_seconds", 0.0),
    }


@router.get("/office/state", response_model=OfficeStateResponse, summary="Get Full Office State")
async def get_office_state() -> OfficeStateResponse:
    """Retrieve full spatial state, all agent telemetry, room occupants, and recent logs."""
    return office_engine.get_state()


@router.get("/rooms", response_model=list[RoomInfo], summary="List All Architectural Rooms")
async def list_rooms() -> list[RoomInfo]:
    """Retrieve list of all 11 zones and their live occupant IDs."""
    return list(office_engine.rooms.values())


@router.get("/rooms/{room_id}", response_model=RoomInfo, summary="Get Specific Room Detail")
async def get_room(room_id: str) -> RoomInfo:
    """Retrieve metadata and live occupants of a specific room."""
    room = office_engine.get_room(room_id)
    if not room:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Room with id '{room_id}' not found.",
        )
    return room


@router.get("/agents", response_model=list[AgentInfo], summary="List All 11 Personnel")
async def list_agents() -> list[AgentInfo]:
    """Retrieve profiles, locations, tasks, and telemetry for all studio agents."""
    return list(office_engine.agents.values())


@router.get("/agents/{agent_id}", response_model=AgentInfo, summary="Get Specific Agent State")
async def get_agent(agent_id: str) -> AgentInfo:
    """Retrieve profile and live spatial telemetry for a specific agent."""
    agent = office_engine.get_agent(agent_id)
    if not agent:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent with id '{agent_id}' not found.",
        )
    return agent


@router.post(
    "/agents/{agent_id}/action",
    response_model=AgentInfo,
    summary="Execute Action or Relocate Agent",
)
async def agent_action(agent_id: str, payload: AgentActionRequest) -> AgentInfo:
    """Direct an agent to a new room, task, status, or tool invocation."""
    agent = office_engine.get_agent(agent_id)
    if not agent:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent with id '{agent_id}' not found.",
        )

    target_room = payload.target_room_id or agent.position.room_id
    if target_room not in office_engine.rooms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Target room '{target_room}' does not exist.",
        )

    try:
        updated_agent = office_engine.move_agent(
            agent_id=agent_id,
            target_room_id=target_room,
            new_status=payload.new_status,
            new_task=payload.new_task,
            tool=payload.tool,
        )
        return updated_agent
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/office/action",
    response_model=OfficeStateResponse,
    summary="Trigger Headquarters-Wide Operational Protocol",
)
async def office_action(payload: OfficeActionRequest) -> OfficeStateResponse:
    """Execute studio-wide orchestration commands:

    - gather_war_room: Assemble all personnel in War Room Amphitheater
    - resume_deep_work: Return personnel to departmental workstations
    - trigger_sleep_cycle: Transition headquarters to rest and recharge pods
    - trigger_recreation: Transition headquarters to recreation and studio break session
    """
    action = payload.action.strip().lower()

    if action == "gather_war_room":
        return office_engine.gather_war_room()
    elif action == "resume_deep_work":
        return office_engine.resume_deep_work()
    elif action == "trigger_sleep_cycle":
        return office_engine.trigger_sleep_cycle()
    elif action == "trigger_recreation":
        return office_engine.trigger_recreation()
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown office action '{payload.action}'. "
                "Supported: 'gather_war_room', 'resume_deep_work', 'trigger_sleep_cycle', 'trigger_recreation'."
            ),
        )


@router.get("/activities", response_model=list[ActivityLog], summary="List Audit Activity Logs")
async def get_activities(
    limit: int = Query(default=50, ge=1, le=200, description="Max logs to return"),
) -> list[ActivityLog]:
    """Retrieve chronological audit and event logs."""
    return office_engine.get_activities(limit=limit)


@router.get("/stream", summary="Server-Sent Events (SSE) Live Telemetry Stream")
async def sse_stream(
    request: Request,
    max_events: Optional[int] = Query(
        default=None,
        description="Optional event cap before completing stream (useful for tests and single probes)",
    ),
) -> StreamingResponse:
    """Stream live office state updates in text/event-stream format every 2 seconds.

    Immediately emits current snapshot on connect, and subsequent frames on state
    change or heartbeat interval.
    """

    async def event_generator() -> AsyncGenerator[str, None]:
        queue = office_engine.subscribe()
        emitted = 0
        try:
            # Emit immediate initial state frame
            initial_state = office_engine.get_state().model_dump_json()
            yield f"event: state\ndata: {initial_state}\n\n"
            emitted += 1

            while max_events is None or emitted < max_events:
                if await request.is_disconnected():
                    break

                try:
                    # Wait for push event or timeout on sse_interval
                    data_str = await asyncio.wait_for(
                        queue.get(),
                        timeout=settings.sse_interval,
                    )
                except asyncio.TimeoutError:
                    data_str = office_engine.get_state().model_dump_json()

                yield f"event: state\ndata: {data_str}\n\n"
                emitted += 1
        except asyncio.CancelledError:
            pass
        finally:
            office_engine.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no",
        },
    )


@router.post(
    "/auth/verify",
    response_model=AuthVerifyResponse,
    summary="Verify Security Command PIN",
)
async def verify_auth(payload: AuthVerifyRequest) -> JSONResponse:
    """Verify security PIN (default 2609) for executive command privileges."""
    if payload.pin == settings.pin:
        session_token = f"yudiaz-hq-{secrets.token_hex(16)}"
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "PIN verified successfully. Executive authorization granted.",
                "token": session_token,
            },
        )

    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={
            "success": False,
            "message": "Invalid security PIN.",
            "detail": "Invalid security PIN.",
            "token": None,
        },
    )
