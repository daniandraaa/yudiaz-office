# 🏢 Yudiaz Virtual HQ — Spatial Cyber-Luxury Virtual Headquarters

> **Interactive 2.5D Isometric Virtual Office & Autonomous Multi-Agent Command Center**  
> Built for **Daniandra Prayudisty** (Founder & CEO) & Yudiaz Creative Studio Engineering Team.

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python 3.12](https://img.shields.io/badge/Python-3.12+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Isometric 2.5D](https://img.shields.io/badge/Graphics-SVG%20Isometric%202.5D-6366F1.svg)]()
[![Caddy](https://img.shields.io/badge/Caddy-TLS%201.3-1F88C0.svg?logo=caddy&logoColor=white)](https://caddyserver.com)
[![Status](https://img.shields.io/badge/Production-Live%20Operational-10B981.svg)]()

---

## 🏛️ System Architecture

```
                                  [ CEO: Pak Dani ]
                                          │
                                          │ (Web Browser)
                                          ▼
                         [ Spatial Isometric Blueprint ]
                        https://office.daniandraaa.my.id
                                          │
                       ┌──────────────────┴──────────────────┐
                       │ (SSE Live Telemetry Stream)         │ (REST Commands)
                       ▼                                     ▼
         ┌────────────────────────────────────────────────────────┐
         │             YUDIAZ VIRTUAL HQ BACKEND                  │
         │          FastAPI ASGI Engine (Port: 9449)              │
         ├────────────────────────────────────────────────────────┤
         │  • OfficeEngine Spatial State Machine                  │
         │  • 10 Personnel Autonomous Agents Tracking             │
         │  • 9 Cyber-Luxury Office Sectors                       │
         │  • Real-Time Event Dispatcher & Activity Ticker        │
         └────────────────────────────────────────────────────────┘
```

---

## 🏢 9 Cyber-Luxury Office Sectors

1. **`SEC-01` CEO Executive Suite & Command Deck**:
   - Curved floating quantum glass desk, holographic telepresence globe orb, executive lounge, emerald/gold aura.
2. **`SEC-02` The War Room & Strategy Amphitheater**:
   - Circular strategy conference table, 10 biometric chairs, central floating rotating 3D holographic Concept 2B ARCH-YD Yudiaz Monogram emitter.
3. **`SEC-03` Creative Director & Visual Labs**:
   - Senna Louviere digital color canvas easel, rotating 3D geometric wireframe polyhedron, Pantone swatch matrix.
4. **`SEC-04` Engineering Workstations (Dev Core)**:
   - Idris Nakamura & Mika Stellan workstation pods, curved ultrawide multi-monitors with green/cyan streaming matrix code.
5. **`SEC-05` Architecture & Research Atelier**:
   - Kael Ashford & Nara Vasquez master drafting tables, 3D holographic cloud network topology, archive knowledge cartridge racks.
6. **`SEC-06` Intelligence Radar NOC**:
   - Jovan Aritza tactical semicircular console, 360° rotating radar sweep beam with target blips, satellite telemetry monitors.
7. **`SEC-07` Executive Concierge & Wellness Pantry**:
   - Elara Sinclair transparent schedule calendar, Italian chrome espresso bar with rising steam vapor, executive waiting lounge.
8. **`SEC-08` Cyber Rest Pods & Zen Quarters**:
   - 4 cryogenic sleep capsules with blue biometric glass covers, animated ECG heartbeat lines, floating "Zzz" particles.
9. **`SEC-09` Core Server & AI Gateway Vault**:
   - 3 heavy server cabinets (*Caddy Ingress*, *Yudiaz Sentinel*, *9Router AI Cluster*), blinking LED activity matrix, cryogenic nitrogen cooling floor vents.

---

## 👥 10 Autonomous Personnel Roster

| Personnel | Codename | Role & Department | Primary Sector | Default State |
| :--- | :--- | :--- | :--- | :--- |
| **Daniandra Prayudisty** | `FOUNDER-01` | Founder & CEO (Executive) | CEO Suite | WORKING |
| **Raziel Hendrix** | `ORCHESTRATOR-01` | CTO & Head of Engineering | CEO Suite / War Room | WORKING |
| **Kael Ashford** | `ARCHITECT-01` | Lead System Architect | Architecture Atelier | RESEARCHING |
| **Nara Vasquez** | `RESEARCHER-01` | Lead R&D & Academic Intelligence | Research Atelier | RESEARCHING |
| **Senna Louviere** | `CREATIVE-01` | Creative Director & UI/UX | Creative Studio | WORKING |
| **Idris Nakamura** | `DEVELOPER-01` | Senior Developer (Backend/Fullstack) | Dev Core | WORKING |
| **Mika Stellan** | `FRONTEND-01` | Frontend & Spatial Visualization | Dev Core | WORKING |
| **Viktor Moreau** | `QA-SEC-01` | Lead QA & Security Engineer | Server Vault | WORKING |
| **Elara Sinclair** | `CONCIERGE-01` | Personal Assistant to CEO | Concierge Pantry | WORKING |
| **Jovan Aritza** | `INTEL-01` | Intelligence Officer (Tel-U) | Radar NOC | STANDBY |
| **Daffa** | `CEO-OFFICE-01` | CEO Office (Strategic Alignment) | CEO Suite | WORKING |

---

## 🕹️ Interactive Executive Features

1. **Executive Mode Controls**:
   - **War Room Summit**: Assembles all 10 agents around the grand conference table with holographic projection.
   - **Deep Work Core**: Disperses engineers and researchers back to their individual workstations.
   - **Night Rest Cycle**: Docks idle agents into Cyber Rest Pods with active night sentry monitors.
2. **Personnel Dossier Inspector**:
   - Click any agent avatar to view live operational stats, CPU/RAM footprint, active process, and real-time thought log.
3. **Room Sector Inspector**:
   - Click any room to zoom in, view environmental telemetry (temperature, air quality, power load), and list present occupants.
4. **Cyber Ticker**:
   - Real-time streaming log feed at the bottom bar showing multi-agent operations as they happen.
5. **Web Audio Ambience Generator**:
   - Synthesized subtle cyber terminal hum (55Hz / 110Hz sub-bass drone) with toggle control.
6. **Security PIN Gate**:
   - Protected by executive PIN (`2609`) with local storage session persistence.

---

## 🚀 Quick Start

### Run Server Locally
```bash
source .venv/bin/activate
uvicorn backend.main:app --host 127.0.0.1 --port 9449
```

### Run Automated Tests
```bash
.venv/bin/pytest tests/
# 19 passed (100%)
```

---
*Developed by the Yudiaz Creative Studio Engineering Team under CTO Raziel Hendrix.*
