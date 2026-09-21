# Clinical Encounter & Transcript Summarizer

## 1. Project Description

In hospitals and clinics, doctors, triage nurses, and specialists write or dictate notes as care happens. A patient might start in the emergency room with chest pain (Version 1), get lab work ordered 20 minutes later (Version 2), and be transferred to cardiology after an ECG (Version 3).

1. **Events arrive out of order**: Version 3 might hit the server before Version 2 because of network latency. If a system blindly overwrites records, newer life-saving findings can get wiped out by late-arriving older notes.
2. **Duplicate webhooks get sent**: Retries and network blips blast the exact same event multiple times.
3. **Fast need trustworthy summaries**: Clinicians don’t have time to read through 15 messy raw notes. They need a clean, structured summary with extracted vitals, medications, and active diagnoses—plus full control to review, edit, and sign off on what the AI generated.

**This system solves all three problems.** It ingests raw clinical notes via webhooks, safely handles duplicates and out-of-order versions, uses an LLM (local via Ollama/vLLM or cloud via OpenAI/Gemini/Claude) to structure and summarize the notes, and provides doctors with a clean workstation to review and attest the results.

---

## 2. Model & Provider Agnostic AI Engine

The system is **100% LLM-provider agnostic**, powered by **LiteLLM**. You can swap models and providers with a single environment variable change—no code changes required.

```
                     ┌────────────────────────────────────────────────────────┐
                     │          Unified AI Client (LiteLLM Layer)             │
                     └───────────────────────────┬────────────────────────────┘
                                                 │
            ┌────────────────────────────────────┼───────────────────────────────────┐
            ▼                                    ▼                                   ▼
 ┌───────────────────────────┐      ┌───────────────────────────┐       ┌───────────────────────────┐
 │ Local Self-Hosted LLMs    │      │ Cloud AI Providers        │       │ Offline Clinical Fallback │
 │ • Ollama (qwen2.5, llama3)│      │ • OpenAI (gpt-4o)         │       │ • Regex & heuristic rule  │
 │ • vLLM, LM Studio         │      │ • Gemini (gemini-1.5)     │       │   engine (zero external   │
 │ • LocalAI                 │      │ • Claude, Groq, Bedrock   │       │   dependencies needed)    │
 └───────────────────────────┘      └───────────────────────────┘       └───────────────────────────┘
```

### Supported Providers:
- **Local Runtimes**: Ollama (`ollama/qwen2.5-coder:7b`, `ollama/llama3`, `ollama/mistral`), vLLM, LM Studio, LocalAI.
- **Cloud APIs**: OpenAI (`gpt-4o`, `gpt-4o-mini`), Google Gemini (`gemini/gemini-1.5-flash`), Anthropic Claude (`claude-3-5-sonnet-20240620`), Groq, Mistral, Azure OpenAI, AWS Bedrock.
- **Built-In Offline Fallback**: If no LLM runtime is running or an API key is absent, the system automatically falls back to a deterministic clinical regex/heuristic extractor without crashing.

---

## 3. How the System Works (The Big Picture)

```
[ Clinical Webhook / Streamer ]
               │
               ▼
   [ 1. Ingestion Endpoint & Validation ]
         ├── Patient ID Mismatch? ──► REJECT ("rejected_patient_mismatch")
         ├── Duplicate event_id?  ──► DROP ("ignored_duplicate")
         └── Valid Event:
               │
               ├─► A. Append to Raw Event Log (`encounter_events` with status: "PENDING")
               │
               ├─► B. Update Latest Encounter Document (`encounters`, latest_version = N)
               │
               ├─► C. Persist Transactional Outbox Record (`outbox_events`, status: "PENDING_DISPATCH")
               │
               └─► D. Fast-Path Kafka Dispatch ──► Kafka Topic ("encounter-summarization")
                                                    └─► Mark Outbox as "DISPATCHED"
                                                        │
                      ┌─────────────────────────────────┘
                      │ (If process crashed before Kafka dispatch)
                      ▼
               [ Outbox Sweeper Daemon ] ──► Polls orphaned records, re-dispatches & tracks SLAs
                      │
                      ▼
   [ 2. Kafka Consumer Worker ] (5 concurrent tasks with backpressure & 3x exponential backoff)
               │
               ▼
   [ 3. AI Extraction (LiteLLM: Ollama / OpenAI / Gemini / Claude) ]
        - Extracts: Encounter Type, Clinical Urgency (Critical/Urgent/Routine),
          Vitals, Active Problems, Medications.
        - Calculates multi-dimensional confidence & flags if human review is needed.
               │
               ▼
   [ 4. Atomic Compare-And-Swap (CAS) Database Update ]
        - Checks: `summary_version < incoming_job_version`
        │
        ├── WON RACE (e.g. v13):
        │     - Updates `encounters.summary` and advances `summary_version = 13`
        │     - Marks `encounter_events` status as "SUMMARIZED"
        │     - Appends to longitudinal `PatientHistory` (OCC)
        │     - Invalidates Redis cache (`summary:encounter:*`, `summary:patient:*`)
        │
        └── LOST RACE (e.g. late-finishing v12):
              - Skips `encounters` and `PatientHistory` updates (prevents stale overwrites)
              - Marks `encounter_events` status as "OBSOLETE" (stores text for 100% auditability)
               │
               ▼
   [ 5. Clinical Workstation UI / Postman / FHIR Export ]
        - Doctors view charts, stream tokens live, review diffs, or query exact-version status.
```

---

## 4. Key Engineering Problems We Solved

### A. Asynchronous Completion Races & Atomic CAS Barriers (v12 vs v13)
* **The Problem**: LLM generation takes variable time ($2\text{s} - 8\text{s}$). If version 13 finishes summarizing before version 12, an uncoordinated database write would allow the stale $v12$ result to overwrite $v13$'s newer chart summary and pollute the longitudinal patient timeline.
* **The Solution**: We enforce an atomic **Compare-And-Swap (CAS)** barrier directly inside MongoDB:
  `update_one({"encounter_id": id, "summary_version": {"$lt": job_version}}, {"$set": ...})`
  * If $v13$ finishes first, `summary_version` becomes `13`.
  * When $v12$ finishes later, MongoDB rejects the update (`modified_count == 0`).
  * $v12$ is marked as **`OBSOLETE`** in the event log, and its text is safely preserved for auditing without touching the active encounter summary or the longitudinal `PatientHistory`.

### B. Transactional Outbox Pattern & Zero-Data-Loss Crash Recovery
* **The Problem**: In dual-write architectures, if MongoDB updates succeed but the microservice crashes before publishing to Kafka, in-memory work is lost.
* **The Solution**: In the same ingestion cycle, an outbox record is persisted in MongoDB (`outbox_events`) with status `PENDING_DISPATCH`. A background [`OutboxSweeper`](file:///c:/Users/sanch/PycharmProjects/transcriptSummarizer/encounter_service/app/services/outbox_sweeper.py) daemon continuously detects orphaned records and dispatches them upon reboot.

### C. Patient Identity Immutability Guard
* **The Problem**: Prevent malicious or accidental mutations where incoming versions try to associate an existing encounter ID with a different patient ID.
* **The Solution**: Inbound webhooks strictly validate patient identity against the existing encounter state. Any mismatch is immediately rejected with status `rejected_patient_mismatch` with zero database or messaging side effects.

### D. Exact-Version Summary Status & SLA Monitoring
* **Status Query**: Inspect exact-version lifecycle state (`PENDING`, `SUMMARIZED`, `OBSOLETE`, `FAILED`) via `GET /encounters/{encounter_id}/versions/{version}/status`.
* **SLA Breach Detection**: The background sweeper monitors the secondary event log for any version pending longer than the SLA threshold ($30\text{s}$) to trigger alerts.

### E. Multi-Dimensional Confidence & Urgency Triage
Instead of a single arbitrary confidence score, the model outputs:
- **Extraction Confidence**: Certainty in vital signs and medication extraction.
- **Diagnosis Confidence**: Certainty in identified active conditions.
- **Ambiguity Risk**: Risk score for contradictory or ambiguous clinical dictations.
- **Urgency Triage**: Automatically categorizes encounters as **`CRITICAL`** (acute chest pain, stroke symptoms), **`URGENT`** (fever, infection), or **`ROUTINE`** (refills, bookings) so the highest acuity patients appear at the top of the queue.

### F. Human-in-the-Loop (HITL) Diffing & Attestation
When a doctor edits an AI draft and signs off, the system computes a unified text diff (`difflib.unified_diff`) between the AI proposal and the doctor's approved note. This maintains a clear audit trail and tracks model accuracy over time.

### G. Dead-Letter Queue (DLQ) Management & Re-Drive
If an LLM call fails or the broker gets overloaded, the Kafka worker retries 3 times with exponential backoff (1s, 2s, 4s). If it still fails, the message is routed to MongoDB's `dlq_messages` collection so admins can inspect the error and re-drive it with one click.


---

## 5. Complete Setup & Installation Guide

### Prerequisites
- **Docker Desktop** installed and running.
- **Any LLM of your choice**:
  - **Local (Ollama)**: [Download Ollama](https://ollama.ai) (default is `qwen2.5-coder:7b` or `llama3`), OR
  - **Cloud (OpenAI / Gemini / Anthropic)**: API Key.

---

### Step 1: Configure Your AI Model Provider

Open [`encounter_service/.env`](file:///c:/Users/sanch/PycharmProjects/transcriptSummarizer/encounter_service/.env) and pick your provider:

#### Option A: Local Ollama (Default - Zero API Costs)
```properties
AI_MODEL=ollama/qwen2.5-coder:7b
AI_API_BASE=http://host.docker.internal:11434
```
*(Make sure to pull your model first: `ollama pull qwen2.5-coder:7b`)*

#### Option B: Google Gemini
```properties
AI_MODEL=gemini/gemini-1.5-flash
GEMINI_API_KEY=your_gemini_api_key_here
```

#### Option C: OpenAI
```properties
AI_MODEL=gpt-4o-mini
OPENAI_API_KEY=your_openai_api_key_here
```

#### Option D: Anthropic Claude
```properties
AI_MODEL=claude-3-5-sonnet-20240620
ANTHROPIC_API_KEY=your_anthropic_api_key_here
```

---

### Step 2: Build & Launch with Docker Compose

In the root directory of the project, run:

```bash
docker compose up -d --build
```

This launches all 5 containers:
1. `encounter_service` (FastAPI backend + Web Workstation on **Port 8000**)
2. `event_streamer` (Partner event generator on **Port 8001**)
3. `encounter_mongodb` (MongoDB on **Port 27017**)
4. `encounter_kafka` (Kafka KRaft broker on **Port 9092**)
5. `encounter_redis` (Redis cache on **Port 6379**)

---

### Step 3: Verify Container Health & Logs

Check that all containers are healthy:
```bash
docker compose ps
```

To view live startup logs:
```bash
docker compose logs -f encounter-service
```

**Expected clean output**:
```text
encounter_service  | [Startup] MongoDB connected and indexed.
encounter_service  | [Startup] Kafka producer started.
encounter_service  | [Kafka Admin] Topic 'encounter-summarization' initialized.
encounter_service  | [Redis] Connected successfully to redis:6379
encounter_service  | [Kafka Consumer] Started listening on 'encounter-summarization' with concurrency limit: 5
encounter_service  | INFO:     Application startup complete.
```

---

## 6. Setup

### Option A: Use the Clinical Web Workstation (Easiest)

Open your web browser and go to:
👉 **`http://localhost:8000/`**

Inside the workstation:
1. **Encounter Ingestion**:
   - Pick a clinical scenario preset (*Acute Chest Discomfort v1*, *ECG Sinus Tachycardia v3*, or *Delayed Labs v2*).
   - Click **Submit Encounter Note** to see instant deduplication, out-of-order handling, and AI summarization.
2. **Patient Chart & Summary**:
   - Search by Encounter ID (`enc-101`) or Patient MRN (`pat-101`).
   - View the rolling longitudinal summary, vitals telemetry HUD, active conditions, and medications.
   - Click **Export HL7 FHIR** to view standard FHIR R4 JSON.
3. **Live Dictation Stream (SSE)**:
   - Enter `enc-101` and click **Connect SSE** to watch the LLM stream tokens in real-time.
4. **Physician Attestation Queue**:
   - Filter unreviewed notes by urgency (**Critical**, **Urgent**, **Routine**).
   - Click **Attest** on any note to edit, sign off, and record modification diffs.
5. **Event Replay & DLQ Audit**:
   - Click **Reconcile Log** to replay all raw versions chronologically.
   - Inspect Dead-Letter Queue (DLQ) failures and re-drive them with one click.
   - Use the slider to launch synthetic batches from the partner event streamer.

---

### Option B: Use the Postman Collection

1. Open Postman and click **Import**.
2. Select [`transcript_summarizer_postman_collection.json`](file:///c:/Users/sanch/PycharmProjects/transcriptSummarizer/transcript_summarizer_postman_collection.json) from the project root.
3. Run the requests across the 5 folders:
   - **`1. Webhook Ingestion`**: Test Version 1, Out-of-Order Version 3, Stale Version 2, and Duplicate suppression.
   - **`2. Summary & Patient Queries`**: Test Redis-cached lookups, FHIR bundle export, and SSE streaming.
   - **`3. Human-In-The-Loop & Operational Features`**: Test urgency filtering, physician override, diff history, feedback metrics, reconcile API, and DLQ retries.
    - **`4. Event Streamer Microservice`**: Trigger automated HTTP or Kafka streaming runs.
    - **`5. Health Checks`**: Verify both services are healthy (`/health`).

---

## 7. Running Automated Tests

The repository includes a dedicated automated test suite in [`encounter_service/tests/test_distributed_guarantees.py`](file:///c:/Users/sanch/PycharmProjects/transcriptSummarizer/encounter_service/tests/test_distributed_guarantees.py) that verifies the core distributed correctness guarantees.

### Option A: Run Tests via Docker (Recommended)

Run the test suite inside the running container:

```bash
docker compose exec encounter-service pytest tests/test_distributed_guarantees.py -v
```

To run all tests:
```bash
docker compose exec encounter-service pytest -v
```

---

### Option B: Run Tests Locally

If running outside Docker with a local Python 3.10+ virtual environment:

```bash
cd encounter_service
pip install -r requirements.txt
pytest tests/test_distributed_guarantees.py -v
```

---

### Key Scenarios Covered in the Test Suite

| Test Function | Scenario & Guarantee Verified |
| :--- | :--- |
| `test_late_v12_cannot_overwrite_newer_v13_summary` | **Async Race Protection (v12 vs v13):**<br>• $v13$ finishes first and atomically sets `summary_version = 13` (`SUMMARIZED`).<br>• Delayed $v12$ fails atomic CAS (`summary_version 13 >= 12`) and is marked **`OBSOLETE`**.<br>• Current chart summary remains $v13$, and longitudinal `PatientHistory` is **not** overwritten. |
| `test_outbox_recovers_pending_records_after_service_crash` | **Transactional Outbox Crash Recovery:**<br>• Simulates a service crash between MongoDB write and Kafka publish.<br>• Verifies `OutboxSweeper` recovers orphaned `PENDING_DISPATCH` records, emits them to Kafka, and marks them **`DISPATCHED`**. |
| `test_patient_identity_mismatch_rejected` | **Patient Identity Immutability Guard:**<br>• Attempts to send a new version for an existing encounter under a different `pId`.<br>• Verifies immediate rejection (`rejected_patient_mismatch`) with zero Kafka or Outbox mutations. |

---
