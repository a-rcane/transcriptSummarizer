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
   [ 1. Ingestion Endpoint ] ──► Stores every raw event in an append-only MongoDB log
               │
      Is version newer?
        ├── NO  ──► Save to raw log, but don't overwrite current latest chart state.
        └── YES ──► Update latest chart state & publish a job to Kafka.
                        │
                        ▼
            [ 2. Kafka Consumer Worker ] (Runs 5 jobs concurrently with backpressure)
                        │
                        ▼
            [ 3. AI Extraction (LiteLLM: Ollama / OpenAI / Gemini / Claude) ]
                 - Extracts: Encounter Type, Urgency (Critical/Urgent/Routine),
                   Vitals, Active Problems, Medications.
                 - Calculates multi-dimensional confidence & flags if human review is needed.
                        │
                        ▼
            [ 4. Database & Cache Updates ]
                 - Updates MongoDB chart & rolling longitudinal patient history.
                 - Saves to Redis cache for instant sub-millisecond retrieval.
                        │
                        ▼
            [ 5. Clinical Workstation UI / Postman / FHIR Export ]
                 - Doctors view charts, stream tokens live, or edit and approve summaries.
```

---

## 4. Key Engineering Problems We Solved

### A. Duplicate & Out-of-Order Events
- **Idempotency**: Every event has an `event_id`. If the server sees an ID it has already processed, it drops it immediately (`status: "ignored_duplicate"`).
- **Dual-Store Strategy**:
  - `encounter_events`: An append-only log that saves every version received (`v1`, `v2`, `v3`) in case we ever need to audit or replay history.
  - `encounters`: The "latest state" collection. It only updates if the incoming version is strictly higher than what is currently recorded.
- **Event Replay Reconciliation**: If notes arrived out-of-order (`v1` -> `v3` -> `v2`), clicking **Reconcile Log** replays all versions chronologically and recalculates the perfect final summary.

### B. Multi-Dimensional Confidence & Urgency Triage
Instead of a single arbitrary confidence score, the model outputs:
- **Extraction Confidence**: Certainty in vital signs and medication extraction.
- **Diagnosis Confidence**: Certainty in identified active conditions.
- **Ambiguity Risk**: Risk score for contradictory or ambiguous clinical dictations.
- **Urgency Triage**: Automatically categorizes encounters as **`CRITICAL`** (acute chest pain, stroke symptoms), **`URGENT`** (fever, infection), or **`ROUTINE`** (refills, bookings) so the highest acuity patients appear at the top of the queue.

### C. Human-in-the-Loop (HITL) Diffing
When a doctor edits an AI draft and signs off, the system computes a unified text diff (`difflib.unified_diff`) between the AI proposal and the doctor's approved note. This maintains a clear audit trail and tracks model accuracy over time.

### D. Dead-Letter Queue (DLQ) Management
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

## 6. How to Use & Test the System

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
