import difflib
from datetime import datetime
from typing import Optional, List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError
from ..models.encounter import EncounterEntity, EncounterType, UrgencyLevel, ReviewFeedbackRecord
from ..models.summary import FeedbackMetricsResponse


class EncounterRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = db["encounters"]

    async def get_by_encounter_id(self, encounter_id: str) -> Optional[EncounterEntity]:
        doc = await self.collection.find_one({"encounter_id": encounter_id})
        return EncounterEntity(**doc) if doc else None

    async def get_latest_version(self, encounter_id: str) -> int:
        doc = await self.collection.find_one({"encounter_id": encounter_id}, {"latest_version": 1})
        return doc.get("latest_version", 0) if doc else 0

    async def upsert_latest_encounter(self, encounter: EncounterEntity) -> bool:
        now = datetime.utcnow()
        try:
            result = await self.collection.update_one(
                {
                    "encounter_id": encounter.encounter_id,
                    "$or": [
                        {"latest_version": {"$lt": encounter.latest_version}},
                        {"latest_version": {"$exists": False}}
                    ]
                },
                {
                    "$set": {
                        "encounter_id": encounter.encounter_id,
                        "pId": encounter.pId,
                        "latest_version": encounter.latest_version,
                        "transcriptions": encounter.transcriptions,
                        "updated_at": now
                    },
                    "$setOnInsert": {
                        "created_at": now,
                        "encounter_type": encounter.encounter_type,
                        "summary": encounter.summary,
                        "ai_generated_summary": encounter.summary,
                        "urgency_level": encounter.urgency_level,
                        "needs_human_review": False
                    }
                },
                upsert=True
            )
            return result.modified_count > 0 or result.upserted_id is not None
        except DuplicateKeyError:
            # Concurrent write occurred with equal or newer version
            return False

    async def update_summary(
        self,
        encounter_id: str,
        summary: str,
        encounter_type: Optional[str] = None,
        confidence_score: float = 1.0,
        confidence_breakdown: Optional[Dict[str, float]] = None,
        urgency_level: UrgencyLevel = UrgencyLevel.ROUTINE,
        needs_human_review: bool = False
    ) -> bool:
        update_fields = {
            "summary": summary,
            "ai_generated_summary": summary,
            "encounter_type": encounter_type,
            "confidence_score": confidence_score,
            "urgency_level": urgency_level.value if isinstance(urgency_level, UrgencyLevel) else urgency_level,
            "needs_human_review": needs_human_review,
            "updated_at": datetime.utcnow()
        }
        if confidence_breakdown:
            update_fields["confidence_breakdown"] = confidence_breakdown

        result = await self.collection.update_one(
            {"encounter_id": encounter_id},
            {"$set": update_fields}
        )
        return result.modified_count > 0

    async def get_pending_human_reviews(
        self,
        urgency: Optional[UrgencyLevel] = None
    ) -> List[EncounterEntity]:
        query = {"needs_human_review": True}
        if urgency:
            query["urgency_level"] = urgency.value if isinstance(urgency, UrgencyLevel) else urgency

        cursor = self.collection.find(query)
        encounters = [EncounterEntity(**doc) async for doc in cursor]

        # Sort priority: Critical (1) -> Urgent (2) -> Routine (3), then lowest confidence, then newest
        urgency_weight = {
            UrgencyLevel.CRITICAL: 1,
            "critical": 1,
            UrgencyLevel.URGENT: 2,
            "urgent": 2,
            UrgencyLevel.ROUTINE: 3,
            "routine": 3,
        }
        encounters.sort(
            key=lambda e: (
                urgency_weight.get(e.urgency_level, 3),
                e.confidence_score or 1.0,
                -(e.updated_at.timestamp() if e.updated_at else 0)
            )
        )
        return encounters

    async def override_summary(
        self,
        encounter_id: str,
        revised_summary: str,
        reviewed_by: str,
        revised_encounter_type: Optional[str] = None
    ) -> bool:
        existing = await self.get_by_encounter_id(encounter_id)
        if not existing:
            return False

        original_ai = existing.ai_generated_summary or existing.summary or ""
        is_modified = (original_ai.strip() != revised_summary.strip())

        # Compute text diff
        diff_lines = list(difflib.unified_diff(
            original_ai.splitlines(),
            revised_summary.splitlines(),
            fromfile="ai_generated_summary",
            tofile="clinician_revised_summary",
            lineterm=""
        ))
        diff_summary = "\n".join(diff_lines) if diff_lines else "No textual modifications (approved as-is)."

        now = datetime.utcnow()
        feedback_record = ReviewFeedbackRecord(
            encounter_id=encounter_id,
            pId=existing.pId,
            original_ai_summary=original_ai,
            revised_summary=revised_summary,
            reviewed_by=reviewed_by,
            reviewed_at=now,
            is_modified=is_modified,
            diff_summary=diff_summary,
            original_confidence=existing.confidence_score,
            revised_encounter_type=EncounterType(revised_encounter_type) if revised_encounter_type else existing.encounter_type
        )

        update_doc = {
            "summary": revised_summary,
            "needs_human_review": False,
            "reviewed_by": reviewed_by,
            "reviewed_at": now,
            "updated_at": now
        }
        if revised_encounter_type:
            update_doc["encounter_type"] = revised_encounter_type

        result = await self.collection.update_one(
            {"encounter_id": encounter_id},
            {
                "$set": update_doc,
                "$push": {"review_history": feedback_record.model_dump(mode="json")}
            }
        )
        return result.modified_count > 0

    async def get_review_history(self, encounter_id: str) -> List[ReviewFeedbackRecord]:
        doc = await self.collection.find_one({"encounter_id": encounter_id}, {"review_history": 1})
        if not doc or "review_history" not in doc:
            return []
        return [ReviewFeedbackRecord(**rec) for rec in doc.get("review_history", [])]

    async def get_feedback_metrics(self) -> FeedbackMetricsResponse:
        cursor = self.collection.find({"review_history.0": {"$exists": True}})
        total_reviewed = 0
        total_modified = 0
        total_unmodified = 0
        conf_sum = 0.0
        extract_sum = 0.0
        diag_sum = 0.0
        count = 0

        async for doc in cursor:
            history = doc.get("review_history", [])
            for rec in history:
                total_reviewed += 1
                if rec.get("is_modified"):
                    total_modified += 1
                else:
                    total_unmodified += 1
                conf_sum += rec.get("original_confidence") or doc.get("confidence_score") or 1.0

            breakdown = doc.get("confidence_breakdown") or {}
            extract_sum += breakdown.get("extraction_confidence", 0.9)
            diag_sum += breakdown.get("diagnosis_confidence", 0.9)
            count += 1

        mod_rate = round((total_modified / total_reviewed * 100), 2) if total_reviewed > 0 else 0.0
        avg_conf = round((conf_sum / total_reviewed), 2) if total_reviewed > 0 else 1.0
        avg_extract = round((extract_sum / count), 2) if count > 0 else 1.0
        avg_diag = round((diag_sum / count), 2) if count > 0 else 1.0

        return FeedbackMetricsResponse(
            total_reviewed_encounters=total_reviewed,
            modified_by_clinician_count=total_modified,
            approved_unmodified_count=total_unmodified,
            clinician_modification_rate_pct=mod_rate,
            avg_original_confidence=avg_conf,
            avg_extraction_confidence=avg_extract,
            avg_diagnosis_confidence=avg_diag
        )
