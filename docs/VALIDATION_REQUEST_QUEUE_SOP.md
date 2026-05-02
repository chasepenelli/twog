# Validation Request Queue SOP

## Purpose

Move a ready validation plan into explicit, human-approved validation work without launching jobs automatically.

This queue is the handoff between synthesis/validation planning and execution lanes such as expert review, docking, Boltz, ADMET, safety review, omics analysis, or wet-lab planning.

## Preconditions

- A research brief has passed synthesis evaluation.
- A validation plan exists with:
  - `status: ready_for_review`
  - `readiness: ready_for_expert_review`
  - at least one task with a `validation_request`
- The operator has reviewed the plan tasks before dispatch.

## Control Flow

1. Create or identify a validation plan.
2. Preview queue items from that plan.
3. Apply the queue operation only after the preview looks correct.
4. Approve individual queue items.
5. Dispatch approved items through the existing async validation hook.

Creating a validation plan does not dispatch work. Queueing a validation request also does not dispatch work. Dispatch requires an explicit approval step.

## CLI

Preview queue items:

```bash
python -m hsa_research.ingestion_bridge.cli queue-validation-requests \
  --plan-id <plan_id>
```

Persist queue items:

```bash
python -m hsa_research.ingestion_bridge.cli queue-validation-requests \
  --plan-id <plan_id> \
  --apply
```

List queue items:

```bash
python -m hsa_research.ingestion_bridge.cli validation-request-queue \
  --plan-id <plan_id>
```

Approve one item:

```bash
python -m hsa_research.ingestion_bridge.cli approve-validation-request \
  --id <queue_item_id> \
  --approved-by <operator> \
  --approval-note "Ready for controlled validation."
```

Dispatch one approved item:

```bash
python -m hsa_research.ingestion_bridge.cli dispatch-validation-request \
  --id <queue_item_id>
```

## Dagster

Manual jobs:

- `validation_request_queue_job`
- `validation_request_queue_library_job`

Recommended first hosted run:

```json
{
  "ops": {
    "validation_request_queue_report": {
      "config": {
        "plan_id": "<plan_id>",
        "task_ids": [],
        "dry_run": true
      }
    }
  }
}
```

After review, run again with `dry_run: false`.

## MCP

Tools:

- `queue_validation_requests`
- `get_validation_request_queue_item`
- `list_validation_request_queue`
- `approve_validation_request`
- `dispatch_validation_request`

Resource:

- `validation-request-queue://{queue_item_id}`

## Approval Standard

Approve only if the task is specific enough to execute:

- The target, candidate, or review object is clear.
- The objective is testable or reviewable.
- Required inputs are available or explicitly named.
- Expected outputs are clear.
- Evidence refs point back to the brief, evaluation, and cited sources.
- The task does not imply clinical action or treatment advice.

## Failure Handling

- Dispatching an unapproved item does not launch work. It records a `last_error` and leaves the item in its current status.
- Duplicate queueing is deduped by `plan_id + task_id`.
- Failed or obsolete queue items should be archived or superseded in a later maintenance pass.

