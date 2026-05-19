const state = {
  runtime: null,
  commandCenter: null,
  bigIdeas: null,
  activityEvents: null,
  actionItems: null,
  validationQueue: null,
  validationAutopilot: null,
  researchLeads: null,
  researchBriefs: null,
  ideas: null,
  promotion: null,
  validationTools: null,
  agentRuns: null,
  agentPerformance: null,
  computeJobs: null,
  selectedBigIdeaKey: null,
  bigIdeaSortables: [],
};

const $ = (id) => document.getElementById(id);
const isStaticPreview = () => window.location.protocol === "file:";

document.addEventListener("DOMContentLoaded", () => {
  $("refreshButton").addEventListener("click", refreshAll);
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.addEventListener("click", () => showPage(button.dataset.page));
  });
  $("bigIdeaStage").addEventListener("change", refreshBigIdeas);
  $("bigIdeaQuery").addEventListener("input", debounce(refreshBigIdeas, 250));
  $("activityFilter").addEventListener("change", refreshActivityEvents);
  $("validationStatus").addEventListener("change", refreshValidationQueue);
  $("validationSource").addEventListener("change", refreshValidationQueue);
  $("leadStatus").addEventListener("change", () => Promise.all([refreshActionItems(), refreshResearchLeads()]));
  $("briefQuality").addEventListener("change", refreshResearchBriefs);
  $("briefSource").addEventListener("change", refreshResearchBriefs);
  $("briefQuery").addEventListener("input", debounce(refreshResearchBriefs, 250));
  $("ideaKind").addEventListener("change", refreshIdeas);
  $("ideaSource").addEventListener("change", refreshIdeas);
  $("ideaQuery").addEventListener("input", debounce(refreshIdeas, 250));
  $("promotionQuery").addEventListener("input", debounce(refreshPromotion, 250));
  $("toolQuery").addEventListener("input", debounce(refreshValidationTools, 250));
  $("agentName").addEventListener("change", refreshAgentRuns);
  $("agentStatus").addEventListener("change", refreshAgentRuns);
  $("agentSource").addEventListener("change", refreshAgentRuns);
  $("agentQuery").addEventListener("input", debounce(refreshAgentRuns, 250));
  $("agentPerformanceLimit").addEventListener("change", refreshAgentPerformance);
  $("agentPerformanceMinSample").addEventListener("change", refreshAgentPerformance);
  $("agentPerformanceEvaluateButton").addEventListener("click", runAgentPerformanceEvaluation);
  $("autopilotPreviewButton").addEventListener("click", refreshValidationAutopilot);
  $("autopilotDryRunButton").addEventListener("click", () => runValidationAutopilot(true));
  $("autopilotRunButton").addEventListener("click", () => runValidationAutopilot(false));
  $("bigIdeaBoard").addEventListener("click", handleBigIdeaBoardClick);
  $("ideaDetailClose").addEventListener("click", closeIdeaDetail);
  $("ideaDetailContent").addEventListener("click", handleBigIdeaBoardClick);
  $("actionItemsList").addEventListener("click", handleQueueAction);
  $("validationRows").addEventListener("click", handleValidationAction);
  $("researchLeadRows").addEventListener("click", handleQueueAction);
  $("agentRunCards").addEventListener("click", handleAgentRunReview);
  refreshAll();
});

async function refreshAll() {
  $("refreshButton").disabled = true;
  try {
    if (isStaticPreview()) {
      renderStaticPreview();
      return;
    }
    await Promise.all([
      refreshRuntime(),
      refreshCommandCenter(),
      refreshBigIdeas(),
      refreshActivityEvents(),
      refreshActionItems(),
      refreshValidationQueue(),
      refreshValidationAutopilot(),
      refreshResearchLeads(),
      refreshResearchBriefs(),
      refreshIdeas(),
      refreshPromotion(),
      refreshValidationTools(),
      refreshAgentRuns(),
      refreshAgentPerformance(),
      refreshComputeJobs(),
    ]);
    showToast("Command center refreshed.");
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    $("refreshButton").disabled = false;
  }
}

async function refreshRuntime() {
  const payload = await getJson("/api/runtime");
  state.runtime = payload;
  renderRuntime(payload);
}

async function refreshCommandCenter() {
  const report = await getJson("/api/command-center?queue_limit=25&lead_limit=25&agent_run_limit=25");
  state.commandCenter = report;
  renderSummary(report.summary || {});
  renderRecommendations(report.recommendations || []);
  renderAgentRuns(report.recent_agent_runs || []);
  renderBriefQueue((report.research_brief_queue || {}).items || []);
}

async function refreshBigIdeas() {
  if (isStaticPreview()) {
    renderStaticPreview();
    return;
  }
  const params = new URLSearchParams();
  const stage = $("bigIdeaStage").value.trim();
  const query = $("bigIdeaQuery").value.trim();
  if (stage) params.append("stage", stage);
  if (query) params.append("query", query);
  params.append("limit", "150");
  const payload = await getJson(`/api/big-ideas?${params.toString()}`);
  state.bigIdeas = payload;
  renderBigIdeas(payload);
}

async function refreshActivityEvents() {
  if (isStaticPreview()) {
    renderStaticPreview();
    return;
  }
  const params = new URLSearchParams();
  const source = $("activityFilter").value.trim();
  if (source) params.append("source", source);
  params.append("limit", "100");
  const payload = await getJson(`/api/activity-events?${params.toString()}`);
  state.activityEvents = payload;
  renderActivityEvents(payload);
}

async function refreshActionItems() {
  const payload = await getJson("/api/action-items?limit=50");
  state.actionItems = payload;
  renderActionItems(payload);
}

async function refreshValidationQueue() {
  const params = new URLSearchParams();
  const status = $("validationStatus").value.trim();
  const source = $("validationSource").value.trim();
  if (status) params.append("status", status);
  if (source) params.append("source", source);
  params.append("limit", "50");
  const payload = await getJson(`/api/validation-requests?${params.toString()}`);
  state.validationQueue = payload;
  renderValidationQueue(payload);
}

async function refreshValidationAutopilot() {
  const params = new URLSearchParams();
  params.append("max_per_run", $("autopilotMaxPerRun").value || "2");
  params.append("manual_grace_period_hours", $("autopilotGraceHours").value || "6");
  params.append("force", $("autopilotForce").checked ? "true" : "false");
  const payload = await getJson(`/api/validation-autopilot?${params.toString()}`);
  state.validationAutopilot = payload;
  renderValidationAutopilot(payload);
}

async function refreshResearchLeads() {
  const params = new URLSearchParams();
  const status = $("leadStatus").value.trim();
  if (status) params.append("status", status);
  params.append("limit", "50");
  const payload = await getJson(`/api/research-leads?${params.toString()}`);
  state.researchLeads = payload;
  renderResearchLeads(payload);
}

async function refreshResearchBriefs() {
  const params = new URLSearchParams();
  const quality = $("briefQuality").value.trim();
  const source = $("briefSource").value.trim();
  const query = $("briefQuery").value.trim();
  if (quality) params.append("quality_status", quality);
  if (source) params.append("source", source);
  if (query) params.append("query", query);
  params.append("limit", "100");
  const payload = await getJson(`/api/research-briefs?${params.toString()}`);
  state.researchBriefs = payload;
  renderResearchBriefs(payload);
}

async function refreshIdeas() {
  const params = new URLSearchParams();
  const kind = $("ideaKind").value.trim();
  const source = $("ideaSource").value.trim();
  const query = $("ideaQuery").value.trim();
  if (kind) params.append("kind", kind);
  if (source) params.append("source", source);
  if (query) params.append("query", query);
  params.append("limit", "100");
  const payload = await getJson(`/api/ideas?${params.toString()}`);
  state.ideas = payload;
  renderIdeas(payload);
}

async function refreshPromotion() {
  const params = new URLSearchParams();
  const query = $("promotionQuery").value.trim();
  if (query) params.append("query", query);
  params.append("limit", "100");
  const payload = await getJson(`/api/hypothesis-promotion?${params.toString()}`);
  state.promotion = payload;
  renderPromotion(payload);
}

async function refreshValidationTools() {
  const params = new URLSearchParams();
  const query = $("toolQuery").value.trim();
  if (query) params.append("query", query);
  params.append("limit", "100");
  const payload = await getJson(`/api/validation-tool-catalog?${params.toString()}`);
  state.validationTools = payload;
  renderValidationTools(payload);
}

async function refreshAgentRuns() {
  const params = new URLSearchParams();
  const agentName = $("agentName").value.trim();
  const status = $("agentStatus").value.trim();
  const source = $("agentSource").value.trim();
  const query = $("agentQuery").value.trim();
  if (agentName) params.append("agent_name", agentName);
  if (status) params.append("status", status);
  if (source) params.append("source", source);
  if (query) params.append("query", query);
  params.append("limit", "100");
  const payload = await getJson(`/api/agent-runs?${params.toString()}`);
  state.agentRuns = payload;
  renderAgentRunsPage(payload);
}

async function refreshAgentPerformance() {
  const params = new URLSearchParams();
  params.append("limit", $("agentPerformanceLimit").value || "500");
  params.append("min_sample_size", $("agentPerformanceMinSample").value || "3");
  const payload = await getJson(`/api/agent-performance?${params.toString()}`);
  state.agentPerformance = payload;
  renderAgentPerformance(payload);
}

async function refreshComputeJobs() {
  const payload = await getJson("/api/compute-jobs?limit=50");
  state.computeJobs = payload;
  renderComputeJobs(payload);
}

function showPage(pageId) {
  document.querySelectorAll(".page-view").forEach((page) => {
    page.classList.toggle("active", page.id === pageId);
  });
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.page === pageId);
  });
  const active = Array.from(document.querySelectorAll(".tab-button")).find((button) => button.dataset.page === pageId);
  const pageTitle = $("pageTitle");
  if (active && pageTitle) pageTitle.textContent = active.textContent.trim();
}

function renderRuntime(payload) {
  const runtime = (payload || {}).validation_dispatch || {};
  const ready = Boolean(runtime.dispatch_ready);
  const model = runtime.default_model ? ` | ${runtime.default_model}` : "";
  $("runtimeStatus").className = `runtime-status ${ready ? "ready" : "blocked"}`;
  $("runtimeStatus").textContent = `${ready ? "Dispatch ready" : "Dispatch disabled"}${model}`;
  $("runtimeStatus").title = runtime.message || "";
  if ($("autopilotRunButton")) {
    $("autopilotRunButton").disabled = !ready;
    $("autopilotRunButton").title = ready ? "" : "OpenRouter must be configured before live autopilot dispatch.";
  }
  if ($("agentPerformanceEvaluateButton")) {
    $("agentPerformanceEvaluateButton").disabled = !ready;
    $("agentPerformanceEvaluateButton").title = ready ? "" : "OpenRouter must be configured before evaluator runs.";
  }
}

function renderStaticPreview() {
  const now = new Date().toISOString();
  const stages = [
    { key: "new_signal", label: "New Signal" },
    { key: "needs_evidence", label: "Needs Evidence" },
    { key: "committee_ready", label: "Committee Ready" },
    { key: "validation_ready", label: "Validation Ready" },
    { key: "queued_running", label: "Queued / Running" },
    { key: "reviewed", label: "Reviewed" },
    { key: "parked", label: "Parked" },
  ];
  const items = [
    {
      entity_type: "research_program",
      entity_id: "preview-vascular-program",
      type_label: "Research program",
      board_stage: "needs_evidence",
      board_stage_label: "Needs Evidence",
      title: "Vascular injury / coagulation ecology",
      summary: "Big-picture program tracking angiogenesis, vascular damage, clotting biology, and HSA transfer risk.",
      status: "active",
      readiness: "bounded evidence loop",
      score: 0.72,
      score_label: "confidence",
      evidence_refs: ["program-review", "omics-packet"],
      candidate_therapies: ["VEGF-axis strategy"],
      targets: ["KDR", "VIM"],
      biomarkers: ["vascular injury score"],
      risks: ["Needs tighter direct canine evidence."],
      blockers: ["evidence gap"],
      next_experiments: ["Run focused evidence brief", "Score processed omics matrix"],
      next_action: "Acquire the next evidence packet before committee expansion.",
      payload: {},
    },
    {
      entity_type: "therapy_idea",
      entity_id: "preview-vim-peptide",
      type_label: "Therapy idea",
      board_stage: "validation_ready",
      board_stage_label: "Validation Ready",
      title: "Vimentin-targeted peptide strategy",
      summary: "High-level peptide lane that asks whether surface VIM can define a selective HSA vulnerability.",
      status: "ready_for_validation_plan",
      readiness: "ready for packet",
      score: 0.81,
      score_label: "priority",
      evidence_refs: ["brief-vim", "omics-readout"],
      candidate_therapies: ["targeted peptide"],
      targets: ["VIM"],
      biomarkers: ["surface vimentin"],
      risks: ["Cell-surface expression context must be proven."],
      blockers: [],
      next_experiments: ["Peptide specialist review", "Target expression review"],
      next_action: "Create a validation packet with expression, assayability, and specificity readouts.",
      payload: {},
    },
    {
      entity_type: "validation_packet",
      entity_id: "preview-omics-packet",
      type_label: "Validation packet",
      board_stage: "queued_running",
      board_stage_label: "Queued / Running",
      title: "Processed omics readout packet",
      summary: "CPU-first readout for VIM, mesenchymal/ECM, angiogenesis/endothelial, and vascular injury scores.",
      status: "queued",
      readiness: "running",
      score: 0.63,
      score_label: "support",
      evidence_refs: ["geo-matrix", "gene-set-scores"],
      candidate_therapies: [],
      targets: ["VIM", "KDR"],
      biomarkers: ["ECM score", "coagulation score"],
      risks: ["Dataset labels may be incomplete."],
      blockers: ["needs review"],
      next_experiments: ["OpenRouter omics review"],
      next_action: "Review computed readouts and decide whether the idea moves to expert panel.",
      payload: {},
    },
  ];
  const counts = stages.reduce((acc, stage) => ({ ...acc, [stage.key]: 0 }), {});
  items.forEach((item) => {
    counts[item.board_stage] = (counts[item.board_stage] || 0) + 1;
  });
  state.runtime = {
    validation_dispatch: {
      dispatch_ready: false,
      default_model: "static preview",
      message: "Open through the local Command Center server for live data and actions.",
    },
  };
  state.bigIdeas = { stages, total: items.length, stage_counts: counts, items, updated_at: now };
  state.activityEvents = {
    total: 3,
    items: [
      {
        event_id: "preview-1",
        event_type: "idea.stage_changed",
        source: "ui",
        severity: "success",
        title: "Vimentin peptide lane promoted",
        summary: "Moved into Validation Ready after committee review and omics packet creation.",
        occurred_at: now,
        entity_type: "therapy_idea",
        entity_id: "preview-vim-peptide",
      },
      {
        event_id: "preview-2",
        event_type: "validation.queued",
        source: "validation",
        severity: "info",
        title: "Omics review queued",
        summary: "Processed-first omics readout awaits specialist interpretation.",
        occurred_at: now,
        entity_type: "validation_packet",
        entity_id: "preview-omics-packet",
      },
      {
        event_id: "preview-3",
        event_type: "program.needs_evidence",
        source: "system",
        severity: "watch",
        title: "Program needs one evidence pass",
        summary: "The board is constrained to bounded loops before therapy idea expansion.",
        occurred_at: now,
        entity_type: "research_program",
        entity_id: "preview-vascular-program",
      },
    ],
  };
  renderRuntime(state.runtime);
  $("runtimeStatus").textContent = "Static preview | run the local server for live data";
  $("runtimeStatus").className = "runtime-status blocked";
  renderSummary({
    brief_queue_total: 12,
    brief_queue_ready: 4,
    brief_queue_failed: 0,
    research_leads_total: 18,
    research_leads_actionable: 7,
    research_leads_followup: 5,
    source_health_failed: 1,
    source_health_triage: 3,
    source_health_watch: 2,
    recommendation_count: 6,
    blocking_recommendations: 1,
  });
  renderBigIdeas(state.bigIdeas);
  renderActivityEvents(state.activityEvents);
  $("actionSummary").textContent = "Static preview. Run the local server to load live queues.";
  $("actionItemsList").innerHTML = `<div class="empty-state">Live action items load from the Command Center API.</div>`;
  showToast("Static preview loaded. Use twog-command-center for live data.");
}

function validationDispatchReady() {
  return Boolean(((state.runtime || {}).validation_dispatch || {}).dispatch_ready);
}

function renderSummary(summary) {
  $("metricBriefQueue").textContent = value(summary.brief_queue_total);
  $("metricBriefQueueMeta").textContent =
    `${value(summary.brief_queue_ready)} ready, ${value(summary.brief_queue_failed)} failed`;
  $("metricResearchLeads").textContent = value(summary.research_leads_total);
  $("metricResearchLeadsMeta").textContent =
    `${value(summary.research_leads_actionable)} actionable, ${value(summary.research_leads_followup)} follow-up`;
  $("metricSourceHealth").textContent = value(summary.source_health_failed);
  $("metricSourceHealthMeta").textContent =
    `${value(summary.source_health_triage)} triage, ${value(summary.source_health_watch)} watch`;
  $("metricRecommendations").textContent = value(summary.recommendation_count);
  $("metricRecommendationsMeta").textContent = `${value(summary.blocking_recommendations)} blocking`;
}

function renderBigIdeas(payload) {
  const stages = payload.stages || [];
  const items = payload.items || [];
  const counts = payload.stage_counts || {};
  $("bigIdeaSummary").textContent =
    `${value(payload.total)} cards | ` +
    stages.map((stage) => `${stage.label}: ${value(counts[stage.key])}`).join(" | ");

  destroyBigIdeaSortables();
  $("bigIdeaBoard").innerHTML = stages.map((stage) => {
    const cards = items.filter((item) => item.board_stage === stage.key);
    return `
      <section class="board-column" data-stage="${escapeAttribute(stage.key)}">
        <div class="board-column-header">
          <strong>${escapeHtml(stage.label)}</strong>
          <span>${cards.length}</span>
        </div>
        <div class="board-card-list" data-stage-list="${escapeAttribute(stage.key)}">
          ${cards.length ? cards.map(renderBigIdeaCard).join("") : `<div class="empty-column">No cards</div>`}
        </div>
      </section>
    `;
  }).join("");
  initBigIdeaDrag();
  if (state.selectedBigIdeaKey) {
    const selected = findBigIdeaByKey(state.selectedBigIdeaKey);
    if (selected) renderIdeaDetail(selected);
  }
}

function renderBigIdeaCard(item) {
  const key = bigIdeaKey(item);
  const score = item.score !== null && item.score !== undefined ? Math.round(Number(item.score) * 100) : null;
  const blockers = (item.blockers || []).length;
  const evidence = (item.evidence_refs || []).length;
  const tags = [
    ...(item.candidate_therapies || []),
    ...(item.targets || []),
    ...(item.biomarkers || []),
  ].slice(0, 5);
  return `
    <article class="board-card" data-entity-type="${escapeAttribute(item.entity_type)}" data-entity-id="${escapeAttribute(item.entity_id)}" data-key="${escapeAttribute(key)}">
      <button class="card-open" type="button" data-action="open-big-idea" data-key="${escapeAttribute(key)}">
        <span class="work-lane">${escapeHtml(item.type_label || item.entity_type)}</span>
        <strong>${escapeHtml(item.title || "Untitled")}</strong>
        <span>${escapeHtml(trimText(item.summary || item.hypothesis || "", 150))}</span>
      </button>
      <div class="board-card-meta">
        ${score !== null ? tag(`${item.score_label || "score"} ${score}`, "info") : ""}
        ${tag(item.readiness || item.status || "unknown", item.status || "info")}
        ${evidence ? tag(`${evidence} refs`, "info") : tag("no refs", "watch")}
        ${blockers ? tag(`${blockers} blockers`, "blocking") : ""}
      </div>
      <div class="tag-row compact-tags">${tags.map((label) => tag(label, "info")).join("")}</div>
      <div class="card-actions">
        <button type="button" data-action="big-idea-work" data-work="evidence_brief" data-key="${escapeAttribute(key)}">Queue Evidence</button>
        <button type="button" data-action="big-idea-work" data-work="validation_packet" data-key="${escapeAttribute(key)}">Queue Validation</button>
      </div>
    </article>
  `;
}

function initBigIdeaDrag() {
  if (!window.Sortable) return;
  document.querySelectorAll("[data-stage-list]").forEach((list) => {
    const sortable = window.Sortable.create(list, {
      group: "big-ideas",
      animation: 120,
      draggable: ".board-card",
      ghostClass: "drag-ghost",
      onEnd: handleBigIdeaDrop,
    });
    state.bigIdeaSortables.push(sortable);
  });
}

function destroyBigIdeaSortables() {
  (state.bigIdeaSortables || []).forEach((sortable) => sortable.destroy());
  state.bigIdeaSortables = [];
}

async function handleBigIdeaDrop(event) {
  const card = event.item;
  const stage = event.to?.dataset?.stageList;
  if (!card || !stage) return;
  const entityType = card.dataset.entityType;
  const entityId = card.dataset.entityId;
  if (isStaticPreview()) {
    showToast("Static preview move only. Run the local server to persist stages.");
    return;
  }
  try {
    await postJson(`/api/big-ideas/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/stage`, {
      stage,
      operator: $("operatorName").value.trim() || "command_center_operator",
    });
    showToast("Board stage updated.");
    await Promise.all([refreshBigIdeas(), refreshActivityEvents()]);
  } catch (error) {
    showToast(error.message || String(error));
    await refreshBigIdeas();
  }
}

async function handleBigIdeaBoardClick(event) {
  const button = event.target.closest("button[data-action]");
  if (!button || button.disabled) return;
  const key = button.dataset.key;
  const item = findBigIdeaByKey(key);
  if (!item) return;
  if (button.dataset.action === "open-big-idea") {
    state.selectedBigIdeaKey = key;
    renderIdeaDetail(item);
    return;
  }
  if (button.dataset.action === "big-idea-stage") {
    if (isStaticPreview()) {
      showToast("Static preview action only. Run the local server to persist stages.");
      return;
    }
    button.disabled = true;
    try {
      await postJson(`/api/big-ideas/${encodeURIComponent(item.entity_type)}/${encodeURIComponent(item.entity_id)}/stage`, {
        stage: button.dataset.stage,
        operator: $("operatorName").value.trim() || "command_center_operator",
      });
      showToast("Board stage updated.");
      await Promise.all([refreshBigIdeas(), refreshActivityEvents()]);
    } catch (error) {
      showToast(error.message || String(error));
    } finally {
      button.disabled = false;
    }
    return;
  }
  if (button.dataset.action === "big-idea-work") {
    if (isStaticPreview()) {
      showToast("Static preview action only. Run the local server to queue work.");
      return;
    }
    button.disabled = true;
    try {
      const payload = await postJson(`/api/big-ideas/${encodeURIComponent(item.entity_type)}/${encodeURIComponent(item.entity_id)}/queue-work`, {
        action: button.dataset.work,
        operator: $("operatorName").value.trim() || "command_center_operator",
      });
      showToast(bigIdeaWorkToast(button.dataset.work, payload));
      await Promise.all([refreshBigIdeas(), refreshActivityEvents(), refreshCommandCenter(), refreshResearchBriefs(), refreshValidationQueue()]);
    } catch (error) {
      showToast(error.message || String(error));
    } finally {
      button.disabled = false;
    }
  }
}

function renderIdeaDetail(item) {
  $("ideaDetailDrawer").classList.add("open");
  $("ideaDetailDrawer").setAttribute("aria-hidden", "false");
  $("ideaDetailTitle").textContent = item.title || "Untitled idea";
  const stageButtons = (state.bigIdeas?.stages || []).map((stage) =>
    `<button type="button" data-action="big-idea-stage" data-stage="${escapeAttribute(stage.key)}" data-key="${escapeAttribute(bigIdeaKey(item))}">${escapeHtml(stage.label)}</button>`
  ).join("");
  $("ideaDetailContent").innerHTML = `
    <div class="detail-section">
      <div class="tag-row">
        ${tag(item.type_label || item.entity_type, "info")}
        ${tag(item.board_stage_label || item.board_stage, "info")}
        ${tag(item.readiness || item.status || "unknown", item.status || "info")}
      </div>
      <p>${escapeHtml(item.summary || item.hypothesis || "No summary recorded.")}</p>
      <p class="subtext">${escapeHtml(item.next_action || "")}</p>
    </div>
    <div class="detail-actions">${stageButtons}</div>
    <div class="detail-actions">
      <button type="button" data-action="big-idea-work" data-work="evidence_brief" data-key="${escapeAttribute(bigIdeaKey(item))}">Queue Evidence Brief</button>
      <button type="button" data-action="big-idea-work" data-work="validation_packet" data-key="${escapeAttribute(bigIdeaKey(item))}">Queue Validation Packet</button>
      <button type="button" data-action="big-idea-work" data-work="promote" data-key="${escapeAttribute(bigIdeaKey(item))}">Promote</button>
      <button type="button" data-action="big-idea-work" data-work="demote" data-key="${escapeAttribute(bigIdeaKey(item))}">Demote</button>
      <button type="button" data-action="big-idea-work" data-work="park" data-key="${escapeAttribute(bigIdeaKey(item))}">Park</button>
    </div>
    <div class="detail-grid two-up">
      <div><strong>Evidence Refs</strong>${renderInlineList(item.evidence_refs || [])}</div>
      <div><strong>Missing Evidence</strong>${renderInlineList(item.missing_evidence || [])}</div>
      <div><strong>Risks / Blockers</strong>${renderInlineList([...(item.risks || []), ...(item.blockers || [])])}</div>
      <div><strong>Next Experiments</strong>${renderInlineList(item.next_experiments || [])}</div>
      <div><strong>Targets</strong>${renderInlineList(item.targets || [])}</div>
      <div><strong>Biomarkers</strong>${renderInlineList(item.biomarkers || [])}</div>
    </div>
    ${renderJsonDetails("Raw payload", item.payload || {})}
  `;
}

function closeIdeaDetail() {
  $("ideaDetailDrawer").classList.remove("open");
  $("ideaDetailDrawer").setAttribute("aria-hidden", "true");
}

function findBigIdeaByKey(key) {
  return (state.bigIdeas?.items || []).find((item) => bigIdeaKey(item) === key);
}

function bigIdeaKey(item) {
  return `${item?.entity_type || ""}:${item?.entity_id || ""}`;
}

function bigIdeaWorkToast(action, payload) {
  if (action === "evidence_brief") return `Queued evidence brief ${shortId(payload.queue_item?.queue_item_id)}.`;
  if (action === "validation_packet") {
    const result = payload.result || {};
    return `Validation packet queue: ${result.queued_count || 0} queued, ${result.existing_queue_count || 0} existing.`;
  }
  return "Big Ideas card updated.";
}

function renderActivityEvents(payload) {
  const events = payload.items || [];
  if (!events.length) {
    $("activityStream").innerHTML = `<div class="empty-state">No activity events yet.</div>`;
    return;
  }
  $("activityStream").innerHTML = events.map((event) => `
    <article class="activity-item ${escapeAttribute(event.severity || "info")}">
      <div class="activity-dot"></div>
      <div>
        <div class="activity-header">
          <strong>${escapeHtml(event.title || event.event_type)}</strong>
          ${tag(event.source || "system", event.severity || "info")}
        </div>
        <p>${escapeHtml(event.summary || "")}</p>
        <span class="subtext">${escapeHtml(formatDateTime(event.occurred_at))} | ${escapeHtml(event.entity_type || "")} ${escapeHtml(shortId(event.entity_id || ""))}</span>
      </div>
    </article>
  `).join("");
}

function renderValidationQueue(payload) {
  const counts = payload.status_counts || {};
  const countText = Object.entries(counts)
    .map(([status, count]) => `${status}: ${count}`)
    .join(" | ");
  $("validationSummary").textContent =
    `${value(payload.visible)} visible of ${value(payload.total)} total` + (countText ? ` | ${countText}` : "");

  const rows = payload.items || [];
  if (!rows.length) {
    $("validationRows").innerHTML = `<tr><td colspan="6" class="empty-state">No validation queue items match the current filters.</td></tr>`;
    return;
  }

  $("validationRows").innerHTML = renderValidationPlanRows(rows);
}

function renderValidationAutopilot(payload) {
  const blockers = payload.blockers || [];
  const selected = payload.selected || [];
  const dispatched = payload.dispatched || [];
  const costText = `est $${Number(payload.estimated_cost_usd || 0).toFixed(4)} | spent $${Number(payload.daily_spend_usd || 0).toFixed(4)}/day`;
  $("autopilotSummary").textContent =
    `${value(payload.selected_count)} selected from ${value(payload.scanned_count)} pending | ` +
    `${value(payload.eligible_count)} eligible | ${value(payload.skipped_count)} skipped | ` +
    `${payload.dry_run ? "preview" : "apply"} | ${costText}` +
    (blockers.length ? ` | blockers: ${blockers.join(", ")}` : "");

  const rows = dispatched.length ? dispatched : selected;
  if (!rows.length) {
    $("autopilotList").innerHTML = `<div class="empty-state">No autopilot-eligible validation requests found.</div>`;
    return;
  }
  $("autopilotList").innerHTML = rows.map((item) => `
    <div class="list-item">
      <div class="list-item-header">
        <strong>${escapeHtml(item.title)}</strong>
        ${tag(item.status, item.status)}
      </div>
      <div class="subtext">${escapeHtml(item.reason)}</div>
      <div class="tag-row">
        ${tag(`priority ${item.priority}`, "info")}
        ${tag(item.task_type, "info")}
        ${tag(item.validation_type, "info")}
        ${item.source_key ? tag(item.source_key, "info") : ""}
        ${item.decision ? tag(`decision: ${item.decision}`, item.decision === "promote" ? "approved" : "watch") : ""}
        ${item.cost_usd !== null && item.cost_usd !== undefined ? tag(`$${Number(item.cost_usd).toFixed(4)}`, "info") : ""}
      </div>
    </div>
  `).join("");
}

function renderValidationPlanRows(items) {
  const groups = new Map();
  items.forEach((item) => {
    const key = item.plan_id || "unplanned";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  });
  return Array.from(groups.entries()).map(([planId, planItems]) => {
    const sortedItems = planItems.slice().sort((a, b) => Number(a.priority || 0) - Number(b.priority || 0));
    const first = sortedItems[0] || {};
    const statuses = sortedItems.reduce((counts, item) => {
      counts[item.status] = (counts[item.status] || 0) + 1;
      return counts;
    }, {});
    const statusText = Object.entries(statuses).map(([status, count]) => `${status}: ${count}`).join(" | ");
    const title = ((first.metadata || {}).idea_title || first.topic || "Validation plan").trim();
    return `
      <tr class="plan-group-row">
        <td colspan="6">
          <div class="plan-group">
            <strong>${escapeHtml(title)}</strong>
            <span>${escapeHtml(shortId(planId))} | ${escapeHtml(sortedItems.length)} tasks${statusText ? ` | ${escapeHtml(statusText)}` : ""}</span>
          </div>
        </td>
      </tr>
      ${sortedItems.map(renderValidationRow).join("")}
    `;
  }).join("");
}

function renderActionItems(payload) {
  const counts = payload.area_counts || {};
  const areaText = Object.entries(counts)
    .map(([area, count]) => `${area}: ${count}`)
    .join(" | ");
  $("actionSummary").textContent = `${value(payload.total)} open action items` + (areaText ? ` | ${areaText}` : "");
  const items = payload.items || [];
  if (!items.length) {
    $("actionItemsList").innerHTML = `<div class="empty-state">No action items found.</div>`;
    return;
  }
  $("actionItemsList").innerHTML = items.map(renderActionItem).join("");
}

function renderActionItem(item) {
  return `
    <article class="action-item">
      <div class="action-topline">
        <div class="title-cell">
          <strong>${escapeHtml(item.title)}</strong>
          <span class="subtext">${escapeHtml(item.description)}</span>
        </div>
        <div class="tag-row">
          ${tag(item.severity || item.status, item.severity || item.status)}
          ${tag(item.area, "info")}
          ${item.source_key ? tag(item.source_key, "info") : ""}
        </div>
      </div>
      <div class="action-meta">
        <span>${escapeHtml(item.kind)}</span>
        <span>Priority ${escapeHtml(item.priority)}</span>
        ${item.job_name ? `<span>${escapeHtml(item.job_name)}</span>` : ""}
      </div>
      ${renderActionButtons(item)}
      <details>
        <summary>Read details</summary>
        <pre>${escapeHtml(JSON.stringify(item.metadata || {}, null, 2))}</pre>
      </details>
    </article>
  `;
}

function renderActionButtons(item) {
  const actions = item.actions || [];
  if (!actions.length) return "";
  const buttons = actions.map((action) => {
    if (action === "approve_validation") {
      return actionButton("approve", item.item_id, "Approve");
    }
    if (action === "dispatch_validation") {
      return actionButton("dispatch", item.item_id, "Dispatch", "", !validationDispatchReady());
    }
    if (action === "promote_lead") {
      return actionButton("lead-status", item.item_id, "Promote", "watching");
    }
    if (action === "mark_followup") {
      return actionButton("lead-status", item.item_id, "Follow-up", "followup");
    }
    if (action === "demote_lead") {
      return actionButton("lead-status", item.item_id, "Demote", "dismissed");
    }
    if (action === "run_followup_search") {
      return actionButton("research-followup-loop", item.item_id, "Run search", "search");
    }
    if (action === "create_refined_queries") {
      return actionButton("refine-followup", item.item_id, "Create refined queries");
    }
    if (action === "reevaluate_followup") {
      return actionButton("research-followup-loop", item.item_id, "Re-evaluate", "evaluate", !validationDispatchReady());
    }
    if (action === "escalate_agent_finding") {
      return actionButton("escalate-agent-finding", item.item_id, "Create follow-up");
    }
    return "";
  });
  return `<div class="actions">${buttons.join("")}</div>`;
}

function renderValidationRow(item) {
  const request = item.validation_request || {};
  const context = request.assay_context || {};
  const blockers = item.dispatch_blockers || [];
  const gates = item.quality_gates || [];
  const metadata = item.metadata || {};
  const requiredInputs = metadata.required_inputs || [];
  const expectedOutputs = metadata.expected_outputs || [];
  const agentResult = metadata.validation_agent_result || {};
  const canApprove = item.status === "needs_approval";
  const canDispatch = item.status === "approved" && validationDispatchReady();
  const dispatchHint = item.status === "approved" && !validationDispatchReady()
    ? `<div class="subtext">Dispatch disabled until OpenRouter is configured on the server.</div>`
    : "";
  return `
    <tr>
      <td>${tag(item.status, item.status)}</td>
      <td>${escapeHtml(item.priority)}</td>
      <td>
        <div class="title-cell">
          <span class="work-lane">${escapeHtml(validationTaskLabel(item.task_type))}</span>
          <strong>${escapeHtml(item.title)}</strong>
          <span class="subtext">${escapeHtml(item.objective || request.objective || "No objective recorded")}</span>
          <div class="tag-row">
            ${tag(item.task_type, "info")}
            ${tag(request.validation_type || "unknown", "info")}
            ${item.source_key ? tag(item.source_key, "info") : tag("all sources", "info")}
          </div>
        </div>
      </td>
      <td>
        <div class="title-cell">
          <span>${escapeHtml(context.assay_type || context.model_system || "No assay/model context")}</span>
          <span class="subtext">${escapeHtml((context.species || []).join(", ") || "No species context")}</span>
          <span class="subtext">${escapeHtml(context.readout || context.endpoint || "No readout/endpoint recorded")}</span>
          <span class="subtext">${escapeHtml(context.disease_context || "No disease context")}</span>
          <div class="tag-row">${gates.slice(0, 4).map((gate) => tag(gate, "info")).join("")}</div>
        </div>
      </td>
      <td>
        ${blockers.length ? `<div class="tag-row">${blockers.map((blocker) => tag(blocker, "blocked")).join("")}</div>` : tag("clear", "approved")}
        ${item.last_error ? `<div class="subtext">${escapeHtml(item.last_error)}</div>` : ""}
        ${renderValidationAgentResult(agentResult)}
        <details>
          <summary>Inputs and outputs</summary>
          <div class="detail-grid">
            <div>
              <strong>Required</strong>
              ${renderInlineList(requiredInputs)}
            </div>
            <div>
              <strong>Expected</strong>
              ${renderInlineList(expectedOutputs)}
            </div>
          </div>
        </details>
      </td>
      <td>
        <div class="actions">
          <button type="button" data-action="approve" data-id="${escapeHtml(item.queue_item_id)}" ${canApprove ? "" : "disabled"}>Approve</button>
          <button type="button" data-action="dispatch" data-id="${escapeHtml(item.queue_item_id)}" ${canDispatch ? "" : "disabled"}>Dispatch</button>
        </div>
        ${dispatchHint}
      </td>
    </tr>
  `;
}

function renderValidationAgentResult(result) {
  if (!result || !result.decision) return "";
  const tone = result.decision === "promote" ? "approved" : result.decision === "demote" ? "blocked" : "watch";
  return `
    <div class="agent-result">
      <div class="tag-row">
        ${tag(`decision: ${result.decision}`, tone)}
        ${tag(`confidence: ${Math.round(Number(result.confidence || 0) * 100)}%`, "info")}
      </div>
      <div class="subtext">${escapeHtml(result.summary || "No validation summary recorded.")}</div>
    </div>
  `;
}

function validationTaskLabel(taskType) {
  const labels = {
    expert_review: "Evidence review",
    wet_lab: "Assay design",
    safety: "Safety risk review",
    target_validation: "Target validation",
    docking: "Docking",
    boltz: "Structure prediction",
    md: "Molecular dynamics",
    omics: "Omics review",
    partner_review: "Partner review",
  };
  return labels[taskType] || taskType || "Validation task";
}

function renderInlineList(values) {
  if (!values.length) {
    return `<p class="subtext">None recorded.</p>`;
  }
  return `<ul class="inline-list">${values.map((value) => `<li>${escapeHtml(value)}</li>`).join("")}</ul>`;
}

function renderResearchLeads(payload) {
  const counts = payload.status_counts || {};
  const countText = Object.entries(counts)
    .map(([status, count]) => `${status}: ${count}`)
    .join(" | ");
  $("researchLeadSummary").textContent =
    `${value(payload.visible)} visible of ${value(payload.total)} total` + (countText ? ` | ${countText}` : "");

  const rows = payload.items || [];
  if (!rows.length) {
    $("researchLeadRows").innerHTML = `<tr><td colspan="6" class="empty-state">No research leads match the current filters.</td></tr>`;
    return;
  }

  $("researchLeadRows").innerHTML = rows.map(renderResearchLeadRow).join("");
}

function renderResearchLeadRow(item) {
  const tags = [...(item.topic_tags || []), ...(item.suggested_sources || [])].slice(0, 8);
  return `
    <tr>
      <td>${tag(item.status, item.status)}</td>
      <td>${escapeHtml(item.priority)}</td>
      <td>
        <div class="title-cell">
          <strong>${escapeHtml(item.title || "Untitled research lead")}</strong>
          <span class="subtext">${escapeHtml(item.lead_type)} | ${escapeHtml(item.source_key || item.origin_source_key || "unknown source")}</span>
          ${item.url ? `<a href="${escapeAttribute(item.url)}" target="_blank" rel="noreferrer">Open source</a>` : ""}
        </div>
      </td>
      <td>${escapeHtml(item.reason || item.summary || "No reason recorded.")}</td>
      <td><div class="tag-row">${tags.map((label) => tag(label, "info")).join("")}</div></td>
      <td>
        <div class="actions">
          ${actionButton("lead-status", item.lead_id, "Promote", "watching", item.status === "watching")}
          ${actionButton("lead-status", item.lead_id, "Follow-up", "followup", item.status === "followup")}
          ${actionButton("lead-status", item.lead_id, "Demote", "dismissed", item.status === "dismissed")}
        </div>
      </td>
    </tr>
  `;
}

function renderResearchBriefs(payload) {
  const qualities = Object.entries(payload.quality_status_counts || {})
    .map(([status, count]) => `${status}: ${count}`)
    .join(" | ");
  const score = payload.average_overall_score !== null && payload.average_overall_score !== undefined
    ? ` | avg score ${Number(payload.average_overall_score).toFixed(2)}`
    : "";
  $("briefSummary").textContent =
    `${value(payload.visible)} visible of ${value(payload.total)} briefs` +
    ` | ${value(payload.evaluated_count)} evaluated | ${value(payload.ready_count)} ready` +
    `${score}` +
    (qualities ? ` | ${qualities}` : "");

  const items = payload.items || [];
  if (!items.length) {
    $("briefList").innerHTML = `<div class="empty-state">No research briefs match the current filters.</div>`;
    return;
  }
  $("briefList").innerHTML = items.map(renderResearchBriefCard).join("");
}

function renderResearchBriefCard(item) {
  const score = item.overall_score !== null && item.overall_score !== undefined
    ? `score ${Number(item.overall_score).toFixed(2)}`
    : "unevaluated";
  const created = item.created_at ? new Date(item.created_at).toLocaleString() : "unknown date";
  return `
    <article class="brief-card">
      <div class="brief-card-header">
        <div class="title-cell">
          <span class="work-lane">${escapeHtml(item.brief_style || "brief")}</span>
          <strong>${escapeHtml(item.topic || "Untitled research brief")}</strong>
          <span class="subtext">${escapeHtml(item.disease_scope || "No disease scope recorded.")}</span>
        </div>
        <div class="tag-row">
          ${tag(item.quality_status || "unknown", item.quality_status || "info")}
          ${tag(score, "info")}
          ${item.source_key ? tag(item.source_key, "info") : tag("all sources", "info")}
          ${item.readiness ? tag(item.readiness, item.passes_quality_bar ? "approved" : "watch") : ""}
        </div>
      </div>
      <div class="brief-metrics">
        <span>${escapeHtml(item.citation_count)} citations</span>
        <span>${escapeHtml(item.finding_count)} findings</span>
        <span>${escapeHtml(item.hypothesis_count)} hypotheses</span>
        <span>${escapeHtml(item.evidence_limitation_count)} limitations</span>
        <span>${escapeHtml(created)}</span>
      </div>
      <details open>
        <summary>Read brief</summary>
        <div class="brief-text">${escapeHtml(item.final_brief || "No final brief text recorded.")}</div>
      </details>
      <details>
        <summary>Hypotheses, citations, limitations</summary>
        <div class="detail-grid two-up">
          <div>
            <strong>Ranked Hypotheses</strong>
            ${renderHypothesisPreview(item.hypothesis_preview || [])}
          </div>
          <div>
            <strong>Citations</strong>
            ${renderCitationPreview(item.citation_preview || [])}
          </div>
          <div>
            <strong>Evidence Limitations</strong>
            ${renderInlineList(item.evidence_limitations || [])}
          </div>
          <div>
            <strong>Record</strong>
            <div class="tag-row">
              ${tag(shortId(item.brief_id), "info")}
              ${item.evaluation_id ? tag(`eval ${shortId(item.evaluation_id)}`, "info") : ""}
              ${item.agent_run_id ? tag(`agent ${shortId(item.agent_run_id)}`, "info") : ""}
            </div>
          </div>
        </div>
      </details>
    </article>
  `;
}

function renderHypothesisPreview(items) {
  if (!items.length) return `<p class="subtext">No ranked hypotheses recorded.</p>`;
  return `<ul class="inline-list">${items.map((item) => `
    <li>
      ${escapeHtml(item.claim || "No claim recorded.")}
      <span class="subtext">${escapeHtml(item.evidence_strength || "unknown")} | ${(item.citations || []).map(escapeHtml).join(", ")}</span>
    </li>
  `).join("")}</ul>`;
}

function renderCitationPreview(items) {
  if (!items.length) return `<p class="subtext">No citations recorded.</p>`;
  return `<ul class="inline-list">${items.map((item) => `
    <li>
      ${escapeHtml(item.citation_id || "citation")}
      ${item.source_url ? `<a href="${escapeAttribute(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.title || item.source_url)}</a>` : escapeHtml(item.title || "Untitled citation")}
      <span class="subtext">${escapeHtml(item.source_key || "")}</span>
    </li>
  `).join("")}</ul>`;
}

function renderIdeas(payload) {
  const kinds = Object.entries(payload.kind_counts || {})
    .map(([kind, count]) => `${kind}: ${count}`)
    .join(" | ");
  const statuses = Object.entries(payload.status_counts || {})
    .map(([status, count]) => `${status}: ${count}`)
    .join(" | ");
  $("ideaSummary").textContent =
    `${value(payload.visible)} visible of ${value(payload.total)} idea records` +
    (kinds ? ` | ${kinds}` : "") +
    (statuses ? ` | ${statuses}` : "");

  const items = payload.items || [];
  if (!items.length) {
    $("ideaList").innerHTML = `<div class="empty-state">No idea records match the current filters.</div>`;
    return;
  }
  $("ideaList").innerHTML = items.map(renderIdeaCard).join("");
}

function renderIdeaCard(item) {
  const score = item.priority_score !== null && item.priority_score !== undefined
    ? `priority ${Math.round(Number(item.priority_score) * 100)}`
    : item.confidence !== null && item.confidence !== undefined
      ? `confidence ${Math.round(Number(item.confidence) * 100)}`
      : "unscored";
  const tags = [
    ...(item.candidate_therapies || []),
    ...(item.targets || []),
    ...(item.biomarkers || []),
  ].slice(0, 12);
  return `
    <article class="idea-card">
      <div class="idea-card-header">
        <div class="title-cell">
          <span class="work-lane">${escapeHtml(ideaKindLabel(item.kind))}</span>
          <strong>${escapeHtml(item.title || "Untitled idea")}</strong>
          <span class="subtext">${escapeHtml(item.topic || "No source topic recorded.")}</span>
        </div>
        <div class="tag-row">
          ${tag(item.status || "recorded", item.status || "info")}
          ${tag(score, "info")}
          ${item.source_key ? tag(item.source_key, "info") : ""}
          ${item.evidence_strength ? tag(`evidence: ${item.evidence_strength}`, "info") : ""}
        </div>
      </div>
      <div class="idea-body">
        <div>
          <h3>Hypothesis</h3>
          <p>${escapeHtml(item.hypothesis || "No hypothesis recorded.")}</p>
        </div>
        <div>
          <h3>Rationale</h3>
          <p>${escapeHtml(item.rationale || "No rationale recorded.")}</p>
        </div>
      </div>
      ${item.mechanism || item.translational_path ? `
        <div class="idea-body compact">
          ${item.mechanism ? `<div><h3>Mechanism</h3><p>${escapeHtml(item.mechanism)}</p></div>` : ""}
          ${item.translational_path ? `<div><h3>Translation Path</h3><p>${escapeHtml(item.translational_path)}</p></div>` : ""}
        </div>
      ` : ""}
      <div class="tag-row">${tags.map((label) => tag(label, "info")).join("")}</div>
      <div class="idea-footer">
        <span>${escapeHtml(shortId(item.idea_id))}</span>
        ${item.committee_run_id ? `<span>committee ${escapeHtml(shortId(item.committee_run_id))}</span>` : ""}
        ${item.plan_id ? `<span>plan ${escapeHtml(shortId(item.plan_id))}</span>` : ""}
        ${item.origin_agent_run_id ? `<span>agent ${escapeHtml(shortId(item.origin_agent_run_id))}</span>` : ""}
      </div>
      <details>
        <summary>Evidence, risks, validation records</summary>
        <div class="detail-grid two-up">
          <div>
            <strong>Evidence Refs</strong>
            ${renderInlineList(item.evidence_refs || [])}
          </div>
          <div>
            <strong>Validation Status</strong>
            ${renderStatusCounts(item.validation_status_counts || {})}
          </div>
          <div>
            <strong>Risks</strong>
            ${renderInlineList(item.risks || [])}
          </div>
          <div>
            <strong>Next Experiments</strong>
            ${renderInlineList(item.next_experiments || [])}
          </div>
        </div>
      </details>
    </article>
  `;
}

function renderStatusCounts(counts) {
  const entries = Object.entries(counts);
  if (!entries.length) return `<p class="subtext">No validation queue records yet.</p>`;
  return `<div class="tag-row">${entries.map(([status, count]) => tag(`${status}: ${count}`, status)).join("")}</div>`;
}

function renderPromotion(payload) {
  const states = Object.entries(payload.state_counts || {})
    .map(([state, count]) => `${state}: ${count}`)
    .join(" | ");
  $("promotionSummary").textContent =
    `${value(payload.candidate_count)} promotion candidates` + (states ? ` | ${states}` : "");
  const items = payload.candidates || [];
  if (!items.length) {
    $("promotionList").innerHTML = `<div class="empty-state">No promotion candidates match the current filters.</div>`;
    return;
  }
  $("promotionList").innerHTML = items.map(renderPromotionCard).join("");
}

function renderPromotionCard(item) {
  const tools = (item.matched_tools || [])
    .map((match) => (match.tool || {}).tool_key)
    .filter(Boolean);
  return `
    <article class="idea-card">
      <div class="idea-card-header">
        <div class="title-cell">
          <span class="work-lane">${escapeHtml(item.source_type || "candidate")}</span>
          <strong>${escapeHtml(item.title || "Untitled candidate")}</strong>
          <span class="subtext">${escapeHtml(item.hypothesis || "")}</span>
        </div>
        <div class="tag-row">
          ${tag(item.promotion_state || "unknown", item.promotion_state || "info")}
          ${tag(`score ${Math.round(Number(item.score || 0) * 100)}`, "info")}
          ${item.recommended_job_name ? tag(item.recommended_job_name, "info") : ""}
        </div>
      </div>
      <div class="idea-body compact">
        <div><h3>Next Action</h3><p>${escapeHtml(item.recommended_next_action || "No next action recorded.")}</p></div>
        <div><h3>Blockers</h3>${renderInlineList(item.blockers || [])}</div>
      </div>
      <div class="tag-row">
        ${(item.candidate_therapies || []).slice(0, 5).map((label) => tag(label, "info")).join("")}
        ${(item.targets || []).slice(0, 5).map((label) => tag(label, "info")).join("")}
        ${tools.slice(0, 5).map((label) => tag(label, "info")).join("")}
      </div>
      <details>
        <summary>Evidence and validation matches</summary>
        <div class="detail-grid two-up">
          <div><strong>Evidence Refs</strong>${renderInlineList(item.evidence_refs || [])}</div>
          <div><strong>Risks</strong>${renderInlineList(item.risks || [])}</div>
          <div><strong>Next Experiments</strong>${renderInlineList(item.next_experiments || [])}</div>
          <div><strong>Matched Tools</strong>${renderInlineList(tools)}</div>
        </div>
      </details>
    </article>
  `;
}

function renderValidationTools(payload) {
  const categories = Object.entries(payload.category_counts || {})
    .map(([category, count]) => `${category}: ${count}`)
    .join(" | ");
  $("toolSummary").textContent =
    `${value(payload.tool_count)} validation tools` + (categories ? ` | ${categories}` : "");
  const items = payload.tools || [];
  if (!items.length) {
    $("toolList").innerHTML = `<div class="empty-state">No validation tools match the current filters.</div>`;
    return;
  }
  $("toolList").innerHTML = items.map(renderValidationToolCard).join("");
}

function renderValidationToolCard(tool) {
  return `
    <article class="idea-card">
      <div class="idea-card-header">
        <div class="title-cell">
          <span class="work-lane">${escapeHtml(tool.category || "validation")}</span>
          <strong>${escapeHtml(tool.display_name || tool.tool_key || "Validation tool")}</strong>
          <span class="subtext">${escapeHtml(tool.description || "")}</span>
        </div>
        <div class="tag-row">
          ${tag(tool.runner_status || "recommend_only", "info")}
          ${tag(tool.tool_hint || tool.tool_key || "tool", "info")}
        </div>
      </div>
      <div class="idea-body compact">
        <div><h3>Required Inputs</h3>${renderInlineList(tool.required_inputs || [])}</div>
        <div><h3>Outputs</h3>${renderInlineList(tool.outputs || [])}</div>
      </div>
      <details>
        <summary>Quality gates and dispatch blockers</summary>
        <div class="detail-grid two-up">
          <div><strong>Quality Gates</strong>${renderInlineList(tool.quality_gates || [])}</div>
          <div><strong>Dispatch Blockers</strong>${renderInlineList(tool.dispatch_blockers || [])}</div>
          <div><strong>Validation Types</strong>${renderInlineList(tool.compatible_validation_types || [])}</div>
          <div><strong>Task Types</strong>${renderInlineList(tool.compatible_task_types || [])}</div>
        </div>
      </details>
    </article>
  `;
}

function ideaKindLabel(kind) {
  const labels = {
    therapy_idea: "Therapy idea",
    validation_hypothesis: "Validation hypothesis",
  };
  return labels[kind] || kind || "Idea";
}

function renderRecommendations(items) {
  if (!items.length) {
    $("recommendationsList").innerHTML = `<div class="empty-state">No current recommendations.</div>`;
    return;
  }
  $("recommendationsList").innerHTML = items.map((item) => `
    <div class="list-item">
      <div class="list-item-header">
        <strong>${escapeHtml(item.action)}</strong>
        ${tag(item.severity, item.severity)}
      </div>
      <div class="subtext">${escapeHtml(item.reason)}</div>
      <div class="tag-row">
        ${tag(item.area, "info")}
        ${item.job_name ? tag(item.job_name, "info") : ""}
      </div>
    </div>
  `).join("");
}

function renderAgentRuns(items) {
  if (!items.length) {
    $("agentRunsList").innerHTML = `<div class="empty-state">No recent agent runs found.</div>`;
    return;
  }
  $("agentRunsList").innerHTML = items.slice(0, 12).map((item) => `
    <div class="list-item">
      <div class="list-item-header">
        <strong>${escapeHtml(item.agent_name)}</strong>
        ${tag(item.status, item.status)}
      </div>
      <div class="subtext">${escapeHtml(formatSummary(item.summary))}</div>
      <div class="tag-row">
        ${tag(item.model_profile || "model profile unset", "info")}
        ${item.source_key ? tag(item.source_key, "info") : ""}
        ${item.error_count ? tag(`${item.error_count} errors`, "failed") : ""}
      </div>
    </div>
  `).join("");
}

function renderComputeJobs(payload) {
  const counts = Object.entries(payload.status_counts || {})
    .map(([status, count]) => `${status}: ${count}`)
    .join(" | ");
  $("computeJobSummary").textContent =
    `${value(payload.visible)} visible of ${value(payload.total)} compute jobs` + (counts ? ` | ${counts}` : "");
  const items = payload.items || [];
  if (!items.length) {
    $("computeJobList").innerHTML = `<div class="empty-state">No compute jobs recorded yet.</div>`;
    return;
  }
  $("computeJobList").innerHTML = items.map((job) => `
    <div class="list-item">
      <div class="list-item-header">
        <strong>${escapeHtml(job.title || "Compute job")}</strong>
        ${tag(job.status || "unknown", job.status || "info")}
      </div>
      <div class="subtext">${escapeHtml(job.validation_type || "validation")} | ${escapeHtml(job.runner_kind || "runner")} | ${escapeHtml(formatDateTime(job.updated_at))}</div>
      <div class="tag-row">
        ${tag(job.compute_profile || "compute", "info")}
        ${job.runpod_job_id ? tag(`runpod ${shortId(job.runpod_job_id)}`, "info") : ""}
        ${job.last_error ? tag("error", "failed") : ""}
      </div>
      ${job.last_error ? `<pre>${escapeHtml(job.last_error)}</pre>` : ""}
    </div>
  `).join("");
}

function renderAgentRunsPage(payload) {
  const statuses = Object.entries(payload.status_counts || {})
    .map(([status, count]) => `${status}: ${count}`)
    .join(" | ");
  const agents = Object.entries(payload.agent_counts || {})
    .slice(0, 6)
    .map(([agentName, count]) => `${agentName}: ${count}`)
    .join(" | ");
  $("agentRunSummary").textContent =
    `${value(payload.visible)} visible of ${value(payload.total)} matching agent runs` +
    (statuses ? ` | ${statuses}` : "") +
    (agents ? ` | ${agents}` : "");

  const items = payload.items || [];
  if (!items.length) {
    $("agentRunCards").innerHTML = `<div class="empty-state">No agent runs match the current filters.</div>`;
    return;
  }
  $("agentRunCards").innerHTML = items.map(renderAgentRunCard).join("");
}

function renderAgentPerformance(payload) {
  const coverage = percent(payload.review_coverage || 0);
  const verdicts = payload.verdict_counts || {};
  $("agentPerformanceSummary").textContent =
    `${value(payload.reviewed_run_count)} reviewed of ${value(payload.agent_run_count)} runs (${coverage}) | ` +
    `${value(payload.operator_reviewed_count)} operator reviewed | ${value(payload.evaluator_reviewed_count)} evaluator reviewed | ` +
    `${value(payload.disagreement_count)} disagreements | ` +
    `useful ${value(verdicts.useful)}, follow-up ${value(verdicts.needs_followup)}, bad ${value(verdicts.bad)}, unclear ${value(verdicts.unclear)}`;

  const topRows = (payload.top_rows || []).filter((row) => row.reviewed_run_count > 0);
  const topKeys = new Set(topRows.map(agentPerformanceRowKey));
  const bottomRows = (payload.bottom_rows || [])
    .filter((row) => row.reviewed_run_count > 0)
    .filter((row) => !topKeys.has(agentPerformanceRowKey(row)));

  renderAgentPerformanceRows("agentPerformanceTopRows", topRows, "No top rows are available yet.");
  renderAgentPerformanceRows("agentPerformanceBottomRows", bottomRows, "No separate bottom rows are available yet.");
}

function agentPerformanceRowKey(row) {
  return `${row.group_type || "unknown"}:${row.group_value || "unknown"}`;
}

function renderAgentPerformanceRows(elementId, rows, emptyMessage) {
  if (!rows.length) {
    $(elementId).innerHTML = `<tr><td colspan="8" class="empty-state">${escapeHtml(emptyMessage)}</td></tr>`;
    return;
  }
  $(elementId).innerHTML = rows.map((row) => `
    <tr>
      <td>
        <div class="title-cell">
          <strong>${escapeHtml(row.group_value)}</strong>
          <span class="subtext">${escapeHtml(row.group_type)} | ${value(row.reviewed_run_count)} reviewed / ${value(row.run_count)} runs</span>
        </div>
      </td>
      <td>${row.performance_score === null || row.performance_score === undefined ? "n/a" : escapeHtml(row.performance_score)}</td>
      <td>${escapeHtml(percent(row.review_coverage || 0))}</td>
      <td>${escapeHtml(percent(row.useful_rate || 0))}</td>
      <td>${escapeHtml(percent(row.followup_rate || 0))}</td>
      <td>${escapeHtml(percent(row.bad_rate || 0))}</td>
      <td>${escapeHtml(percent(row.unclear_rate || 0))}</td>
      <td>
        <div class="tag-row">
          ${row.low_sample ? tag("low sample", "watch") : ""}
          ${row.disagreement_count ? tag(`${row.disagreement_count} disagree`, "watch") : ""}
        </div>
      </td>
    </tr>
  `).join("");
}

function renderAgentRunCard(item) {
  const started = formatDateTime(item.started_at);
  const completed = item.completed_at ? formatDateTime(item.completed_at) : "not completed";
  const duration = item.duration_seconds !== null && item.duration_seconds !== undefined
    ? formatDuration(item.duration_seconds)
    : "open";
  return `
    <article class="agent-run-card">
      <div class="agent-run-card-header">
        <div class="title-cell">
          <span class="work-lane">${escapeHtml(item.agent_version || "v1")}</span>
          <strong>${escapeHtml(item.agent_name)}</strong>
          <span class="subtext">${escapeHtml(shortId(item.agent_run_id))} | ${escapeHtml(started)} -> ${escapeHtml(completed)}</span>
        </div>
        <div class="tag-row">
          ${tag(item.status || "unknown", item.status || "info")}
          ${tag(item.model_profile || "model profile unset", "info")}
          ${item.source_key ? tag(item.source_key, "info") : ""}
          ${item.partition_date ? tag(item.partition_date, "info") : ""}
          ${item.errors && item.errors.length ? tag(`${item.errors.length} errors`, "failed") : ""}
        </div>
      </div>
      <div class="agent-run-metrics">
        <span>${escapeHtml(duration)}</span>
        <span>input ${escapeHtml(formatBytes(item.input_size || 0))}</span>
        <span>output ${escapeHtml(formatBytes(item.output_size || 0))}</span>
        ${item.dagster_run_id ? `<span>dagster ${escapeHtml(shortId(item.dagster_run_id))}</span>` : ""}
      </div>
      <div class="agent-run-summary">${escapeHtml(formatSummary(item.summary))}</div>
      ${renderAgentRunErrors(item.errors || [])}
      ${renderAgentReviewSummary(item.latest_reviews || [])}
      <div class="agent-review-actions">
        <input type="text" data-review-feedback="${escapeAttribute(item.agent_run_id)}" placeholder="Optional review note">
        <button type="button" data-action="agent-review" data-id="${escapeAttribute(item.agent_run_id)}" data-verdict="useful">Useful</button>
        <button type="button" data-action="agent-review" data-id="${escapeAttribute(item.agent_run_id)}" data-verdict="needs_followup">Needs Follow-up</button>
        <button type="button" data-action="agent-review" data-id="${escapeAttribute(item.agent_run_id)}" data-verdict="bad">Bad</button>
        <button type="button" data-action="agent-review" data-id="${escapeAttribute(item.agent_run_id)}" data-verdict="unclear">Unclear</button>
      </div>
      <div class="agent-run-details">
        ${renderJsonDetails("Summary", item.summary)}
        ${renderJsonDetails("Input", item.input_payload)}
        ${renderJsonDetails("Output", item.output_payload)}
        ${renderJsonDetails("Metadata", item.metadata)}
      </div>
    </article>
  `;
}

function renderAgentReviewSummary(reviews) {
  if (!reviews.length) {
    return `<div class="subtext">No operator reviews recorded yet.</div>`;
  }
  return `
    <div class="agent-review-summary">
      ${reviews.slice(0, 3).map((review) => `
        <div class="review-chip">
          ${tag(review.verdict, review.verdict)}
          <span>${escapeHtml(review.reviewer || "operator")}</span>
          <span>${escapeHtml(formatDateTime(review.created_at))}</span>
          ${review.feedback ? `<span>${escapeHtml(review.feedback)}</span>` : ""}
        </div>
      `).join("")}
    </div>
  `;
}

function renderAgentRunErrors(errors) {
  if (!errors.length) return "";
  return `
    <details open>
      <summary>Errors</summary>
      <pre>${escapeHtml(JSON.stringify(errors, null, 2))}</pre>
    </details>
  `;
}

function renderJsonDetails(label, payload) {
  if (!hasJsonPayload(payload)) return "";
  return `
    <details>
      <summary>${escapeHtml(label)}</summary>
      <pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre>
    </details>
  `;
}

function hasJsonPayload(payload) {
  if (payload === null || payload === undefined) return false;
  if (Array.isArray(payload)) return payload.length > 0;
  if (typeof payload === "object") return Object.keys(payload).length > 0;
  return String(payload).length > 0;
}

function renderBriefQueue(items) {
  if (!items.length) {
    $("briefQueueRows").innerHTML = `<tr><td colspan="6" class="empty-state">No research brief queue items found.</td></tr>`;
    return;
  }
  $("briefQueueRows").innerHTML = items.map((item) => `
    <tr>
      <td>${tag(item.status, item.status)}</td>
      <td>${escapeHtml(item.priority)}</td>
      <td>${escapeHtml(item.topic)}</td>
      <td>${escapeHtml(item.source_key || "all sources")}</td>
      <td>${escapeHtml(item.attempts)}</td>
      <td>${escapeHtml(item.last_error || "")}</td>
    </tr>
  `).join("");
}

async function handleValidationAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button || button.disabled) return;
  const action = button.dataset.action;
  const id = button.dataset.id;
  button.disabled = true;
  try {
    if (action === "approve") {
      await approveValidationRequest(id);
      showToast("Validation request approved.");
    }
    if (action === "dispatch") {
      const result = await dispatchValidationRequest(id);
      const item = result.item || {};
      showToast(validationDispatchToast(item));
    }
    await Promise.all([refreshCommandCenter(), refreshActionItems(), refreshValidationQueue(), refreshAgentRuns()]);
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    button.disabled = false;
  }
}

async function handleQueueAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button || button.disabled) return;
  const action = button.dataset.action;
  const id = button.dataset.id;
  const status = button.dataset.status;
  button.disabled = true;
  try {
    if (action === "approve") {
      await approveValidationRequest(id);
      showToast("Validation request approved.");
    }
    if (action === "dispatch") {
      const result = await dispatchValidationRequest(id);
      const item = result.item || {};
      showToast(validationDispatchToast(item));
    }
    if (action === "lead-status") {
      await updateResearchLeadStatus(id, status);
      showToast(`Research lead moved to ${status}.`);
    }
    if (action === "escalate-agent-finding") {
      const result = await escalateAgentFinding(id);
      showToast(`Created ${result.research_leads_created || 0} lead(s), ${result.source_queries_created || 0} source quer${(result.source_queries_created || 0) === 1 ? "y" : "ies"}.`);
    }
    if (action === "research-followup-loop") {
      const result = await runResearchFollowupLoop(id, status);
      showToast(followupLoopToast(result, status));
    }
    if (action === "refine-followup") {
      const result = await refineResearchFollowup(id);
      showToast(`Created ${result.source_queries_created || 0} refined source quer${(result.source_queries_created || 0) === 1 ? "y" : "ies"}.`);
    }
    await Promise.all([
      refreshCommandCenter(),
      refreshActionItems(),
      refreshValidationQueue(),
      refreshResearchLeads(),
      refreshAgentRuns(),
    ]);
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    button.disabled = false;
  }
}

async function runResearchFollowupLoop(leadId, mode) {
  const evaluate = mode === "evaluate";
  return postJson(`/api/research-leads/${encodeURIComponent(leadId)}/followup-loop`, {
    ingest: !evaluate,
    resolve: evaluate,
    evaluate,
    dry_run: false,
    limit_per_query: evaluate ? 1 : 2,
    max_queries: 10,
    followup_lane: "agent_evaluator_followup",
    model_profile: "agent_performance_evaluator",
    operator: $("operatorName").value.trim() || "command_center_operator",
  });
}

async function refineResearchFollowup(leadId) {
  return postJson(`/api/research-leads/${encodeURIComponent(leadId)}/refine-followup`, {
    dry_run: false,
    max_queries_per_review: 4,
    operator: $("operatorName").value.trim() || "command_center_operator",
  });
}

function followupLoopToast(result, mode) {
  if (mode === "evaluate") {
    const verdict = result.latest_evaluator_verdict ? ` verdict ${result.latest_evaluator_verdict}` : "";
    return `Re-evaluated lead.${verdict} est $${Number(result.estimated_cost_usd || 0).toFixed(4)} spent $${Number(result.actual_cost_usd || 0).toFixed(4)}.`;
  }
  const fit = evidenceFitToast(result.evidence_fit);
  const linkedFollowups = Number(result.source_followups_linked || result.source_followups_queued || 0);
  const newFollowups = Number(result.source_followups_newly_queued || result.source_followups_queued || 0);
  const alreadyIngestedFollowups = Number(result.source_followups_already_ingested || 0);
  const pendingFollowups = Number(result.source_followups_pending || 0);
  const ingestedThisRun = Number(result.source_followups_ingested_this_run || result.source_followups_ingested || 0);
  const sourceFollowups = linkedFollowups
    ? ` Identifier follow-ups: ${linkedFollowups} linked, ${newFollowups} new, ${alreadyIngestedFollowups} already ingested, ${pendingFollowups} pending, ${ingestedThisRun} ingested this run.`
    : "";
  const claims = result.claim_chunks_seen
    ? ` Claims written: ${result.claims_written || 0}.`
    : "";
  return `Ran ${result.query_count || 0} follow-up quer${(result.query_count || 0) === 1 ? "y" : "ies"}; added ${result.document_chunks || 0} chunk(s).${sourceFollowups}${claims}${fit}`;
}

function evidenceFitToast(evidenceFit) {
  if (!evidenceFit || !evidenceFit.fit) {
    return "";
  }
  const details = [
    evidenceFit.target_safety_fit ? `target ${evidenceFit.target_safety_fit}` : "",
    evidenceFit.disease_directness_fit ? `disease ${evidenceFit.disease_directness_fit}` : "",
    evidenceFit.actionability ? `action ${evidenceFit.actionability}` : "",
    evidenceFit.transfer_risk ? `risk ${evidenceFit.transfer_risk}` : "",
  ].filter(Boolean);
  return ` Evidence fit: ${evidenceFit.fit}${details.length ? ` (${details.join(", ")})` : ""}.`;
}

async function escalateAgentFinding(itemId) {
  const reviewId = String(itemId || "").replace(/^agent-review:/, "");
  return postJson("/api/agent-findings/escalate", {
    review_id: reviewId,
    operator: $("operatorName").value.trim() || "command_center_operator",
    dry_run: false,
    create_research_leads: true,
    create_source_queries: true,
  });
}

async function handleAgentRunReview(event) {
  const button = event.target.closest("button[data-action='agent-review']");
  if (!button || button.disabled) return;
  const agentRunId = button.dataset.id;
  const verdict = button.dataset.verdict;
  const feedbackInput = button.closest(".agent-run-card")?.querySelector("input[data-review-feedback]");
  button.disabled = true;
  try {
    await postJson(`/api/agent-runs/${encodeURIComponent(agentRunId)}/reviews`, {
      verdict,
      feedback: feedbackInput ? feedbackInput.value.trim() : "",
      reviewer: $("operatorName").value.trim() || "command_center_operator",
      operator: $("operatorName").value.trim() || "command_center_operator",
    });
    if (feedbackInput) feedbackInput.value = "";
    showToast(`Agent run marked ${verdict}.`);
    await Promise.all([refreshAgentRuns(), refreshAgentPerformance()]);
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    button.disabled = false;
  }
}

async function runAgentPerformanceEvaluation() {
  const button = $("agentPerformanceEvaluateButton");
  if (!button || button.disabled) return;
  button.disabled = true;
  try {
    const payload = await postJson("/api/agent-performance/evaluate", {
      limit: Number($("agentPerformanceEvalLimit").value || 25),
      reviewed_only: true,
      model_profile: "agent_performance_evaluator",
      operator: $("operatorName").value.trim() || "command_center_operator",
    });
    showToast(`Evaluator created ${payload.review_created_count || 0} review(s).`);
    await Promise.all([refreshAgentPerformance(), refreshAgentRuns()]);
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    button.disabled = validationDispatchReady() ? false : true;
  }
}

async function runValidationAutopilot(dryRun) {
  const button = dryRun ? $("autopilotDryRunButton") : $("autopilotRunButton");
  if (!button || button.disabled) return;
  button.disabled = true;
  try {
    const payload = await postJson("/api/validation-autopilot/run", {
      enabled: true,
      dry_run: dryRun,
      force: $("autopilotForce").checked,
      max_per_run: Number($("autopilotMaxPerRun").value || 2),
      manual_grace_period_hours: Number($("autopilotGraceHours").value || 6),
      model_profile: "openrouter_required",
      operator: $("operatorName").value.trim() || "command_center_operator",
    });
    state.validationAutopilot = payload;
    renderValidationAutopilot(payload);
    showToast(dryRun ? "Autopilot dry run recorded." : `Autopilot dispatched ${payload.dispatched_count || 0} item(s).`);
    await Promise.all([
      refreshCommandCenter(),
      refreshActionItems(),
      refreshValidationQueue(),
      refreshValidationAutopilot(),
      refreshAgentRuns(),
    ]);
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    button.disabled = dryRun ? false : validationDispatchReady() ? false : true;
  }
}

function approveValidationRequest(id) {
  return postJson(`/api/validation-requests/${encodeURIComponent(id)}/approve`, {
    approved_by: $("operatorName").value.trim() || "command_center_operator",
  });
}

function dispatchValidationRequest(id) {
  return postJson(`/api/validation-requests/${encodeURIComponent(id)}/dispatch`, {
    model_profile: "openrouter_required",
  });
}

function validationDispatchToast(item) {
  if (item.status === "completed") return "Validation agent completed.";
  if (item.status === "blocked") return "Dispatch blocked. Missing context is listed in the queue.";
  if (item.status === "failed") return "Validation agent failed. Error is listed in the queue.";
  return "Validation request dispatched.";
}

function updateResearchLeadStatus(id, status) {
  return postJson(`/api/research-leads/${encodeURIComponent(id)}/status`, {
    status,
    operator: $("operatorName").value.trim() || "command_center_operator",
  });
}

async function getJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  return readJsonResponse(response);
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  return readJsonResponse(response);
}

async function readJsonResponse(response) {
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || `Request failed with ${response.status}`);
  }
  return payload;
}

function tag(label, tone) {
  return `<span class="tag ${escapeHtml(tone || "")}">${escapeHtml(label || "unknown")}</span>`;
}

function actionButton(action, id, label, status = "", disabled = false) {
  return `<button type="button" data-action="${escapeAttribute(action)}" data-id="${escapeAttribute(id)}" data-status="${escapeAttribute(status)}" ${disabled ? "disabled" : ""}>${escapeHtml(label)}</button>`;
}

function value(input) {
  return input === null || input === undefined ? "0" : String(input);
}

function percent(input) {
  return `${Math.round(Number(input || 0) * 100)}%`;
}

function shortId(input) {
  const value = String(input || "");
  return value.length > 12 ? `${value.slice(0, 8)}...` : value;
}

function trimText(input, length) {
  const value = String(input || "").trim();
  return value.length > length ? `${value.slice(0, length - 3)}...` : value;
}

function formatDateTime(input) {
  if (!input) return "unknown date";
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return String(input);
  return date.toLocaleString();
}

function formatDuration(seconds) {
  const value = Number(seconds || 0);
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  if (value < 60) return `${value.toFixed(1)} sec`;
  if (value < 3600) return `${Math.floor(value / 60)} min ${Math.round(value % 60)} sec`;
  return `${Math.floor(value / 3600)} hr ${Math.round((value % 3600) / 60)} min`;
}

function formatBytes(bytes) {
  const value = Number(bytes || 0);
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatSummary(input) {
  if (!input) return "No summary";
  if (typeof input === "string") return input;
  if (Array.isArray(input)) return input.join(", ");
  const pairs = Object.entries(input)
    .slice(0, 5)
    .map(([key, val]) => `${key}: ${formatSummaryValue(val)}`);
  return pairs.length ? pairs.join(" | ") : "No summary";
}

function formatSummaryValue(input) {
  if (input === null || input === undefined) return "";
  if (typeof input === "object") return JSON.stringify(input);
  return String(input);
}

function escapeHtml(input) {
  return String(input === null || input === undefined ? "" : input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttribute(input) {
  return escapeHtml(input).replaceAll("`", "&#096;");
}

function debounce(fn, delay) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

let toastTimer = null;
function showToast(message) {
  const toast = $("toast");
  toast.textContent = message;
  toast.classList.add("visible");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("visible"), 3200);
}
