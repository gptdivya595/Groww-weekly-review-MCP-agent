export type ReadinessCheck = {
  key: string;
  label: string;
  status: string;
  detail: string;
};

export type RunSummary = {
  run_id: string;
  product_slug: string;
  iso_week: string;
  stage: string;
  status: string;
  lookback_weeks: number;
  started_at: string;
  completed_at: string | null;
  docs_status: string | null;
  gmail_status: string | null;
  warning: string | null;
  summary_path: string | null;
};

export type ProductStatus = {
  slug: string;
  display_name: string;
  active: boolean;
  default_lookback_weeks: number;
  app_store_configured: boolean;
  play_store_configured: boolean;
  stakeholder_count: number;
  google_doc_configured: boolean;
  issues: string[];
  latest_run: RunSummary | null;
};

export type PhaseStatus = {
  phase: string;
  title: string;
  implementation_status: string;
  end_to_end_status: string;
  notes: string[];
};

export type CompletionAudit = {
  overall_status: string;
  notes: string[];
  phases: PhaseStatus[];
};

export type ServiceStatus = {
  key: string;
  label: string;
  category: string;
  status: string;
  detail: string;
  checked_at: string;
  active: boolean;
  product_slug: string | null;
  run_id: string | null;
  latency_ms: number | null;
};

export type IssueSnapshot = {
  issue_id: string;
  severity: string;
  source: string;
  title: string;
  detail: string;
  observed_at: string;
  product_slug: string | null;
  run_id: string | null;
};

export type LockSnapshot = {
  key: string;
  status: string;
  product_slug: string;
  iso_week: string;
  path: string;
  acquired_at: string | null;
  age_seconds: number | null;
  pid: number | null;
  detail: string;
};

export type SchedulerStatus = {
  enabled: boolean;
  mode: string;
  status: string;
  timezone: string;
  cadence: string;
  detail: string;
  next_run_at: string | null;
  last_started_at: string | null;
  last_success_at: string | null;
};

export type DashboardStats = {
  active_products: number;
  active_services: number;
  running_jobs: number;
  recorded_deliveries: number;
  ready_services: number;
  warning_services: number;
  failed_services: number;
  open_issues: number;
  active_locks: number;
  failed_runs_last_24h: number;
};

export type JobItem = {
  product_slug: string;
  status: string;
  run_id: string | null;
  summary_path: string | null;
  error: string | null;
};

export type JobSnapshot = {
  job_id: string;
  kind: string;
  status: string;
  submitted_at: string;
  started_at: string | null;
  completed_at: string | null;
  iso_week: string | null;
  product_slug: string | null;
  target: string;
  run_id: string | null;
  summary_path: string | null;
  error: string | null;
  items: JobItem[];
};

export type OverviewResponse = {
  checked_at: string;
  stats: DashboardStats;
  scheduler: SchedulerStatus;
  services: ServiceStatus[];
  issues: IssueSnapshot[];
  locks: LockSnapshot[];
  readiness: ReadinessCheck[];
  completion: CompletionAudit;
  products: ProductStatus[];
  recent_runs: RunSummary[];
  jobs: JobSnapshot[];
};

export type DeliverySummary = {
  target: string;
  status: string;
  external_id: string | null;
  external_link: string | null;
  payload_hash: string | null;
  updated_at: string;
};

export type RunDetail = {
  run: RunSummary;
  deliveries: DeliverySummary[];
  audit: Record<string, unknown>;
};

type TriggerRunRequest = {
  product_slug: string;
  iso_week?: string;
  weeks?: number;
  target?: "docs" | "gmail" | "all";
};

type TriggerWeeklyRequest = {
  iso_week?: string;
  weeks?: number;
  target?: "docs" | "gmail" | "all";
};

const API_BASE_URL = (
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000"
).replace(/\/$/, "");

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(`${API_BASE_URL}${path}`, {
    cache: "no-store",
    ...init,
    headers,
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `API request failed with ${response.status}`);
  }

  return (await response.json()) as T;
}

export async function fetchOverview(): Promise<OverviewResponse> {
  return request<OverviewResponse>("/api/overview");
}

export async function fetchRunDetail(runId: string): Promise<RunDetail> {
  return request<RunDetail>(`/api/runs/${runId}`);
}

export async function fetchJobs(): Promise<JobSnapshot[]> {
  return request<JobSnapshot[]>("/api/jobs");
}

export async function triggerRun(
  payload: TriggerRunRequest,
): Promise<JobSnapshot> {
  return request<JobSnapshot>("/api/triggers/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function triggerWeekly(
  payload: TriggerWeeklyRequest,
): Promise<JobSnapshot> {
  return request<JobSnapshot>("/api/triggers/weekly", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function apiBaseUrl(): string {
  return API_BASE_URL;
}
