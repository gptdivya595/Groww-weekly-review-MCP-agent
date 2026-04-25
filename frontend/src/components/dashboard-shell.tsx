"use client";

import { StatusBadge } from "@/components/status-badge";
import {
  type CompletionAudit,
  type IssueSnapshot,
  type JobSnapshot,
  type LockSnapshot,
  type OverviewResponse,
  type ProductStatus,
  type RunDetail,
  type RunSummary,
  type SchedulerStatus,
  type ServiceStatus,
  apiBaseUrl,
  fetchOverview,
  fetchRunDetail,
  triggerRun,
} from "@/lib/api";
import { useDeferredValue, useEffect, useMemo, useState, useTransition } from "react";

const DATE_FORMATTER = new Intl.DateTimeFormat("en-IN", {
  dateStyle: "medium",
  timeStyle: "short",
});

const NUMBER_FORMATTER = new Intl.NumberFormat("en-IN");
const VISIBLE_PRODUCT_SLUG = "groww";
const VISIBLE_PRODUCT_NAME = "Groww";
const DEFAULT_LOOKBACK_WEEKS = 10;
const RUNNING_JOB_STATUSES = new Set(["queued", "running"]);

function formatDate(value: string | null) {
  if (!value) {
    return "Not available";
  }

  return DATE_FORMATTER.format(new Date(value));
}

function formatRelativeTime(value: string | null) {
  if (!value) {
    return "No timestamp yet";
  }

  const deltaMs = new Date(value).getTime() - Date.now();
  const deltaMinutes = Math.round(deltaMs / 60000);
  const formatter = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

  if (Math.abs(deltaMinutes) < 60) {
    return formatter.format(deltaMinutes, "minute");
  }

  const deltaHours = Math.round(deltaMinutes / 60);
  if (Math.abs(deltaHours) < 48) {
    return formatter.format(deltaHours, "hour");
  }

  const deltaDays = Math.round(deltaHours / 24);
  return formatter.format(deltaDays, "day");
}

function formatAgeSeconds(value: number | null) {
  if (value === null) {
    return "Unknown age";
  }
  if (value < 60) {
    return `${value}s old`;
  }
  if (value < 3600) {
    return `${Math.round(value / 60)}m old`;
  }
  if (value < 86400) {
    return `${Math.round(value / 3600)}h old`;
  }
  return `${Math.round(value / 86400)}d old`;
}

function humanizeSlug(value: string) {
  return value.replaceAll("-", " ");
}

function targetLabel(value: "all" | "docs" | "gmail") {
  if (value === "docs") {
    return "Docs publish";
  }
  if (value === "gmail") {
    return "Gmail publish";
  }
  return "End-to-end flow";
}

function isVisibleProductSlug(value: string | null | undefined) {
  return value === VISIBLE_PRODUCT_SLUG;
}

function findVisibleProduct(products: ProductStatus[]) {
  return products.find((product) => isVisibleProductSlug(product.slug)) ?? null;
}

function findLatestVisibleRun(runs: RunSummary[]) {
  return runs.find((run) => isVisibleProductSlug(run.product_slug)) ?? null;
}

function filterVisibleIssues(issues: IssueSnapshot[]) {
  return issues.filter((issue) => {
    if (issue.product_slug) {
      return isVisibleProductSlug(issue.product_slug);
    }

    return issue.source !== "readiness";
  });
}

function filterVisibleLocks(locks: LockSnapshot[]) {
  return locks.filter((lock) => isVisibleProductSlug(lock.product_slug));
}

function filterVisibleJobs(jobs: JobSnapshot[]) {
  return jobs.flatMap((job) => {
    const visibleItems = job.items.filter((item) =>
      isVisibleProductSlug(item.product_slug),
    );

    if (job.product_slug && !isVisibleProductSlug(job.product_slug)) {
      return [];
    }

    if (!job.product_slug && visibleItems.length === 0) {
      return [];
    }

    return [
      {
        ...job,
        product_slug: job.product_slug ?? VISIBLE_PRODUCT_SLUG,
        items: visibleItems,
      },
    ];
  });
}

function filterVisibleReadiness(readiness: OverviewResponse["readiness"]) {
  return readiness.filter((check) => check.key !== "products");
}

export function DashboardShell() {
  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const deferredSelectedRunId = useDeferredValue(selectedRunId);
  const [runDetail, setRunDetail] = useState<RunDetail | null>(null);
  const [loadingOverview, setLoadingOverview] = useState(true);
  const [runDetailStatus, setRunDetailStatus] = useState<
    "idle" | "loading" | "loaded" | "error"
  >("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [flashMessage, setFlashMessage] = useState<string | null>(null);
  const [activeActionKey, setActiveActionKey] = useState<string | null>(null);
  const [selectedProductSlug, setSelectedProductSlug] =
    useState<string | null>(VISIBLE_PRODUCT_SLUG);
  const [selectedTarget, setSelectedTarget] = useState<"all" | "docs" | "gmail">("all");
  const [requestedWeeks, setRequestedWeeks] = useState(DEFAULT_LOOKBACK_WEEKS);
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    let cancelled = false;

    async function loadOverviewSnapshot() {
      try {
        const nextOverview = await fetchOverview();
        if (cancelled) {
          return;
        }

        setOverview(nextOverview);
        setErrorMessage(null);

        startTransition(() => {
          const visibleRun =
            nextOverview.recent_runs.find(
              (run) =>
                run.run_id === selectedRunId && isVisibleProductSlug(run.product_slug),
            ) ?? null;
          const latestVisibleRun = findLatestVisibleRun(nextOverview.recent_runs);

          if (!visibleRun) {
            if (latestVisibleRun) {
              setRunDetail(null);
              setRunDetailStatus("loading");
              setSelectedRunId(latestVisibleRun.run_id);
            } else if (selectedRunId) {
              setRunDetail(null);
              setRunDetailStatus("idle");
              setSelectedRunId(null);
            }
          }

          const visibleProduct = findVisibleProduct(nextOverview.products);

          if (visibleProduct && selectedProductSlug !== visibleProduct.slug) {
            setSelectedProductSlug(visibleProduct.slug);
            setRequestedWeeks(visibleProduct.default_lookback_weeks);
          } else if (!visibleProduct && selectedProductSlug) {
            setSelectedProductSlug(null);
            setRequestedWeeks(DEFAULT_LOOKBACK_WEEKS);
          }
        });
      } catch (error) {
        if (cancelled) {
          return;
        }

        const message =
          error instanceof Error
            ? error.message
            : "Unable to reach the Pulse backend API.";
        setErrorMessage(message);
      } finally {
        if (!cancelled) {
          setLoadingOverview(false);
        }
      }
    }

    void loadOverviewSnapshot();
    const intervalId = window.setInterval(() => {
      void loadOverviewSnapshot();
    }, 10000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [selectedProductSlug, selectedRunId, startTransition]);

  useEffect(() => {
    if (!deferredSelectedRunId) {
      return;
    }

    const runId = deferredSelectedRunId;
    let cancelled = false;

    async function loadRunDetailSnapshot() {
      try {
        const detail = await fetchRunDetail(runId);
        if (cancelled) {
          return;
        }

        if (!isVisibleProductSlug(detail.run.product_slug)) {
          setRunDetail(null);
          setRunDetailStatus("idle");
          return;
        }

        setRunDetail(detail);
        setRunDetailStatus("loaded");
        setErrorMessage(null);
      } catch (error) {
        if (cancelled) {
          return;
        }

        const message =
          error instanceof Error ? error.message : "Unable to load run detail.";
        setRunDetail(null);
        setRunDetailStatus("error");
        setErrorMessage(message);
      }
    }

    void loadRunDetailSnapshot();
    return () => {
      cancelled = true;
    };
  }, [deferredSelectedRunId]);

  const visibleProducts = useMemo(() => {
    if (!overview) {
      return [];
    }

    return overview.products.filter((product) => isVisibleProductSlug(product.slug));
  }, [overview]);

  const visibleRecentRuns = useMemo(() => {
    if (!overview) {
      return [];
    }

    return overview.recent_runs.filter((run) => isVisibleProductSlug(run.product_slug));
  }, [overview]);

  const visibleIssues = useMemo(() => {
    if (!overview) {
      return [];
    }

    return filterVisibleIssues(overview.issues);
  }, [overview]);

  const visibleLocks = useMemo(() => {
    if (!overview) {
      return [];
    }

    return filterVisibleLocks(overview.locks);
  }, [overview]);

  const visibleJobs = useMemo(() => {
    if (!overview) {
      return [];
    }

    return filterVisibleJobs(overview.jobs);
  }, [overview]);

  const visibleReadiness = useMemo(() => {
    if (!overview) {
      return [];
    }

    return filterVisibleReadiness(overview.readiness);
  }, [overview]);

  const visibleStats = useMemo(
    () => ({
      activeProducts: visibleProducts.filter((product) => product.active).length,
      runningJobs: visibleJobs.filter((job) => RUNNING_JOB_STATUSES.has(job.status)).length,
      openIssues: visibleIssues.length,
    }),
    [visibleIssues, visibleJobs, visibleProducts],
  );

  const selectedProduct = useMemo(() => {
    if (!selectedProductSlug) {
      return null;
    }

    return (
      visibleProducts.find((product) => product.slug === selectedProductSlug) ?? null
    );
  }, [selectedProductSlug, visibleProducts]);

  const selectedProductDisplayName = useMemo(() => {
    if (selectedProduct) {
      return selectedProduct.display_name;
    }

    if (selectedProductSlug === VISIBLE_PRODUCT_SLUG) {
      return VISIBLE_PRODUCT_NAME;
    }

    return null;
  }, [selectedProduct, selectedProductSlug]);

  const servicesByCategory = useMemo(() => {
    if (!overview) {
      return [];
    }

    const visibleProduct = findVisibleProduct(overview.products);
    const services = overview.services
      .filter((service) => !service.product_slug || isVisibleProductSlug(service.product_slug))
      .map((service) => {
        if (service.key !== "ingestion_agent" || !visibleProduct) {
          return service;
        }

        const reviewSourcesConfigured =
          visibleProduct.app_store_configured && visibleProduct.play_store_configured;

        return {
          ...service,
          status: reviewSourcesConfigured ? "ready" : "warning",
          detail: reviewSourcesConfigured
            ? `${visibleProduct.display_name} has both App Store and Google Play identifiers configured.`
            : `${visibleProduct.display_name} is still missing one or more review source identifiers.`,
        };
      });

    const order = ["platform", "pipeline", "mcp", "ops"];
    return order
      .map((category) => ({
        category,
        services: services.filter((service) => service.category === category),
      }))
      .filter((group) => group.services.length > 0);
  }, [overview]);

  function selectRun(runId: string) {
    startTransition(() => {
      setRunDetail(null);
      setRunDetailStatus("loading");
      setSelectedRunId(runId);
    });
  }

  function handleProductSelection(nextSlug: string) {
    setSelectedProductSlug(nextSlug);
    const product = visibleProducts.find((item) => item.slug === nextSlug);
    if (product) {
      setRequestedWeeks(product.default_lookback_weeks);
    }
  }

  async function refreshOverviewNow() {
    const nextOverview = await fetchOverview();
    setOverview(nextOverview);
    setErrorMessage(null);
  }

  async function handleTriggerProduct(product: ProductStatus) {
    const actionKey = `product:${product.slug}`;
    try {
      setActiveActionKey(actionKey);
      const job = await triggerRun({
        product_slug: product.slug,
        weeks: product.default_lookback_weeks,
        target: "all",
      });
      setFlashMessage(
        `Queued an end-to-end flow for ${product.display_name} as job ${job.job_id.slice(0, 8)}.`,
      );
      if (job.run_id) {
        selectRun(job.run_id);
      }
      await refreshOverviewNow();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to queue the product run.";
      setErrorMessage(message);
    } finally {
      setActiveActionKey(null);
    }
  }

  async function handleTriggerOneShot() {
    if (!selectedProduct) {
      setErrorMessage("Choose a product before queuing a one-shot flow.");
      return;
    }

    const actionKey = "one-shot";
    try {
      setActiveActionKey(actionKey);
      const job = await triggerRun({
        product_slug: selectedProduct.slug,
        weeks: requestedWeeks,
        target: selectedTarget,
      });
      setFlashMessage(
        `Queued ${targetLabel(selectedTarget).toLowerCase()} for ${selectedProduct.display_name} as job ${job.job_id.slice(0, 8)}.`,
      );
      if (job.run_id) {
        selectRun(job.run_id);
      }
      await refreshOverviewNow();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to queue the one-shot flow.";
      setErrorMessage(message);
    } finally {
      setActiveActionKey(null);
    }
  }

  async function handleTriggerWeekly() {
    const product = selectedProduct ?? visibleProducts[0] ?? null;

    if (!product) {
      setErrorMessage("Groww is not configured for the weekly pulse yet.");
      return;
    }

    const actionKey = "weekly";
    try {
      setActiveActionKey(actionKey);
      const job = await triggerRun({
        product_slug: product.slug,
        weeks: product.default_lookback_weeks,
        target: "all",
      });
      setFlashMessage(
        `Queued the weekly pulse for ${product.display_name} as job ${job.job_id.slice(0, 8)}.`,
      );
      if (job.run_id) {
        selectRun(job.run_id);
      }
      await refreshOverviewNow();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to queue the weekly pulse.";
      setErrorMessage(message);
    } finally {
      setActiveActionKey(null);
    }
  }

  return (
    <main className="flex flex-1">
      <div className="mx-auto flex w-full max-w-[92rem] flex-1 flex-col gap-8 px-5 py-8 sm:px-8 lg:px-10">
        <header className="overflow-hidden rounded-[2rem] border border-slate-900/10 bg-[var(--card-strong)] shadow-[var(--shadow)] backdrop-blur">
          <div className="grid gap-0 lg:grid-cols-[1.2fr_0.8fr]">
            <div className="p-6 sm:p-8">
              <p className="mb-3 text-xs font-semibold uppercase tracking-[0.38em] text-[var(--muted)]">
                Weekly Product Review Pulse
              </p>
              <div className="flex flex-wrap items-center gap-3">
                <h1 className="text-3xl font-semibold tracking-[-0.05em] text-slate-950 sm:text-5xl">
                  Operations Dashboard
                </h1>
                <StatusBadge
                  value={overview?.completion.overall_status ?? "loading"}
                />
              </div>
              <p className="mt-4 max-w-3xl text-sm leading-7 text-[var(--muted)] sm:text-base">
                Monitor the backend, MCP delivery layer, scheduler posture, product
                readiness, active jobs, lock state, and issue backlog from one
                command center. Use the one-shot controls to run the complete review
                ingestion to Docs or Gmail flow on demand.
              </p>

              <div className="mt-6 flex flex-wrap gap-3">
                <div className="rounded-2xl border border-slate-900/10 bg-white/70 px-4 py-3 text-sm text-[var(--muted)]">
                  <div className="font-mono text-xs uppercase tracking-[0.3em]">
                    API endpoint
                  </div>
                  <div className="mt-1 truncate font-semibold text-slate-900">
                    {apiBaseUrl()}
                  </div>
                </div>
                <div className="rounded-2xl border border-slate-900/10 bg-white/70 px-4 py-3 text-sm text-[var(--muted)]">
                  <div className="font-mono text-xs uppercase tracking-[0.3em]">
                    Last refresh
                  </div>
                  <div className="mt-1 font-semibold text-slate-900">
                    {overview ? formatDate(overview.checked_at) : "Loading"}
                  </div>
                </div>
              </div>
            </div>

            <div className="border-t border-slate-900/10 bg-slate-950/[0.04] p-6 sm:p-8 lg:border-l lg:border-t-0">
              <p className="text-xs font-semibold uppercase tracking-[0.34em] text-[var(--muted)]">
                Command Center
              </p>
              <h2 className="mt-2 text-2xl font-semibold tracking-[-0.04em] text-slate-950">
                Queue a one-shot flow
              </h2>
              <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                Pick a product, choose the delivery target, and run the pipeline
                immediately without waiting for a weekly schedule.
              </p>

              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                <label className="space-y-2 text-sm">
                  <span className="font-semibold text-slate-900">Product</span>
                  <select
                    value={selectedProductSlug ?? VISIBLE_PRODUCT_SLUG}
                    onChange={(event) => {
                      handleProductSelection(event.target.value);
                    }}
                    disabled={visibleProducts.length <= 1}
                    className="w-full rounded-2xl border border-slate-900/10 bg-white px-4 py-3 text-slate-950 outline-none ring-0 transition focus:border-slate-900/30"
                  >
                    {visibleProducts.length ? (
                      visibleProducts.map((product) => (
                        <option key={product.slug} value={product.slug}>
                          {product.display_name}
                        </option>
                      ))
                    ) : (
                      <option value={VISIBLE_PRODUCT_SLUG}>{VISIBLE_PRODUCT_NAME}</option>
                    )}
                  </select>
                </label>

                <label className="space-y-2 text-sm">
                  <span className="font-semibold text-slate-900">Target</span>
                  <select
                    value={selectedTarget}
                    onChange={(event) => {
                      setSelectedTarget(event.target.value as "all" | "docs" | "gmail");
                    }}
                    className="w-full rounded-2xl border border-slate-900/10 bg-white px-4 py-3 text-slate-950 outline-none ring-0 transition focus:border-slate-900/30"
                  >
                    <option value="all">End-to-end flow</option>
                    <option value="docs">Docs publish only</option>
                    <option value="gmail">Gmail publish only</option>
                  </select>
                </label>

                <label className="space-y-2 text-sm">
                  <span className="font-semibold text-slate-900">Lookback weeks</span>
                  <input
                    min={1}
                    max={24}
                    type="number"
                    value={requestedWeeks}
                    onChange={(event) => {
                      setRequestedWeeks(Number(event.target.value));
                    }}
                    className="w-full rounded-2xl border border-slate-900/10 bg-white px-4 py-3 text-slate-950 outline-none ring-0 transition focus:border-slate-900/30"
                  />
                </label>

                <div className="rounded-2xl border border-slate-900/10 bg-white/80 px-4 py-3 text-sm text-[var(--muted)]">
                  <div className="font-semibold text-slate-900">Selected action</div>
                  <div className="mt-1">
                    {selectedProductDisplayName
                      ? `${targetLabel(selectedTarget)} for ${selectedProductDisplayName}`
                      : "Choose a product"}
                  </div>
                </div>
              </div>

              <div className="mt-5 flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={() => {
                    void handleTriggerOneShot();
                  }}
                  disabled={activeActionKey === "one-shot"}
                  className="rounded-2xl bg-[var(--accent)] px-5 py-4 text-sm font-semibold text-white transition hover:translate-y-[-1px] hover:shadow-lg disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {activeActionKey === "one-shot"
                    ? "Queueing One-Shot..."
                    : "Run One-Shot Flow"}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    void handleTriggerWeekly();
                  }}
                  disabled={activeActionKey === "weekly"}
                  className="rounded-2xl border border-slate-900/10 bg-white px-5 py-4 text-sm font-semibold text-slate-900 transition hover:translate-y-[-1px] hover:bg-slate-900/5 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {activeActionKey === "weekly"
                    ? "Queueing Weekly Pulse..."
                    : "Run Weekly Pulse"}
                </button>
              </div>
            </div>
          </div>
        </header>

        {flashMessage ? (
          <div className="rounded-2xl border border-emerald-700/15 bg-emerald-50 px-4 py-3 text-sm text-emerald-950">
            {flashMessage}
          </div>
        ) : null}

        {errorMessage ? (
          <div className="rounded-2xl border border-rose-700/15 bg-rose-50 px-4 py-3 text-sm text-rose-950">
            {errorMessage}
          </div>
        ) : null}

        <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-6">
          <MetricCard
            label="Active Products"
            value={NUMBER_FORMATTER.format(visibleStats.activeProducts)}
            note="Products visible in this dashboard."
          />
          <MetricCard
            label="Live Services"
            value={overview ? NUMBER_FORMATTER.format(overview.stats.active_services) : "0"}
            note="Services or agents currently active right now."
          />
          <MetricCard
            label="Running Jobs"
            value={NUMBER_FORMATTER.format(visibleStats.runningJobs)}
            note="Queued or running Groww jobs in the current API process."
          />
          <MetricCard
            label="Open Issues"
            value={NUMBER_FORMATTER.format(visibleStats.openIssues)}
            note="Warnings and errors visible in the Groww view."
          />
          <MetricCard
            label="Recorded Deliveries"
            value={overview ? NUMBER_FORMATTER.format(overview.stats.recorded_deliveries) : "0"}
            note="Persisted Docs and Gmail delivery records in SQLite."
          />
          <MetricCard
            label="Next Scheduled Run"
            value={overview?.scheduler.next_run_at ? formatRelativeTime(overview.scheduler.next_run_at) : "Manual"}
            note={overview?.scheduler.cadence ?? "Scheduler metadata is still loading."}
          />
        </section>

        <section className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="flex flex-col gap-6">
            <Panel title="Service Health" eyebrow="Live platform and agent state">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading live service health..." />
              ) : (
                <ServiceGroups groups={servicesByCategory} />
              )}
            </Panel>

            <Panel title="Product Fleet" eyebrow="Per-product operational state">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading product fleet..." />
              ) : visibleProducts.length ? (
                <div className="grid gap-4 lg:grid-cols-2">
                  {visibleProducts.map((product) => (
                    <ProductCard
                      key={product.slug}
                      product={product}
                      onRun={() => {
                        void handleTriggerProduct(product);
                      }}
                      isBusy={activeActionKey === `product:${product.slug}`}
                      onInspectRun={(run) => {
                        selectRun(run.run_id);
                      }}
                    />
                  ))}
                </div>
              ) : (
                <Placeholder copy="Groww has not been configured for this dashboard yet." />
              )}
            </Panel>

            <Panel title="Recent Runs" eyebrow="End-to-end execution history">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading recent run history..." />
              ) : (
                <RunTable
                  runs={visibleRecentRuns}
                  selectedRunId={selectedRunId}
                  onSelectRun={(run) => {
                    selectRun(run.run_id);
                  }}
                />
              )}
            </Panel>
          </div>

          <div className="flex flex-col gap-6">
            <Panel title="Scheduler" eyebrow="Cadence, posture, and next run">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading scheduler state..." />
              ) : overview ? (
                <SchedulerPanel scheduler={overview.scheduler} />
              ) : null}
            </Panel>

            <Panel title="Warnings And Errors" eyebrow="Tracker">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading issue tracker..." />
              ) : (
                <IssueFeed issues={visibleIssues} />
              )}
            </Panel>

            <Panel title="Active Locks" eyebrow="Current lock visibility">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading run locks..." />
              ) : (
                <LockList locks={visibleLocks} />
              )}
            </Panel>

            <Panel title="Program Status" eyebrow="Phase and readiness audit">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading readiness and phase audit..." />
              ) : overview ? (
                <ProgramStatusPanel
                  audit={overview.completion}
                  readiness={visibleReadiness}
                />
              ) : null}
            </Panel>

            <Panel title="Triggered Jobs" eyebrow="Background queue">
              {loadingOverview && !overview ? (
                <Placeholder copy="Loading queued jobs..." />
              ) : (
                <JobList jobs={visibleJobs} />
              )}
            </Panel>

            <Panel title="Run Detail" eyebrow="Deliveries and audit payload">
              {runDetailStatus === "loading" ? (
                <Placeholder copy="Loading run detail..." />
              ) : runDetail ? (
                <RunDetailPanel detail={runDetail} />
              ) : (
                <Placeholder copy="Select a run to inspect its deliveries and audit payload." />
              )}
            </Panel>
          </div>
        </section>

        <footer className="pb-4 text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
          Polling every 10 seconds | One-shot flow controls available |{" "}
          {isPending ? "refreshing view..." : "view stable"}
        </footer>
      </div>
    </main>
  );
}

function MetricCard({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="rounded-[1.6rem] border border-slate-900/10 bg-[var(--card)] p-5 shadow-[var(--shadow)] backdrop-blur">
      <div className="text-xs font-semibold uppercase tracking-[0.34em] text-[var(--muted)]">
        {label}
      </div>
      <div className="mt-4 text-3xl font-semibold tracking-[-0.05em] text-slate-950">
        {value}
      </div>
      <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{note}</p>
    </div>
  );
}

function Panel({
  eyebrow,
  title,
  children,
}: {
  eyebrow: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-[2rem] border border-slate-900/10 bg-[var(--card-strong)] p-5 shadow-[var(--shadow)] backdrop-blur sm:p-6">
      <p className="text-xs font-semibold uppercase tracking-[0.34em] text-[var(--muted)]">
        {eyebrow}
      </p>
      <h2 className="mt-2 text-2xl font-semibold tracking-[-0.04em] text-slate-950">
        {title}
      </h2>
      <div className="mt-5">{children}</div>
    </section>
  );
}

function Placeholder({ copy }: { copy: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-slate-900/15 bg-white/55 px-4 py-8 text-center text-sm text-[var(--muted)]">
      {copy}
    </div>
  );
}

function ServiceGroups({
  groups,
}: {
  groups: { category: string; services: ServiceStatus[] }[];
}) {
  if (!groups.length) {
    return <Placeholder copy="No service health data has been loaded yet." />;
  }

  return (
    <div className="space-y-6">
      {groups.map((group) => (
        <div key={group.category}>
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
              {group.category}
            </h3>
            <div className="text-xs text-[var(--muted)]">
              {group.services.length} services
            </div>
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            {group.services.map((service) => (
              <ServiceCard key={service.key} service={service} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ServiceCard({ service }: { service: ServiceStatus }) {
  return (
    <article className="rounded-[1.5rem] border border-slate-900/10 bg-white/80 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <span
              className={`inline-flex h-2.5 w-2.5 rounded-full ${
                service.active ? "animate-pulse bg-emerald-600" : "bg-slate-300"
              }`}
            />
            <p className="font-mono text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
              {service.category}
            </p>
          </div>
          <h3 className="mt-2 text-lg font-semibold text-slate-950">
            {service.label}
          </h3>
        </div>
        <StatusBadge value={service.status} />
      </div>

      <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{service.detail}</p>

      <div className="mt-4 flex flex-wrap gap-2">
        <StatusBadge label="Activity" value={service.active ? "running" : "idle"} />
        {service.latency_ms !== null ? (
          <StatusBadge label="Latency" value={`${service.latency_ms}ms`} />
        ) : null}
        {service.product_slug ? (
          <StatusBadge label="Product" value={service.product_slug} />
        ) : null}
      </div>

      <p className="mt-3 text-xs uppercase tracking-[0.24em] text-[var(--muted)]">
        Checked {formatRelativeTime(service.checked_at)}
      </p>
    </article>
  );
}

function SchedulerPanel({ scheduler }: { scheduler: SchedulerStatus }) {
  return (
    <div className="space-y-4">
      <div className="rounded-2xl border border-slate-900/10 bg-white/80 p-4">
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge value={scheduler.status} />
          <StatusBadge label="Mode" value={scheduler.mode} />
          <StatusBadge label="Timezone" value={scheduler.timezone} />
        </div>
        <p className="mt-4 text-sm leading-6 text-[var(--muted)]">{scheduler.detail}</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <InfoTile
          label="Cadence"
          value={scheduler.cadence}
          note="Configured scheduler pattern"
        />
        <InfoTile
          label="Next run"
          value={scheduler.next_run_at ? formatDate(scheduler.next_run_at) : "Manual only"}
          note={
            scheduler.next_run_at
              ? formatRelativeTime(scheduler.next_run_at)
              : "No automatic schedule forecast"
          }
        />
        <InfoTile
          label="Last started"
          value={formatDate(scheduler.last_started_at)}
          note="Most recent recorded pipeline start"
        />
        <InfoTile
          label="Last success"
          value={formatDate(scheduler.last_success_at)}
          note="Most recent successful completed run"
        />
      </div>
    </div>
  );
}

function InfoTile({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="rounded-2xl border border-slate-900/10 bg-white/80 p-4">
      <div className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
        {label}
      </div>
      <div className="mt-2 text-base font-semibold text-slate-950">{value}</div>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{note}</p>
    </div>
  );
}

function IssueFeed({ issues }: { issues: IssueSnapshot[] }) {
  if (!issues.length) {
    return <Placeholder copy="No warnings or errors are open right now." />;
  }

  return (
    <div className="space-y-3">
      {issues.map((issue) => (
        <article
          key={issue.issue_id}
          className="rounded-2xl border border-slate-900/10 bg-white/80 p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
                {issue.source}
              </p>
              <h3 className="mt-1 text-sm font-semibold text-slate-950">
                {issue.title}
              </h3>
            </div>
            <StatusBadge value={issue.severity} />
          </div>
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{issue.detail}</p>
          <div className="mt-3 flex flex-wrap gap-2">
            {issue.product_slug ? (
              <StatusBadge label="Product" value={issue.product_slug} />
            ) : null}
            {issue.run_id ? <StatusBadge label="Run" value={issue.run_id} /> : null}
          </div>
          <p className="mt-3 text-xs uppercase tracking-[0.24em] text-[var(--muted)]">
            Observed {formatRelativeTime(issue.observed_at)}
          </p>
        </article>
      ))}
    </div>
  );
}

function LockList({ locks }: { locks: LockSnapshot[] }) {
  if (!locks.length) {
    return <Placeholder copy="No active or stale run locks are visible right now." />;
  }

  return (
    <div className="space-y-3">
      {locks.map((lock) => (
        <article
          key={lock.key}
          className="rounded-2xl border border-slate-900/10 bg-white/80 p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
                {lock.product_slug}
              </p>
              <h3 className="mt-1 text-sm font-semibold text-slate-950">
                {lock.iso_week}
              </h3>
            </div>
            <StatusBadge value={lock.status} />
          </div>
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{lock.detail}</p>
          <div className="mt-3 flex flex-wrap gap-2">
            <StatusBadge label="Age" value={formatAgeSeconds(lock.age_seconds)} />
            {lock.pid !== null ? <StatusBadge label="PID" value={String(lock.pid)} /> : null}
          </div>
          <p className="mt-3 break-all font-mono text-xs text-[var(--muted)]">{lock.path}</p>
        </article>
      ))}
    </div>
  );
}

function ProgramStatusPanel({
  audit,
  readiness,
}: {
  audit: CompletionAudit;
  readiness: { key: string; label: string; status: string; detail: string }[];
}) {
  return (
    <div className="space-y-5">
      <div className="rounded-2xl border border-slate-900/10 bg-white/80 p-4">
        <div className="flex flex-wrap items-center gap-3">
          <StatusBadge value={audit.overall_status} />
          <p className="text-sm leading-6 text-[var(--muted)]">{audit.notes[0]}</p>
        </div>
      </div>

      <div className="space-y-3">
        {audit.phases.map((phase) => (
          <div
            key={phase.phase}
            className="rounded-2xl border border-slate-900/10 bg-white/80 p-4"
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="font-mono text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
                  {phase.phase}
                </p>
                <h3 className="mt-1 text-sm font-semibold text-slate-950">
                  {phase.title}
                </h3>
              </div>
              <div className="flex flex-wrap gap-2">
                <StatusBadge label="Code" value={phase.implementation_status} />
                <StatusBadge label="E2E" value={phase.end_to_end_status} />
              </div>
            </div>
            <div className="mt-3 space-y-2">
              {phase.notes.map((note) => (
                <p
                  key={note}
                  className="rounded-xl bg-slate-900/[0.03] px-3 py-2 text-sm text-[var(--muted)]"
                >
                  {note}
                </p>
              ))}
            </div>
          </div>
        ))}
      </div>

      <div className="space-y-3">
        {readiness.map((check) => (
          <div
            key={check.key}
            className="rounded-2xl border border-slate-900/10 bg-white/80 p-4"
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <h3 className="text-sm font-semibold text-slate-950">{check.label}</h3>
              <StatusBadge value={check.status} />
            </div>
            <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{check.detail}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function ProductCard({
  product,
  isBusy,
  onRun,
  onInspectRun,
}: {
  product: ProductStatus;
  isBusy: boolean;
  onRun: () => void;
  onInspectRun: (run: RunSummary) => void;
}) {
  return (
    <article className="flex h-full flex-col rounded-[1.6rem] border border-slate-900/10 bg-white/80 p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
            {product.slug}
          </p>
          <h3 className="mt-1 text-xl font-semibold tracking-[-0.03em] text-slate-950">
            {product.display_name}
          </h3>
        </div>
        <StatusBadge value={product.active ? "ready" : "warning"} />
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <StatusBadge
          label="App Store"
          value={product.app_store_configured ? "ready" : "missing"}
        />
        <StatusBadge
          label="Play"
          value={product.play_store_configured ? "ready" : "missing"}
        />
        <StatusBadge
          label="Docs"
          value={product.google_doc_configured ? "ready" : "warning"}
        />
        <StatusBadge
          label="Emails"
          value={product.stakeholder_count > 0 ? "ready" : "missing"}
        />
      </div>

      <div className="mt-4 rounded-2xl border border-slate-900/10 bg-slate-900/[0.03] p-4">
        <div className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
          Latest run
        </div>
        {product.latest_run ? (
          <div className="mt-2 space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge value={product.latest_run.status} />
              <StatusBadge label="Stage" value={product.latest_run.stage} />
              {product.latest_run.docs_status ? (
                <StatusBadge label="Docs" value={product.latest_run.docs_status} />
              ) : null}
              {product.latest_run.gmail_status ? (
                <StatusBadge label="Gmail" value={product.latest_run.gmail_status} />
              ) : null}
            </div>
            <p className="text-sm text-[var(--muted)]">
              {product.latest_run.iso_week} | started{" "}
              {formatDate(product.latest_run.started_at)}
            </p>
            <button
              type="button"
              onClick={() => {
                const latestRun = product.latest_run;
                if (latestRun) {
                  onInspectRun(latestRun);
                }
              }}
              className="rounded-xl border border-slate-900/10 px-3 py-2 text-sm font-semibold text-slate-900 transition hover:bg-slate-900/5"
            >
              Inspect latest run
            </button>
          </div>
        ) : (
          <p className="mt-2 text-sm text-[var(--muted)]">
            No runs have been recorded yet for this product.
          </p>
        )}
      </div>

      <div className="mt-4 flex-1">
        <div className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
          Issues
        </div>
        {product.issues.length ? (
          <ul className="mt-2 space-y-2 text-sm leading-6 text-[var(--muted)]">
            {product.issues.map((issue) => (
              <li key={issue} className="rounded-xl bg-amber-50 px-3 py-2 text-amber-950">
                {issue}
              </li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 text-sm text-emerald-900">
            Product metadata is filled well enough for a normal pipeline trigger.
          </p>
        )}
      </div>

      <div className="mt-5 flex items-center justify-between gap-3">
        <div className="text-sm text-[var(--muted)]">
          Default window:{" "}
          <span className="font-semibold text-slate-950">
            {product.default_lookback_weeks} weeks
          </span>
        </div>
        <button
          type="button"
          onClick={onRun}
          disabled={isBusy}
          className="rounded-2xl bg-slate-950 px-4 py-3 text-sm font-semibold text-white transition hover:translate-y-[-1px] hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {isBusy ? "Queueing..." : "Run full flow"}
        </button>
      </div>
    </article>
  );
}

function RunTable({
  runs,
  selectedRunId,
  onSelectRun,
}: {
  runs: RunSummary[];
  selectedRunId: string | null;
  onSelectRun: (run: RunSummary) => void;
}) {
  if (!runs.length) {
    return <Placeholder copy="No runs recorded yet." />;
  }

  return (
    <div className="overflow-hidden rounded-[1.6rem] border border-slate-900/10">
      <div className="grid grid-cols-[1.05fr_0.8fr_0.8fr_1fr] gap-3 bg-slate-900/5 px-4 py-3 text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
        <div>Product</div>
        <div>Week</div>
        <div>Status</div>
        <div>Started</div>
      </div>
      <div className="divide-y divide-slate-900/10 bg-white/75">
        {runs.map((run) => {
          const isSelected = selectedRunId === run.run_id;
          return (
            <button
              key={run.run_id}
              type="button"
              onClick={() => {
                onSelectRun(run);
              }}
              className={`grid w-full grid-cols-[1.05fr_0.8fr_0.8fr_1fr] gap-3 px-4 py-4 text-left transition ${
                isSelected ? "bg-slate-950/[0.06]" : "hover:bg-slate-900/[0.03]"
              }`}
            >
              <div>
                <div className="font-semibold text-slate-950">
                  {humanizeSlug(run.product_slug)}
                </div>
                <div className="mt-1 font-mono text-xs text-[var(--muted)]">
                  {run.run_id}
                </div>
              </div>
              <div className="text-sm text-[var(--muted)]">{run.iso_week}</div>
              <div className="flex flex-wrap gap-2">
                <StatusBadge value={run.status} />
              </div>
              <div className="text-sm text-[var(--muted)]">
                {formatDate(run.started_at)}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function JobList({ jobs }: { jobs: JobSnapshot[] }) {
  if (!jobs.length) {
    return <Placeholder copy="No trigger jobs have been queued from this dashboard session yet." />;
  }

  return (
    <div className="space-y-3">
      {jobs.slice(0, 8).map((job) => (
        <div
          key={job.job_id}
          className="rounded-2xl border border-slate-900/10 bg-white/75 p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.28em] text-[var(--muted)]">
                {job.kind}
              </p>
              <h3 className="mt-1 text-sm font-semibold text-slate-950">
                {job.product_slug ? humanizeSlug(job.product_slug) : "All active products"}
              </h3>
            </div>
            <StatusBadge value={job.status} />
          </div>
          <p className="mt-3 text-sm text-[var(--muted)]">
            Submitted {formatDate(job.submitted_at)} | target {job.target}
          </p>
          {job.items.length ? (
            <div className="mt-3 flex flex-wrap gap-2">
              {job.items.map((item) => (
                <StatusBadge
                  key={`${job.job_id}-${item.product_slug}`}
                  label={item.product_slug}
                  value={item.status}
                />
              ))}
            </div>
          ) : null}
          {job.error ? (
            <p className="mt-3 rounded-xl bg-rose-50 px-3 py-2 text-sm text-rose-950">
              {job.error}
            </p>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function RunDetailPanel({ detail }: { detail: RunDetail }) {
  const auditPreview = JSON.stringify(detail.audit, null, 2);

  return (
    <div className="space-y-4">
      <div className="rounded-2xl border border-slate-900/10 bg-white/75 p-4">
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge value={detail.run.status} />
          <StatusBadge label="Stage" value={detail.run.stage} />
          {detail.run.docs_status ? (
            <StatusBadge label="Docs" value={detail.run.docs_status} />
          ) : null}
          {detail.run.gmail_status ? (
            <StatusBadge label="Gmail" value={detail.run.gmail_status} />
          ) : null}
        </div>
        <div className="mt-4 space-y-2 text-sm leading-6 text-[var(--muted)]">
          <p>
            <span className="font-semibold text-slate-950">Run ID:</span>{" "}
            <span className="font-mono">{detail.run.run_id}</span>
          </p>
          <p>
            <span className="font-semibold text-slate-950">Product:</span>{" "}
            {humanizeSlug(detail.run.product_slug)}
          </p>
          <p>
            <span className="font-semibold text-slate-950">ISO week:</span>{" "}
            {detail.run.iso_week}
          </p>
          <p>
            <span className="font-semibold text-slate-950">Started:</span>{" "}
            {formatDate(detail.run.started_at)}
          </p>
          <p>
            <span className="font-semibold text-slate-950">Completed:</span>{" "}
            {formatDate(detail.run.completed_at)}
          </p>
          {detail.run.warning ? (
            <p className="rounded-xl bg-amber-50 px-3 py-2 text-amber-950">
              {detail.run.warning}
            </p>
          ) : null}
        </div>
      </div>

      <div className="space-y-3">
        {detail.deliveries.map((delivery) => (
          <div
            key={`${detail.run.run_id}-${delivery.target}`}
            className="rounded-2xl border border-slate-900/10 bg-white/75 p-4"
          >
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h3 className="text-sm font-semibold uppercase tracking-[0.24em] text-slate-950">
                {delivery.target}
              </h3>
              <StatusBadge value={delivery.status} />
            </div>
            <div className="mt-3 space-y-2 text-sm text-[var(--muted)]">
              {delivery.external_id ? (
                <p>
                  <span className="font-semibold text-slate-950">External ID:</span>{" "}
                  <span className="font-mono">{delivery.external_id}</span>
                </p>
              ) : null}
              {delivery.external_link ? (
                <a
                  href={delivery.external_link}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex rounded-xl border border-slate-900/10 px-3 py-2 font-semibold text-slate-950 transition hover:bg-slate-900/5"
                >
                  Open delivery link
                </a>
              ) : null}
            </div>
          </div>
        ))}
      </div>

      <div className="overflow-hidden rounded-2xl border border-slate-900/10 bg-slate-950 text-slate-100">
        <div className="border-b border-white/10 px-4 py-3 font-mono text-xs uppercase tracking-[0.28em] text-slate-300">
          Audit payload
        </div>
        <pre className="max-h-[28rem] overflow-auto px-4 py-4 text-xs leading-6 text-slate-200">
          {auditPreview}
        </pre>
      </div>
    </div>
  );
}
