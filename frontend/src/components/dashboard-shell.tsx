"use client";

import { StatusBadge, statusToneFromValue } from "@/components/status-badge";
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
  resendGmail,
  triggerRun,
  uploadReviewCsv,
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
const SECTION_LINKS = [
  { href: "#overview", label: "Overview" },
  { href: "#command", label: "Trigger" },
  { href: "#services", label: "Services" },
  { href: "#activity", label: "Activity" },
  { href: "#runs", label: "Runs" },
  { href: "#readiness", label: "Readiness" },
] as const;

const FOCUS_SERVICE_CONFIG = [
  {
    key: "ingestion_agent",
    title: "Review Ingestion",
    subtitle: "App Store and Google Play review pull readiness.",
  },
  {
    key: "docs_mcp",
    title: "Google Docs MCP",
    subtitle: "Append weekly pulse sections into the Groww running doc.",
  },
  {
    key: "gmail_mcp",
    title: "Gmail MCP",
    subtitle: "Send or draft stakeholder delivery through MCP only.",
  },
  {
    key: "scheduler",
    title: "Periodic Scheduler",
    subtitle: "Weekly cadence, next run timing, and periodic posture.",
  },
] as const;

type ActivityEvent = {
  id: string;
  kind: "issue" | "job" | "run";
  title: string;
  detail: string;
  status: string;
  timestamp: string | null;
  meta: string;
};

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

function titleize(value: string) {
  return value
    .split(/[\s-_]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
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

function inputModeLabel(value: "scrape" | "csv_upload" | string | null | undefined) {
  if (value === "csv_upload") {
    return "CSV upload";
  }
  return "Live store pull";
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

function categoryLabel(value: string) {
  switch (value) {
    case "platform":
      return "Platform";
    case "pipeline":
      return "Pipeline";
    case "mcp":
      return "MCP Delivery";
    case "ops":
      return "Operations";
    default:
      return titleize(value);
  }
}

function percent(value: number, total: number) {
  if (total <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round((value / total) * 100)));
}

function sortByNewest<T extends { timestamp: string | null }>(items: T[]) {
  return [...items].sort((left, right) => {
    const leftValue = left.timestamp ? new Date(left.timestamp).getTime() : 0;
    const rightValue = right.timestamp ? new Date(right.timestamp).getTime() : 0;
    return rightValue - leftValue;
  });
}

function buildActivityFeed(
  issues: IssueSnapshot[],
  jobs: JobSnapshot[],
  runs: RunSummary[],
) {
  const issueEvents: ActivityEvent[] = issues.map((issue) => ({
    id: `issue:${issue.issue_id}`,
    kind: "issue",
    title: issue.title,
    detail: issue.detail,
    status: issue.severity,
    timestamp: issue.observed_at,
    meta: issue.product_slug ? humanizeSlug(issue.product_slug) : issue.source,
  }));

  const jobEvents: ActivityEvent[] = jobs.map((job) => ({
    id: `job:${job.job_id}`,
    kind: "job",
    title: `${targetLabel(
      job.target as "all" | "docs" | "gmail",
    )} queued for ${humanizeSlug(job.product_slug ?? VISIBLE_PRODUCT_SLUG)}`,
    detail: job.warning
      ? job.warning
      : job.error
        ? job.error
        : `${job.kind} job ${job.job_id.slice(0, 8)} currently ${job.status}.`,
    status: job.status,
    timestamp: job.completed_at ?? job.started_at ?? job.submitted_at,
    meta: job.iso_week ?? "manual trigger",
  }));

  const runEvents: ActivityEvent[] = runs.map((run) => ({
    id: `run:${run.run_id}`,
    kind: "run",
    title: `${humanizeSlug(run.product_slug)} ${run.iso_week}`,
    detail: `Stage ${run.stage}. Docs ${run.docs_status ?? "pending"}, Gmail ${
      run.gmail_status ?? "pending"
    }.`,
    status: run.status,
    timestamp: run.completed_at ?? run.started_at,
    meta: run.run_id.slice(0, 8),
  }));

  return sortByNewest([...issueEvents, ...jobEvents, ...runEvents]).slice(0, 12);
}

function countServiceTones(services: ServiceStatus[]) {
  return services.reduce(
    (accumulator, service) => {
      const tone = statusToneFromValue(service.status);
      if (tone === "ready") {
        accumulator.ready += 1;
      } else if (tone === "danger") {
        accumulator.danger += 1;
      } else if (tone === "warning") {
        accumulator.warning += 1;
      } else {
        accumulator.other += 1;
      }
      return accumulator;
    },
    { ready: 0, warning: 0, danger: 0, other: 0 },
  );
}

function countDeliveryHealth(
  runs: RunSummary[],
  field: "docs_status" | "gmail_status",
) {
  return runs.reduce(
    (accumulator, run) => {
      const value = run[field];
      const tone = statusToneFromValue(value);
      if (tone === "ready") {
        accumulator.ready += 1;
      } else if (tone === "danger") {
        accumulator.failed += 1;
      } else if (tone === "warning") {
        accumulator.pending += 1;
      } else if (value) {
        accumulator.other += 1;
      }
      return accumulator;
    },
    { ready: 0, failed: 0, pending: 0, other: 0 },
  );
}

function toneDotClass(value: string) {
  const tone = statusToneFromValue(value);
  if (tone === "ready") {
    return "bg-emerald-500 shadow-[0_0_18px_rgba(16,185,129,0.55)]";
  }
  if (tone === "warning") {
    return "bg-amber-500 shadow-[0_0_18px_rgba(245,158,11,0.45)]";
  }
  if (tone === "danger") {
    return "bg-rose-500 shadow-[0_0_18px_rgba(244,63,94,0.45)]";
  }
  if (tone === "info") {
    return "bg-sky-500 shadow-[0_0_18px_rgba(14,165,233,0.45)]";
  }
  return "bg-slate-400 shadow-[0_0_18px_rgba(148,163,184,0.35)]";
}

function buttonStyle(active: boolean) {
  return active
    ? "border-emerald-700/10 bg-emerald-600 text-white shadow-[0_18px_40px_rgba(5,150,105,0.25)]"
    : "border-emerald-950/10 bg-white/80 text-slate-900 hover:bg-emerald-50";
}

function metricAccent(value: "primary" | "warning" | "danger" | "info") {
  if (value === "warning") {
    return "from-amber-500/18 to-white";
  }
  if (value === "danger") {
    return "from-rose-500/18 to-white";
  }
  if (value === "info") {
    return "from-sky-500/18 to-white";
  }
  return "from-emerald-500/18 to-white";
}

function jobFlashMessage(job: JobSnapshot, queuedMessage: string) {
  if (job.warning) {
    return job.warning;
  }
  if (job.error) {
    return job.error;
  }
  return queuedMessage;
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
  const [commandMode, setCommandMode] = useState<"scrape" | "csv_upload">("scrape");
  const [selectedTarget, setSelectedTarget] = useState<"all" | "docs" | "gmail">("all");
  const [requestedWeeks, setRequestedWeeks] = useState(DEFAULT_LOOKBACK_WEEKS);
  const [selectedCsvFile, setSelectedCsvFile] = useState<File | null>(null);
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

  useEffect(() => {
    if (!flashMessage) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      setFlashMessage(null);
    }, 5000);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [flashMessage]);

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

  const visibleServices = useMemo(() => {
    if (!overview) {
      return [];
    }

    return overview.services.filter(
      (service) => !service.product_slug || isVisibleProductSlug(service.product_slug),
    );
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

    return visibleProducts.find((product) => product.slug === selectedProductSlug) ?? null;
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
        label: categoryLabel(category),
        services: services.filter((service) => service.category === category),
      }))
      .filter((group) => group.services.length > 0);
  }, [overview]);

  const latestVisibleRun = useMemo(
    () => findLatestVisibleRun(visibleRecentRuns),
    [visibleRecentRuns],
  );

  const serviceToneCounts = useMemo(
    () => countServiceTones(visibleServices),
    [visibleServices],
  );

  const docsDeliveryHealth = useMemo(
    () => countDeliveryHealth(visibleRecentRuns, "docs_status"),
    [visibleRecentRuns],
  );

  const gmailDeliveryHealth = useMemo(
    () => countDeliveryHealth(visibleRecentRuns, "gmail_status"),
    [visibleRecentRuns],
  );

  const readinessScore = useMemo(() => {
    const readyCount = visibleReadiness.filter(
      (check) => statusToneFromValue(check.status) === "ready",
    ).length;
    return percent(readyCount, visibleReadiness.length);
  }, [visibleReadiness]);

  const phaseCompletion = useMemo(() => {
    const phases = overview?.completion.phases ?? [];
    const completeCount = phases.filter((phase) =>
      ["complete", "completed", "ready"].includes(
        phase.end_to_end_status.toLowerCase(),
      ),
    ).length;
    return {
      completeCount,
      totalCount: phases.length,
      percent: percent(completeCount, phases.length),
    };
  }, [overview]);

  const latestNotes = overview?.completion.notes ?? [];
  const newestIssue = visibleIssues[0] ?? null;
  const spotlightService =
    visibleServices.find((service) => statusToneFromValue(service.status) !== "ready") ??
    visibleServices[0] ??
    null;
  const activityFeed = useMemo(
    () => buildActivityFeed(visibleIssues, visibleJobs, visibleRecentRuns),
    [visibleIssues, visibleJobs, visibleRecentRuns],
  );
  const focusServices = useMemo(
    () =>
      FOCUS_SERVICE_CONFIG.map((item) => ({
        ...item,
        service: visibleServices.find((service) => service.key === item.key) ?? null,
      })),
    [visibleServices],
  );

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

  async function handleRefreshSnapshot() {
    try {
      setActiveActionKey("refresh");
      await refreshOverviewNow();
      setFlashMessage("Dashboard snapshot refreshed from the live backend.");
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Unable to refresh the live backend snapshot.";
      setErrorMessage(message);
    } finally {
      setActiveActionKey(null);
    }
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
        jobFlashMessage(
          job,
          `Queued an end-to-end flow for ${product.display_name} as job ${job.job_id.slice(0, 8)}.`,
        ),
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
        jobFlashMessage(
          job,
          `Queued ${targetLabel(selectedTarget).toLowerCase()} for ${selectedProduct.display_name} as job ${job.job_id.slice(0, 8)}.`,
        ),
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

  async function handleTriggerCsvUpload() {
    if (!selectedProduct) {
      setErrorMessage("Choose a product before uploading a CSV.");
      return;
    }

    if (!selectedCsvFile) {
      setErrorMessage("Pick a review CSV before starting the upload analysis flow.");
      return;
    }

    const actionKey = "upload-csv";
    try {
      setActiveActionKey(actionKey);
      const csvText = await selectedCsvFile.text();
      if (!csvText.trim()) {
        throw new Error("The selected CSV file is empty.");
      }

      const job = await uploadReviewCsv({
        product_slug: selectedProduct.slug,
        csv_text: csvText,
        filename: selectedCsvFile.name,
        weeks: requestedWeeks,
        target: selectedTarget,
      });
      setFlashMessage(
        jobFlashMessage(
          job,
          `Uploaded ${selectedCsvFile.name} and queued ${targetLabel(selectedTarget).toLowerCase()} for ${selectedProduct.display_name} as job ${job.job_id.slice(0, 8)}.`,
        ),
      );
      if (job.run_id) {
        selectRun(job.run_id);
      }
      await refreshOverviewNow();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to upload and analyze the CSV.";
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
        jobFlashMessage(
          job,
          `Queued the weekly pulse for ${product.display_name} as job ${job.job_id.slice(0, 8)}.`,
        ),
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

  async function handleResendGmail(run: RunSummary) {
    const actionKey = `resend:${run.run_id}`;
    try {
      setActiveActionKey(actionKey);
      const job = await resendGmail({ run_id: run.run_id });
      setFlashMessage(
        jobFlashMessage(
          job,
          `Queued a Gmail resend for ${humanizeSlug(run.product_slug)} ${run.iso_week} as job ${job.job_id.slice(0, 8)}.`,
        ),
      );
      if (job.run_id) {
        selectRun(job.run_id);
      }
      await refreshOverviewNow();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to queue the Gmail resend.";
      setErrorMessage(message);
    } finally {
      setActiveActionKey(null);
    }
  }

  return (
    <div className="min-h-screen text-[var(--foreground)]">
      <header className="sticky top-0 z-40 border-b border-emerald-950/8 bg-[rgba(248,252,249,0.88)] backdrop-blur-xl">
        <div className="mx-auto flex max-w-[1520px] items-center justify-between gap-4 px-4 py-3 sm:px-6 lg:px-8">
          <div className="flex min-w-0 items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-emerald-500/12 text-emerald-700 shadow-[inset_0_1px_0_rgba(255,255,255,0.75)]">
              <MiniIcon name="pulse" />
            </div>
            <div className="min-w-0">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.34em] text-emerald-800/80">
                Groww Analysis Center
              </p>
              <h1 className="truncate text-lg font-semibold tracking-[-0.03em] text-slate-950">
                Weekly Product Review Pulse
              </h1>
            </div>
          </div>

          <div className="hidden min-w-[18rem] flex-1 items-center justify-center md:flex">
            <nav className="flex flex-wrap items-center justify-center gap-2 rounded-full border border-emerald-950/10 bg-white/75 px-3 py-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.82)]">
              {SECTION_LINKS.map((item) => (
                <a
                  key={item.href}
                  href={item.href}
                  className="rounded-full px-3 py-1.5 text-sm font-semibold text-slate-900 transition hover:bg-emerald-50"
                >
                  {item.label}
                </a>
              ))}
            </nav>
          </div>

          <div className="flex items-center gap-2">
            <StatusBadge value={overview?.completion.overall_status ?? "loading"} />
            <button
              type="button"
              onClick={() => {
                void handleRefreshSnapshot();
              }}
              disabled={activeActionKey === "refresh"}
              className="inline-flex items-center gap-2 rounded-full border border-emerald-950/10 bg-white/85 px-4 py-2 text-sm font-semibold text-slate-900 transition hover:bg-emerald-50 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <MiniIcon name="refresh" />
              {activeActionKey === "refresh" ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>
      </header>

      <div className="mx-auto flex max-w-[1520px] gap-6 px-4 py-6 sm:px-6 lg:px-8">
        <aside className="hidden w-[250px] shrink-0 flex-col gap-4 lg:flex">
          <div className="overflow-hidden rounded-[1.9rem] border border-emerald-950/8 bg-white/85 shadow-[var(--shadow)]">
            <div className="border-b border-emerald-950/8 bg-[linear-gradient(135deg,rgba(16,185,129,0.14),rgba(255,255,255,0.65))] p-5">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-emerald-600 text-white">
                  <MiniIcon name="leaf" />
                </div>
                <div>
                  <p className="text-[0.68rem] font-semibold uppercase tracking-[0.32em] text-emerald-900/70">
                    Product Lens
                  </p>
                  <h2 className="text-xl font-semibold tracking-[-0.03em] text-slate-950">
                    Groww Only
                  </h2>
                </div>
              </div>
            </div>
            <div className="space-y-4 p-5">
              <div className="flex items-center justify-between rounded-2xl bg-emerald-500/8 px-4 py-3">
                <div>
                  <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                    Review Window
                  </p>
                  <div className="mt-1 text-xl font-semibold text-slate-950">
                    {selectedProduct?.default_lookback_weeks ?? requestedWeeks} weeks
                  </div>
                </div>
                <div className={`h-3 w-3 rounded-full ${toneDotClass(
                  overview?.completion.overall_status ?? "loading",
                )}`} />
              </div>

              <nav className="space-y-2">
                <SidebarLink href="#overview" icon="dashboard" label="Overview" />
                <SidebarLink href="#services" icon="servers" label="Services" />
                <SidebarLink href="#activity" icon="activity" label="Activity Log" />
                <SidebarLink href="#runs" icon="runs" label="Runs & Delivery" />
                <SidebarLink href="#readiness" icon="checklist" label="Readiness" />
              </nav>

              <div className="rounded-[1.4rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(232,250,241,0.8))] p-4">
                <p className="text-[0.68rem] font-semibold uppercase tracking-[0.32em] text-[var(--muted)]">
                  Live Health
                </p>
                <div className="mt-3 text-3xl font-semibold tracking-[-0.05em] text-slate-950">
                  {percent(serviceToneCounts.ready, visibleServices.length)}%
                </div>
                <p className="mt-1 text-sm leading-6 text-[var(--muted)]">
                  Services ready across the Groww ingestion, clustering, render, Docs,
                  Gmail, and scheduler chain.
                </p>
                <div className="mt-4 space-y-3">
                  <ProgressRow
                    label="Ready"
                    value={serviceToneCounts.ready}
                    total={visibleServices.length}
                    tone="ready"
                  />
                  <ProgressRow
                    label="Warning"
                    value={serviceToneCounts.warning}
                    total={visibleServices.length}
                    tone="warning"
                  />
                  <ProgressRow
                    label="Failed"
                    value={serviceToneCounts.danger}
                    total={visibleServices.length}
                    tone="danger"
                  />
                </div>
              </div>
            </div>
          </div>
        </aside>

        <main className="min-w-0 flex-1 space-y-6">
          {errorMessage ? (
            <Banner tone="danger" title="Backend attention needed" copy={errorMessage} />
          ) : null}
          {flashMessage ? (
            <Banner tone="ready" title="Action queued" copy={flashMessage} />
          ) : null}

          <section
            id="overview"
            className="scroll-mt-28 grid gap-6 xl:grid-cols-[1.35fr_0.9fr]"
          >
            <div className="overflow-hidden rounded-[2rem] border border-emerald-950/8 bg-white/88 shadow-[var(--shadow)]">
              <div className="grid gap-0 xl:grid-cols-[1.2fr_0.8fr]">
                <div className="p-6 sm:p-8">
                  <div className="flex flex-wrap items-center gap-2">
                    <StatusBadge value={overview?.completion.overall_status ?? "loading"} />
                    <StatusBadge
                      label="Product"
                      value={selectedProductDisplayName ?? VISIBLE_PRODUCT_NAME}
                    />
                    <StatusBadge label="Target" value={targetLabel(selectedTarget)} />
                  </div>

                  <p className="mt-5 text-[0.72rem] font-semibold uppercase tracking-[0.36em] text-emerald-900/70">
                    Operator Overview
                  </p>
                  <h2 className="mt-2 max-w-3xl text-4xl font-semibold tracking-[-0.05em] text-slate-950 sm:text-5xl">
                    Review intelligence, MCP delivery, and scheduler status in one
                    Groww control room.
                  </h2>
                  <p className="mt-4 max-w-3xl text-sm leading-7 text-[var(--muted)] sm:text-base">
                    This dashboard monitors the real weekly review pulse pipeline:
                    App Store and Google Play ingestion, clustering, theme generation,
                    Docs append, Gmail delivery, and run audit visibility.
                  </p>

                  <div className="mt-6 flex flex-wrap gap-3">
                    <button
                      type="button"
                      onClick={() => {
                        void handleTriggerWeekly();
                      }}
                      disabled={activeActionKey === "weekly"}
                      className="inline-flex items-center gap-2 rounded-full bg-emerald-600 px-5 py-3 text-sm font-semibold text-white shadow-[0_18px_40px_rgba(5,150,105,0.25)] transition hover:translate-y-[-1px] hover:bg-emerald-700 disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      <MiniIcon name="play" />
                      {activeActionKey === "weekly"
                        ? "Queueing..."
                        : "Queue Periodic Weekly Flow"}
                    </button>
                    <a
                      href="#services"
                      className="inline-flex items-center gap-2 rounded-full border border-emerald-950/10 bg-white/85 px-5 py-3 text-sm font-semibold text-slate-900 transition hover:bg-emerald-50"
                    >
                      <MiniIcon name="servers" />
                      Inspect Services
                    </a>
                  </div>

                  <div className="mt-8 grid gap-4 md:grid-cols-3">
                    <MetricChip
                      label="API Endpoint"
                      value={apiBaseUrl()}
                      note="Frontend to Railway bridge"
                    />
                    <MetricChip
                      label="Last Refresh"
                      value={overview ? formatDate(overview.checked_at) : "Loading..."}
                      note={overview ? formatRelativeTime(overview.checked_at) : "Waiting"}
                    />
                    <MetricChip
                      label="Latest Groww Run"
                      value={latestVisibleRun?.iso_week ?? "No run yet"}
                      note={
                        latestVisibleRun
                          ? latestVisibleRun.status
                          : "Trigger a first one-shot flow"
                      }
                    />
                  </div>
                </div>

                <div className="border-t border-emerald-950/8 bg-[linear-gradient(180deg,rgba(225,249,237,0.6),rgba(255,255,255,0.92))] p-6 sm:p-8 xl:border-l xl:border-t-0">
                  <p className="text-[0.72rem] font-semibold uppercase tracking-[0.34em] text-emerald-900/70">
                    System Pulse
                  </p>

                  <div className="mt-4 rounded-[1.6rem] border border-emerald-950/8 bg-slate-950 px-5 py-5 text-white shadow-[0_22px_44px_rgba(15,23,42,0.2)]">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-emerald-200/70">
                          Service Readiness
                        </p>
                        <div className="mt-2 text-4xl font-semibold tracking-[-0.06em]">
                          {serviceToneCounts.ready}
                        </div>
                      </div>
                      <div className="flex h-16 w-16 items-center justify-center rounded-full border border-white/10 bg-white/5">
                        <div className={`h-4 w-4 rounded-full ${toneDotClass(
                          overview?.completion.overall_status ?? "loading",
                        )}`} />
                      </div>
                    </div>
                    <p className="mt-3 text-sm leading-6 text-slate-300">
                      {visibleServices.length
                        ? `${visibleServices.length} live services are tracked for Groww right now.`
                        : "Waiting for service telemetry from the backend."}
                    </p>
                  </div>

                  <div className="mt-4 grid gap-3">
                    <PulseStrip
                      label="Docs Delivery Rail"
                      value={`${docsDeliveryHealth.ready}`}
                      sublabel="successful recent doc updates"
                      percentValue={percent(
                        docsDeliveryHealth.ready,
                        visibleRecentRuns.length,
                      )}
                      tone="ready"
                    />
                    <PulseStrip
                      label="Gmail Delivery Rail"
                      value={`${gmailDeliveryHealth.ready}`}
                      sublabel="healthy recent email outcomes"
                      percentValue={percent(
                        gmailDeliveryHealth.ready,
                        visibleRecentRuns.length,
                      )}
                      tone={gmailDeliveryHealth.failed > 0 ? "warning" : "ready"}
                    />
                    <PulseStrip
                      label="Phase Completion"
                      value={`${phaseCompletion.completeCount}/${phaseCompletion.totalCount || 0}`}
                      sublabel="documented phases at ready/completed E2E"
                      percentValue={phaseCompletion.percent}
                      tone={phaseCompletion.percent < 100 ? "warning" : "ready"}
                    />
                  </div>

                  <div className="mt-4 flex flex-wrap gap-2">
                    <StatusBadge
                      label="Products"
                      value={String(visibleStats.activeProducts || 1)}
                    />
                    <StatusBadge label="Readiness" value={`${readinessScore}%`} />
                    <StatusBadge
                      label="Running Jobs"
                      value={String(visibleStats.runningJobs)}
                    />
                    <StatusBadge
                      label="Issues"
                      value={String(visibleStats.openIssues)}
                    />
                  </div>
                </div>
              </div>
            </div>

            <SectionPanel
              id="command"
              eyebrow="Command Center"
              title="Queue scrape, upload, or periodic flow"
              subtitle="Run Groww immediately with live store pull, upload a review CSV for analysis, or queue the standard weekly full-flow trigger that mirrors the periodic cadence."
            >
              <div className="space-y-4">
                <label className="block space-y-2 text-sm">
                  <span className="font-semibold text-slate-950">Product</span>
                  <select
                    value={selectedProductSlug ?? VISIBLE_PRODUCT_SLUG}
                    onChange={(event) => {
                      handleProductSelection(event.target.value);
                    }}
                    disabled={visibleProducts.length <= 1}
                    className="w-full rounded-2xl border border-emerald-950/10 bg-white px-4 py-3 text-slate-950 outline-none transition focus:border-emerald-500"
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

                <div className="space-y-2">
                  <span className="text-sm font-semibold text-slate-950">Input mode</span>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                    {([
                      ["scrape", "Live store pull"],
                      ["csv_upload", "Upload review CSV"],
                    ] as const).map(([mode, label]) => (
                      <button
                        key={mode}
                        type="button"
                        onClick={() => {
                          setCommandMode(mode);
                        }}
                        className={`rounded-2xl border px-4 py-3 text-sm font-semibold transition ${buttonStyle(
                          commandMode === mode,
                        )}`}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="space-y-2">
                  <span className="text-sm font-semibold text-slate-950">Target</span>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                    {(["all", "docs", "gmail"] as const).map((target) => (
                      <button
                        key={target}
                        type="button"
                        onClick={() => {
                          setSelectedTarget(target);
                        }}
                        className={`rounded-2xl border px-4 py-3 text-sm font-semibold transition ${buttonStyle(
                          selectedTarget === target,
                        )}`}
                      >
                        {targetLabel(target)}
                      </button>
                    ))}
                  </div>
                </div>

                <label className="block space-y-2 text-sm">
                  <span className="font-semibold text-slate-950">Lookback weeks</span>
                  <input
                    min={1}
                    max={24}
                    type="number"
                    value={requestedWeeks}
                    onChange={(event) => {
                      setRequestedWeeks(Number(event.target.value));
                    }}
                    className="w-full rounded-2xl border border-emerald-950/10 bg-white px-4 py-3 text-slate-950 outline-none transition focus:border-emerald-500"
                  />
                </label>

                {commandMode === "csv_upload" ? (
                  <div className="space-y-3 rounded-[1.4rem] border border-emerald-950/10 bg-white/80 p-4">
                    <div>
                      <p className="text-sm font-semibold text-slate-950">Review CSV</p>
                      <p className="mt-1 text-sm leading-6 text-[var(--muted)]">
                        Upload a CSV and the backend will parse it into the same review
                        pipeline used by the scraper. Supported columns are flexible, but
                        common ones are <code>body</code>, <code>content</code>,{" "}
                        <code>review</code>, <code>title</code>, <code>rating</code>,{" "}
                        <code>review_created_at</code>, <code>review_updated_at</code>,{" "}
                        <code>author</code>, <code>source</code>, and <code>review_id</code>.
                      </p>
                    </div>
                    <label className="block space-y-2 text-sm">
                      <span className="font-semibold text-slate-950">Choose CSV file</span>
                      <input
                        type="file"
                        accept=".csv,text/csv"
                        onChange={(event) => {
                          const nextFile = event.target.files?.[0] ?? null;
                          setSelectedCsvFile(nextFile);
                        }}
                        className="block w-full rounded-2xl border border-emerald-950/10 bg-white px-4 py-3 text-slate-950 file:mr-4 file:rounded-full file:border-0 file:bg-emerald-100 file:px-4 file:py-2 file:font-semibold file:text-emerald-950"
                      />
                    </label>
                    <div className="rounded-2xl bg-emerald-50 px-4 py-3 text-sm text-emerald-950">
                      {selectedCsvFile
                        ? `${selectedCsvFile.name} selected. This upload will be analyzed for ${selectedProductDisplayName ?? VISIBLE_PRODUCT_NAME}.`
                        : "No CSV selected yet. Pick a file, then run the upload analysis flow."}
                    </div>
                  </div>
                ) : null}

                <div className="rounded-[1.4rem] border border-emerald-950/10 bg-[linear-gradient(135deg,rgba(16,185,129,0.1),rgba(255,255,255,0.9))] p-4">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                        Selected Product
                      </p>
                      <h3 className="mt-1 text-xl font-semibold tracking-[-0.03em] text-slate-950">
                        {selectedProductDisplayName ?? VISIBLE_PRODUCT_NAME}
                      </h3>
                    </div>
                    <StatusBadge value={targetLabel(selectedTarget)} />
                  </div>
                  <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                    {commandMode === "csv_upload"
                      ? "The run uses the uploaded CSV as the review source, then routes the rendered pulse through Google Docs MCP and Gmail MCP."
                      : "The run uses the configured App Store and Google Play IDs, creates a review pulse, then routes delivery through Google Docs MCP and Gmail MCP."}
                  </p>
                </div>

                {commandMode === "scrape" ? (
                  <p className="rounded-[1.2rem] border border-amber-200 bg-amber-50 px-4 py-3 text-sm leading-6 text-amber-950">
                    Same product plus ISO week is idempotent for the live store pull flow.
                    If Groww already has this week&apos;s Docs or Gmail delivery, clicking
                    again will reuse the existing run instead of sending the same email
                    twice.
                  </p>
                ) : null}

                <button
                  type="button"
                  onClick={() => {
                    if (commandMode === "csv_upload") {
                      void handleTriggerCsvUpload();
                      return;
                    }
                    void handleTriggerOneShot();
                  }}
                  disabled={
                    activeActionKey === "one-shot" ||
                    activeActionKey === "upload-csv" ||
                    isPending
                  }
                  className="inline-flex w-full items-center justify-center gap-2 rounded-[1.2rem] bg-slate-950 px-5 py-3.5 text-sm font-semibold text-white transition hover:translate-y-[-1px] hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  <MiniIcon name="play" />
                  {activeActionKey === "one-shot" ||
                  activeActionKey === "upload-csv" ||
                  isPending
                    ? commandMode === "csv_upload"
                      ? "Uploading CSV and queueing analysis..."
                      : "Queueing one-shot flow..."
                    : commandMode === "csv_upload"
                      ? `Upload CSV & Run ${targetLabel(selectedTarget)}`
                      : `Run One-Shot ${targetLabel(selectedTarget)}`}
                </button>

                <div className="grid gap-3 sm:grid-cols-[1fr_auto]">
                  <div className="rounded-[1.2rem] border border-emerald-950/10 bg-white/80 px-4 py-3">
                    <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                      Periodic cadence
                    </p>
                    <p className="mt-1 text-sm font-semibold text-slate-950">
                      {overview?.scheduler.cadence ?? "Waiting for scheduler data"}
                    </p>
                    <p className="mt-1 text-sm leading-6 text-[var(--muted)]">
                      {overview?.scheduler.next_run_at
                        ? `Next predicted run ${formatDate(overview.scheduler.next_run_at)}.`
                        : overview?.scheduler.detail ??
                          "Use the periodic button when automatic scheduling is disabled."}
                    </p>
                    <p className="mt-2 text-xs font-medium uppercase tracking-[0.22em] text-emerald-900/55">
                      Periodic flow always uses live store pull
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => {
                      void handleTriggerWeekly();
                    }}
                    disabled={activeActionKey === "weekly"}
                    className="inline-flex items-center justify-center gap-2 rounded-[1.2rem] bg-emerald-600 px-5 py-3.5 text-sm font-semibold text-white shadow-[0_18px_40px_rgba(5,150,105,0.2)] transition hover:translate-y-[-1px] hover:bg-emerald-700 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <MiniIcon name="refresh" />
                    {activeActionKey === "weekly" ? "Queueing..." : "Run Periodic Weekly"}
                  </button>
                </div>
              </div>
            </SectionPanel>
          </section>

          <section className="scroll-mt-28 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            {focusServices.map((item) => (
              <FocusServiceCard
                key={item.key}
                title={item.title}
                subtitle={item.subtitle}
                service={item.service}
              />
            ))}
          </section>

          <section className="scroll-mt-28 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              eyebrow="Services Ready"
              value={`${serviceToneCounts.ready}/${visibleServices.length || 0}`}
              note="Live services healthy in the Groww operator stack."
              accent="primary"
              footer={`${percent(serviceToneCounts.ready, visibleServices.length)}% readiness`}
            />
            <MetricCard
              eyebrow="Open Issues"
              value={NUMBER_FORMATTER.format(visibleIssues.length)}
              note="Warnings and errors needing attention."
              accent={visibleIssues.length > 0 ? "warning" : "primary"}
              footer={
                newestIssue ? `Latest: ${formatRelativeTime(newestIssue.observed_at)}` : "No active alerts"
              }
            />
            <MetricCard
              eyebrow="Scheduler"
              value={overview?.scheduler.next_run_at ? formatDate(overview.scheduler.next_run_at) : "Manual"}
              note={overview?.scheduler.detail ?? "Waiting for scheduler telemetry."}
              accent={overview?.scheduler.enabled ? "primary" : "info"}
              footer={
                overview?.scheduler.next_run_at
                  ? formatRelativeTime(overview.scheduler.next_run_at)
                  : "No auto-run forecast"
              }
            />
            <MetricCard
              eyebrow="Latest Run Status"
              value={latestVisibleRun?.status ?? "No run"}
              note={
                latestVisibleRun
                  ? `${latestVisibleRun.iso_week} | ${latestVisibleRun.stage}`
                  : "Queue the first Groww pipeline run."
              }
              accent={
                latestVisibleRun &&
                statusToneFromValue(latestVisibleRun.status) === "danger"
                  ? "danger"
                  : "primary"
              }
              footer={
                latestVisibleRun
                  ? `Started ${formatDate(latestVisibleRun.started_at)}`
                  : "Awaiting execution"
              }
            />
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
            <SectionPanel
              eyebrow="Groww Pulse Board"
              title="Ingestion, delivery, and phase progress"
              subtitle="A single visual layer for review-source readiness, MCP publish health, and implementation phase posture."
            >
              <div className="grid gap-4 xl:grid-cols-[0.92fr_1.08fr]">
                <div className="space-y-4">
                  <SubPanel title="Review Ingestion Readiness" icon="ingestion">
                    <div className="grid gap-3 sm:grid-cols-2">
                      <CheckTile
                        label="App Store feed"
                        value={selectedProduct?.app_store_configured ? "ready" : "missing"}
                        note="Apple review source configured for Groww."
                      />
                      <CheckTile
                        label="Google Play feed"
                        value={selectedProduct?.play_store_configured ? "ready" : "missing"}
                        note="Google Play review pull configured."
                      />
                      <CheckTile
                        label="Stakeholders"
                        value={
                          selectedProduct && selectedProduct.stakeholder_count > 0
                            ? "ready"
                            : "missing"
                        }
                        note={`${selectedProduct?.stakeholder_count ?? 0} email recipients configured.`}
                      />
                      <CheckTile
                        label="Google Doc"
                        value={selectedProduct?.google_doc_configured ? "ready" : "warning"}
                        note="Canonical running Google Doc target."
                      />
                    </div>

                    {selectedProduct?.issues.length ? (
                      <div className="mt-4 space-y-2">
                        {selectedProduct.issues.map((issue) => (
                          <p
                            key={issue}
                            className="rounded-2xl bg-amber-50 px-3 py-3 text-sm text-amber-950"
                          >
                            {issue}
                          </p>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-4 rounded-2xl bg-emerald-50 px-3 py-3 text-sm text-emerald-950">
                        Groww has the minimum product metadata needed for review pull,
                        report generation, and Workspace delivery.
                      </p>
                    )}
                  </SubPanel>

                  <SubPanel title="Delivery Rail" icon="delivery">
                    <div className="space-y-3">
                      <SignalMeter
                        label="Docs MCP"
                        status={latestVisibleRun?.docs_status ?? "pending"}
                        value={docsDeliveryHealth.ready}
                        total={visibleRecentRuns.length}
                      />
                      <SignalMeter
                        label="Gmail MCP"
                        status={latestVisibleRun?.gmail_status ?? "pending"}
                        value={gmailDeliveryHealth.ready}
                        total={visibleRecentRuns.length}
                      />
                    </div>
                  </SubPanel>
                </div>

                <SubPanel title="Phase Completion Grid" icon="phases">
                  <div className="space-y-4">
                    <div className="rounded-[1.4rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(230,250,240,0.78),rgba(255,255,255,0.96))] p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div>
                          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                            Completion Score
                          </p>
                          <div className="mt-1 text-3xl font-semibold tracking-[-0.05em] text-slate-950">
                            {phaseCompletion.percent}%
                          </div>
                        </div>
                        <StatusBadge value={overview?.completion.overall_status ?? "loading"} />
                      </div>
                      <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                        {latestNotes[0] ??
                          "The phase audit will show up here as soon as the backend returns it."}
                      </p>
                    </div>

                    <div className="grid gap-3">
                      {(overview?.completion.phases ?? []).map((phase) => (
                        <PhaseTile key={phase.phase} phase={phase} />
                      ))}
                    </div>
                  </div>
                </SubPanel>
              </div>
            </SectionPanel>

            <SectionPanel
              eyebrow="Scheduler & Alerts"
              title="Timing, warnings, and lock watch"
              subtitle="Track the next scheduled run, recent success timing, current alert pressure, and any stale run locks."
            >
              <div className="space-y-4">
                {overview ? <SchedulerPanel scheduler={overview.scheduler} /> : null}

                <SubPanel title="Alert Feed" icon="alert">
                  <IssueFeed issues={visibleIssues.slice(0, 4)} compact />
                </SubPanel>

                <SubPanel title="Lock Watch" icon="lock">
                  <LockList locks={visibleLocks.slice(0, 3)} compact />
                </SubPanel>
              </div>
            </SectionPanel>
          </section>

          <section
            id="services"
            className="scroll-mt-28 grid gap-6 xl:grid-cols-[1.18fr_0.82fr]"
          >
            <SectionPanel
              eyebrow="Service Matrix"
              title="Live health of all agents and MCP rails"
              subtitle="Each card reflects the real status returned by the backend health probes and operator overview API."
            >
              <div className="space-y-5">
                {servicesByCategory.map((group) => (
                  <div key={group.category} className="space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="text-[0.68rem] font-semibold uppercase tracking-[0.3em] text-[var(--muted)]">
                          {group.label}
                        </p>
                        <h3 className="mt-1 text-lg font-semibold tracking-[-0.03em] text-slate-950">
                          {group.services.length} tracked services
                        </h3>
                      </div>
                      <StatusBadge
                        label="Healthy"
                        value={String(
                          group.services.filter(
                            (service) => statusToneFromValue(service.status) === "ready",
                          ).length,
                        )}
                      />
                    </div>

                    <div className="grid gap-3 md:grid-cols-2">
                      {group.services.map((service) => (
                        <ServiceCard key={service.key} service={service} />
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </SectionPanel>

            <SectionPanel
              eyebrow="Pipeline Spotlight"
              title="Selected run and service focus"
              subtitle="A zoomed-in view for the most important Groww service right now plus the currently selected run."
            >
              <div className="space-y-4">
                {spotlightService ? (
                  <div className="overflow-hidden rounded-[1.6rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(231,248,239,0.92))]">
                    <div className="flex items-center justify-between gap-3 border-b border-emerald-950/8 px-5 py-4">
                      <div>
                        <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                          Service Spotlight
                        </p>
                        <h3 className="mt-1 text-xl font-semibold tracking-[-0.03em] text-slate-950">
                          {spotlightService.label}
                        </h3>
                      </div>
                      <StatusBadge value={spotlightService.status} />
                    </div>
                    <div className="space-y-4 px-5 py-4">
                      <p className="text-sm leading-6 text-[var(--muted)]">
                        {spotlightService.detail}
                      </p>
                      <div className="flex flex-wrap gap-2">
                        <StatusBadge
                          label="Category"
                          value={categoryLabel(spotlightService.category)}
                        />
                        <StatusBadge
                          label="Latency"
                          value={
                            spotlightService.latency_ms !== null
                              ? `${spotlightService.latency_ms}ms`
                              : "n/a"
                          }
                        />
                        <StatusBadge
                          label="Checked"
                          value={formatRelativeTime(spotlightService.checked_at)}
                        />
                      </div>
                    </div>
                  </div>
                ) : (
                  <Placeholder copy="No service spotlight yet." />
                )}

                <div className="rounded-[1.6rem] border border-emerald-950/8 bg-slate-950 text-slate-100">
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-5 py-4">
                    <div>
                      <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-slate-400">
                        Run Focus
                      </p>
                      <h3 className="mt-1 text-xl font-semibold tracking-[-0.03em] text-white">
                        {runDetail?.run.run_id ?? latestVisibleRun?.run_id ?? "Waiting"}
                      </h3>
                    </div>
                    {runDetail?.run ? (
                      <StatusBadge value={runDetail.run.status} />
                    ) : latestVisibleRun ? (
                      <StatusBadge value={latestVisibleRun.status} />
                    ) : null}
                  </div>
                  <div className="space-y-3 px-5 py-4">
                    <RunKeyValue
                      label="ISO Week"
                      value={runDetail?.run.iso_week ?? latestVisibleRun?.iso_week ?? "n/a"}
                    />
                    <RunKeyValue
                      label="Started"
                      value={formatDate(
                        runDetail?.run.started_at ?? latestVisibleRun?.started_at ?? null,
                      )}
                    />
                    <RunKeyValue
                      label="Docs"
                      value={
                        runDetail?.run.docs_status ??
                        latestVisibleRun?.docs_status ??
                        "pending"
                      }
                    />
                    <RunKeyValue
                      label="Gmail"
                      value={
                        runDetail?.run.gmail_status ??
                        latestVisibleRun?.gmail_status ??
                        "pending"
                      }
                    />
                    {runDetail?.run.warning ?? latestVisibleRun?.warning ? (
                      <p className="rounded-2xl bg-amber-500/12 px-3 py-3 text-sm text-amber-100">
                        {runDetail?.run.warning ?? latestVisibleRun?.warning}
                      </p>
                    ) : null}
                  </div>
                </div>
              </div>
            </SectionPanel>
          </section>

          <section
            id="activity"
            className="scroll-mt-28 grid gap-6 xl:grid-cols-[1.05fr_0.95fr]"
          >
            <SectionPanel
              eyebrow="Activity Log"
              title="Recent pipeline events"
              subtitle="A single feed combining issues, queued jobs, and completed runs so you can see the latest operator movement."
            >
              <ActivityTable events={activityFeed} />
            </SectionPanel>

            <SectionPanel
              eyebrow="Queue Status"
              title="Jobs waiting or recently processed"
              subtitle="One-shot, CSV upload, and weekly triggers appear here with item-level state for Groww."
            >
              <JobList jobs={visibleJobs} />
            </SectionPanel>
          </section>

          <section
            id="runs"
            className="scroll-mt-28 grid gap-6 xl:grid-cols-[1.02fr_0.98fr]"
          >
            <SectionPanel
              eyebrow="Recent Runs"
              title="Run history"
              subtitle="Click a row to inspect the run audit, delivery links, and stage-level posture."
            >
              <RunTable
                runs={visibleRecentRuns}
                selectedRunId={selectedRunId}
                onSelectRun={(run) => {
                  selectRun(run.run_id);
                }}
              />
            </SectionPanel>

            <SectionPanel
              eyebrow="Selected Run Detail"
              title="Docs, Gmail, and audit payload"
              subtitle="The right side shows the live run detail response from the backend for the selected Groww run."
            >
              {runDetail ? (
                <RunDetailPanel
                  detail={runDetail}
                  onResendGmail={handleResendGmail}
                  resending={activeActionKey === `resend:${runDetail.run.run_id}`}
                />
              ) : runDetailStatus === "loading" ? (
                <Placeholder copy="Loading run detail..." />
              ) : latestVisibleRun ? (
                <LatestRunSummary run={latestVisibleRun} />
              ) : (
                <Placeholder copy="No Groww run detail is available yet." />
              )}
            </SectionPanel>
          </section>

          <section
            id="readiness"
            className="scroll-mt-28 grid gap-6 xl:grid-cols-[1fr_1fr]"
          >
            <SectionPanel
              eyebrow="Readiness"
              title="Phase audit and launch posture"
              subtitle="Track documented phase completion alongside the operational checks that still gate a full live flow."
            >
              {overview ? (
                <ProgramStatusPanel
                  audit={overview.completion}
                  readiness={visibleReadiness}
                />
              ) : (
                <Placeholder copy="Waiting for completion audit data." />
              )}
            </SectionPanel>

            <SectionPanel
              eyebrow="Groww Product Card"
              title="Configured product target"
              subtitle="This stays focused on Groww only, with source readiness, recipient count, and a direct one-click run."
            >
              {selectedProduct ? (
                <ProductCard
                  product={selectedProduct}
                  isBusy={activeActionKey === `product:${selectedProduct.slug}`}
                  onRun={() => {
                    void handleTriggerProduct(selectedProduct);
                  }}
                  onInspectRun={(run) => {
                    selectRun(run.run_id);
                  }}
                />
              ) : (
                <Placeholder copy="Groww metadata is not visible from the backend yet." />
              )}
            </SectionPanel>
          </section>

          <footer className="rounded-[1.6rem] border border-emerald-950/8 bg-white/80 px-5 py-4 text-sm text-[var(--muted)] shadow-[var(--shadow)]">
            <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
              <p>
                Auto-refreshes every 10 seconds while this page stays open. The UI is
                intentionally limited to Groww so the signal stays focused.
              </p>
              <div className="flex flex-wrap gap-2">
                <StatusBadge
                  label="Backend"
                  value={loadingOverview ? "loading" : "connected"}
                />
                <StatusBadge
                  label="Run Detail"
                  value={runDetailStatus}
                />
              </div>
            </div>
          </footer>
        </main>
      </div>
    </div>
  );
}

function Banner({
  tone,
  title,
  copy,
}: {
  tone: "danger" | "ready";
  title: string;
  copy: string;
}) {
  const toneClasses =
    tone === "danger"
      ? "border-rose-200 bg-rose-50 text-rose-950"
      : "border-emerald-200 bg-emerald-50 text-emerald-950";

  return (
    <div className={`rounded-[1.4rem] border px-4 py-4 shadow-[var(--shadow)] ${toneClasses}`}>
      <p className="text-[0.68rem] font-semibold uppercase tracking-[0.3em]">{title}</p>
      <p className="mt-2 text-sm leading-6">{copy}</p>
    </div>
  );
}

function SidebarLink({
  href,
  icon,
  label,
}: {
  href: string;
  icon: IconName;
  label: string;
}) {
  return (
    <a
      href={href}
      className="flex items-center gap-3 rounded-2xl px-4 py-3 text-sm font-semibold text-slate-900 transition hover:bg-emerald-50"
    >
      <span className="flex h-9 w-9 items-center justify-center rounded-2xl bg-emerald-500/10 text-emerald-700">
        <MiniIcon name={icon} />
      </span>
      <span>{label}</span>
    </a>
  );
}

function SectionPanel({
  id,
  eyebrow,
  title,
  subtitle,
  children,
}: {
  id?: string;
  eyebrow: string;
  title: string;
  subtitle: string;
  children: React.ReactNode;
}) {
  return (
    <section
      id={id}
      className="scroll-mt-28 rounded-[1.9rem] border border-emerald-950/8 bg-white/85 p-5 shadow-[var(--shadow)] sm:p-6"
    >
      <div className="mb-5">
        <p className="text-[0.68rem] font-semibold uppercase tracking-[0.34em] text-emerald-900/70">
          {eyebrow}
        </p>
        <h2 className="mt-2 text-2xl font-semibold tracking-[-0.04em] text-slate-950">
          {title}
        </h2>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-[var(--muted)]">
          {subtitle}
        </p>
      </div>
      {children}
    </section>
  );
}

function MetricChip({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="rounded-[1.2rem] border border-emerald-950/8 bg-white/88 px-4 py-4">
      <p className="text-[0.65rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
        {label}
      </p>
      <div className="mt-2 break-all text-sm font-semibold text-slate-950">{value}</div>
      <p className="mt-2 text-xs leading-5 text-[var(--muted)]">{note}</p>
    </div>
  );
}

function MetricCard({
  eyebrow,
  value,
  note,
  footer,
  accent,
}: {
  eyebrow: string;
  value: string;
  note: string;
  footer: string;
  accent: "primary" | "warning" | "danger" | "info";
}) {
  return (
    <article
      className={`rounded-[1.7rem] border border-emerald-950/8 bg-[linear-gradient(180deg,var(--tw-gradient-stops))] p-5 shadow-[var(--shadow)] ${metricAccent(
        accent,
      )}`}
    >
      <p className="text-[0.68rem] font-semibold uppercase tracking-[0.32em] text-[var(--muted)]">
        {eyebrow}
      </p>
      <div className="mt-3 text-3xl font-semibold tracking-[-0.05em] text-slate-950">
        {value}
      </div>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{note}</p>
      <div className="mt-4 rounded-full border border-emerald-950/8 bg-white/78 px-3 py-2 text-xs font-semibold uppercase tracking-[0.22em] text-slate-900">
        {footer}
      </div>
    </article>
  );
}

function ProgressRow({
  label,
  value,
  total,
  tone,
}: {
  label: string;
  value: number;
  total: number;
  tone: "ready" | "warning" | "danger";
}) {
  const width = percent(value, total);
  const barClass =
    tone === "ready"
      ? "bg-emerald-500"
      : tone === "warning"
        ? "bg-amber-500"
        : "bg-rose-500";

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-sm">
        <span className="font-medium text-slate-900">{label}</span>
        <span className="font-mono text-[var(--muted)]">
          {value}/{total || 0}
        </span>
      </div>
      <div className="h-2 rounded-full bg-slate-900/8">
        <div className={`h-full rounded-full ${barClass}`} style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}

function PulseStrip({
  label,
  value,
  sublabel,
  percentValue,
  tone,
}: {
  label: string;
  value: string;
  sublabel: string;
  percentValue: number;
  tone: "ready" | "warning";
}) {
  return (
    <div className="rounded-[1.3rem] border border-emerald-950/8 bg-white/75 p-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
            {label}
          </p>
          <div className="mt-1 text-2xl font-semibold tracking-[-0.04em] text-slate-950">
            {value}
          </div>
        </div>
        <StatusBadge value={tone === "ready" ? "ready" : "warning"} />
      </div>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{sublabel}</p>
      <div className="mt-3 h-2 rounded-full bg-slate-900/8">
        <div
          className={`h-full rounded-full ${tone === "ready" ? "bg-emerald-500" : "bg-amber-500"}`}
          style={{ width: `${percentValue}%` }}
        />
      </div>
    </div>
  );
}

function SubPanel({
  title,
  icon,
  children,
}: {
  title: string;
  icon: IconName;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-[1.6rem] border border-emerald-950/8 bg-white/82 p-4">
      <div className="mb-4 flex items-center gap-3">
        <span className="flex h-10 w-10 items-center justify-center rounded-2xl bg-emerald-500/10 text-emerald-700">
          <MiniIcon name={icon} />
        </span>
        <h3 className="text-lg font-semibold tracking-[-0.03em] text-slate-950">
          {title}
        </h3>
      </div>
      {children}
    </div>
  );
}

function CheckTile({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="rounded-[1.2rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(240,249,244,0.9))] p-4">
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-slate-950">{label}</p>
        <StatusBadge value={value} />
      </div>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{note}</p>
    </div>
  );
}

function SignalMeter({
  label,
  status,
  value,
  total,
}: {
  label: string;
  status: string;
  value: number;
  total: number;
}) {
  return (
    <div className="rounded-[1.2rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(235,249,241,0.86))] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
            {label}
          </p>
          <div className="mt-1 text-lg font-semibold text-slate-950">
            {value}/{total || 0}
          </div>
        </div>
        <StatusBadge value={status} />
      </div>
      <div className="mt-3 h-2 rounded-full bg-slate-900/8">
        <div
          className={`h-full rounded-full ${
            statusToneFromValue(status) === "danger"
              ? "bg-rose-500"
              : statusToneFromValue(status) === "warning"
                ? "bg-amber-500"
                : "bg-emerald-500"
          }`}
          style={{ width: `${percent(value, total)}%` }}
        />
      </div>
    </div>
  );
}

function PhaseTile({ phase }: { phase: CompletionAudit["phases"][number] }) {
  const endToEndTone = statusToneFromValue(phase.end_to_end_status);
  const width =
    endToEndTone === "ready"
      ? 100
      : endToEndTone === "warning"
        ? 62
        : endToEndTone === "danger"
          ? 28
          : 45;

  return (
    <article className="rounded-[1.3rem] border border-emerald-950/8 bg-white/82 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
            {phase.phase}
          </p>
          <h4 className="mt-1 text-sm font-semibold text-slate-950">{phase.title}</h4>
        </div>
        <div className="flex flex-wrap gap-2">
          <StatusBadge label="Code" value={phase.implementation_status} />
          <StatusBadge label="E2E" value={phase.end_to_end_status} />
        </div>
      </div>
      <div className="mt-3 h-2 rounded-full bg-slate-900/8">
        <div
          className={`h-full rounded-full ${
            endToEndTone === "ready"
              ? "bg-emerald-500"
              : endToEndTone === "warning"
                ? "bg-amber-500"
                : endToEndTone === "danger"
                  ? "bg-rose-500"
                  : "bg-sky-500"
          }`}
          style={{ width: `${width}%` }}
        />
      </div>
      <div className="mt-3 space-y-2">
        {phase.notes.slice(0, 2).map((note) => (
          <p
            key={note}
            className="rounded-2xl bg-slate-900/[0.03] px-3 py-2 text-sm leading-6 text-[var(--muted)]"
          >
            {note}
          </p>
        ))}
      </div>
    </article>
  );
}

function ServiceCard({ service }: { service: ServiceStatus }) {
  return (
    <article className="rounded-[1.4rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(239,249,244,0.92))] p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-3">
          <span className="flex h-11 w-11 items-center justify-center rounded-2xl bg-emerald-500/10 text-emerald-700">
            <MiniIcon name="servers" />
          </span>
          <div>
            <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
              {categoryLabel(service.category)}
            </p>
            <h3 className="mt-1 text-lg font-semibold text-slate-950">{service.label}</h3>
          </div>
        </div>
        <StatusBadge value={service.status} />
      </div>

      <p className="mt-4 text-sm leading-6 text-[var(--muted)]">{service.detail}</p>

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

function FocusServiceCard({
  title,
  subtitle,
  service,
}: {
  title: string;
  subtitle: string;
  service: ServiceStatus | null;
}) {
  return (
    <article className="rounded-[1.7rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(237,249,243,0.92))] p-5 shadow-[var(--shadow)]">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
            {title}
          </p>
          <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{subtitle}</p>
        </div>
        <StatusBadge value={service?.status ?? "loading"} />
      </div>

      <div className="mt-4 text-2xl font-semibold tracking-[-0.04em] text-slate-950">
        {service?.latency_ms !== null && service?.latency_ms !== undefined
          ? `${service.latency_ms}ms`
          : service?.active
            ? "Active"
            : "Standby"}
      </div>

      <p className="mt-2 min-h-[4.5rem] text-sm leading-6 text-[var(--muted)]">
        {service?.detail ?? "Waiting for live service status from the backend."}
      </p>

      <div className="mt-4 flex flex-wrap gap-2">
        <StatusBadge label="Category" value={service ? categoryLabel(service.category) : "n/a"} />
        <StatusBadge label="Check" value={service ? formatRelativeTime(service.checked_at) : "pending"} />
      </div>
    </article>
  );
}

function ActivityTable({ events }: { events: ActivityEvent[] }) {
  if (!events.length) {
    return <Placeholder copy="No activity events are visible yet." />;
  }

  return (
    <div className="overflow-hidden rounded-[1.6rem] border border-emerald-950/8">
      <div className="grid grid-cols-[0.95fr_0.8fr_1.7fr_0.7fr] gap-3 bg-emerald-50/70 px-4 py-3 text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
        <div>Time</div>
        <div>Type</div>
        <div>Event</div>
        <div>Status</div>
      </div>
      <div className="divide-y divide-emerald-950/8 bg-white/82">
        {events.map((event) => (
          <div
            key={event.id}
            className="grid grid-cols-[0.95fr_0.8fr_1.7fr_0.7fr] gap-3 px-4 py-4"
          >
            <div className="text-sm text-[var(--muted)]">
              <div>{formatDate(event.timestamp)}</div>
              <div className="mt-1 text-xs uppercase tracking-[0.22em]">
                {formatRelativeTime(event.timestamp)}
              </div>
            </div>
            <div className="text-sm font-semibold capitalize text-slate-950">
              {event.kind}
              <div className="mt-1 text-xs font-normal uppercase tracking-[0.22em] text-[var(--muted)]">
                {event.meta}
              </div>
            </div>
            <div>
              <div className="text-sm font-semibold text-slate-950">{event.title}</div>
              <div className="mt-1 text-sm leading-6 text-[var(--muted)]">
                {event.detail}
              </div>
            </div>
            <div className="flex items-start justify-start">
              <StatusBadge value={event.status} />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function SchedulerPanel({ scheduler }: { scheduler: SchedulerStatus }) {
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <SchedulerTile
        label="Status"
        value={scheduler.status}
        note={scheduler.detail}
      />
      <SchedulerTile
        label="Cadence"
        value={scheduler.cadence}
        note={`${scheduler.mode} | ${scheduler.timezone}`}
      />
      <SchedulerTile
        label="Next Run"
        value={scheduler.next_run_at ? formatDate(scheduler.next_run_at) : "Manual only"}
        note={
          scheduler.next_run_at
            ? formatRelativeTime(scheduler.next_run_at)
            : "No auto-run forecast"
        }
      />
      <SchedulerTile
        label="Last Success"
        value={formatDate(scheduler.last_success_at)}
        note={
          scheduler.last_started_at
            ? `Started ${formatDate(scheduler.last_started_at)}`
            : "No run start recorded"
        }
      />
    </div>
  );
}

function SchedulerTile({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="rounded-[1.3rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(239,249,244,0.92))] p-4">
      <div className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
        {label}
      </div>
      <div className="mt-2 text-base font-semibold text-slate-950">{value}</div>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{note}</p>
    </div>
  );
}

function IssueFeed({
  issues,
  compact = false,
}: {
  issues: IssueSnapshot[];
  compact?: boolean;
}) {
  if (!issues.length) {
    return <Placeholder copy="No warnings or errors are open right now." />;
  }

  return (
    <div className="space-y-3">
      {issues.map((issue) => (
        <article
          key={issue.issue_id}
          className="rounded-[1.2rem] border border-emerald-950/8 bg-white/82 p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                {issue.source}
              </p>
              <h3 className="mt-1 text-sm font-semibold text-slate-950">{issue.title}</h3>
            </div>
            <StatusBadge value={issue.severity} />
          </div>
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{issue.detail}</p>
          {!compact ? (
            <div className="mt-3 flex flex-wrap gap-2">
              {issue.product_slug ? (
                <StatusBadge label="Product" value={issue.product_slug} />
              ) : null}
              {issue.run_id ? <StatusBadge label="Run" value={issue.run_id} /> : null}
            </div>
          ) : null}
          <p className="mt-3 text-xs uppercase tracking-[0.24em] text-[var(--muted)]">
            Observed {formatRelativeTime(issue.observed_at)}
          </p>
        </article>
      ))}
    </div>
  );
}

function LockList({
  locks,
  compact = false,
}: {
  locks: LockSnapshot[];
  compact?: boolean;
}) {
  if (!locks.length) {
    return <Placeholder copy="No active or stale run locks are visible right now." />;
  }

  return (
    <div className="space-y-3">
      {locks.map((lock) => (
        <article
          key={lock.key}
          className="rounded-[1.2rem] border border-emerald-950/8 bg-white/82 p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                {lock.product_slug}
              </p>
              <h3 className="mt-1 text-sm font-semibold text-slate-950">{lock.iso_week}</h3>
            </div>
            <StatusBadge value={lock.status} />
          </div>
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{lock.detail}</p>
          <div className="mt-3 flex flex-wrap gap-2">
            <StatusBadge label="Age" value={formatAgeSeconds(lock.age_seconds)} />
            {!compact && lock.pid !== null ? (
              <StatusBadge label="PID" value={String(lock.pid)} />
            ) : null}
          </div>
          {!compact ? (
            <p className="mt-3 break-all font-mono text-xs text-[var(--muted)]">{lock.path}</p>
          ) : null}
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
      <div className="rounded-[1.5rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(231,249,240,0.92),rgba(255,255,255,0.96))] p-4">
        <div className="flex flex-wrap items-center gap-3">
          <StatusBadge value={audit.overall_status} />
          <p className="text-sm leading-6 text-[var(--muted)]">
            {audit.notes[0] ?? "No completion notes returned yet."}
          </p>
        </div>
      </div>

      <div className="space-y-3">
        {audit.phases.map((phase) => (
          <div
            key={phase.phase}
            className="rounded-[1.4rem] border border-emerald-950/8 bg-white/82 p-4"
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
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
                  className="rounded-2xl bg-slate-900/[0.03] px-3 py-2 text-sm text-[var(--muted)]"
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
            className="rounded-[1.4rem] border border-emerald-950/8 bg-white/82 p-4"
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
    <article className="flex h-full flex-col rounded-[1.8rem] border border-emerald-950/8 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(237,249,243,0.92))] p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
            {product.slug}
          </p>
          <h3 className="mt-1 text-2xl font-semibold tracking-[-0.03em] text-slate-950">
            {product.display_name}
          </h3>
        </div>
        <StatusBadge value={product.active ? "ready" : "warning"} />
      </div>

      <div className="mt-4 grid gap-2 sm:grid-cols-2">
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

      <div className="mt-5 rounded-[1.4rem] border border-emerald-950/8 bg-slate-950/[0.035] p-4">
        <div className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
          Latest run
        </div>
        {product.latest_run ? (
          <div className="mt-3 space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge value={product.latest_run.status} />
              <StatusBadge label="Stage" value={product.latest_run.stage} />
              <StatusBadge
                label="Input"
                value={inputModeLabel(product.latest_run.input_mode)}
              />
              {product.latest_run.docs_status ? (
                <StatusBadge label="Docs" value={product.latest_run.docs_status} />
              ) : null}
              {product.latest_run.gmail_status ? (
                <StatusBadge label="Gmail" value={product.latest_run.gmail_status} />
              ) : null}
            </div>
            <p className="text-sm leading-6 text-[var(--muted)]">
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
              className="rounded-full border border-emerald-950/10 px-4 py-2 text-sm font-semibold text-slate-900 transition hover:bg-white"
            >
              Inspect latest run
            </button>
          </div>
        ) : (
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
            No runs have been recorded yet for this product.
          </p>
        )}
      </div>

      <div className="mt-5 flex-1">
        <div className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
          Product issues
        </div>
        {product.issues.length ? (
          <ul className="mt-2 space-y-2 text-sm leading-6 text-[var(--muted)]">
            {product.issues.map((issue) => (
              <li key={issue} className="rounded-2xl bg-amber-50 px-3 py-2 text-amber-950">
                {issue}
              </li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 rounded-2xl bg-emerald-50 px-3 py-3 text-sm text-emerald-950">
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
          className="rounded-full bg-slate-950 px-4 py-3 text-sm font-semibold text-white transition hover:translate-y-[-1px] hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
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
    <div className="overflow-hidden rounded-[1.6rem] border border-emerald-950/8">
      <div className="grid grid-cols-[1.05fr_0.75fr_0.82fr_1fr] gap-3 bg-emerald-50/70 px-4 py-3 text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
        <div>Product</div>
        <div>Week</div>
        <div>Status</div>
        <div>Started</div>
      </div>
      <div className="divide-y divide-emerald-950/8 bg-white/78">
        {runs.map((run) => {
          const isSelected = selectedRunId === run.run_id;
          return (
            <button
              key={run.run_id}
              type="button"
              onClick={() => {
                onSelectRun(run);
              }}
              className={`grid w-full grid-cols-[1.05fr_0.75fr_0.82fr_1fr] gap-3 px-4 py-4 text-left transition ${
                isSelected ? "bg-emerald-500/8" : "hover:bg-emerald-50/70"
              }`}
            >
              <div>
                <div className="font-semibold text-slate-950">
                  {humanizeSlug(run.product_slug)}
                </div>
                <div className="mt-1 flex flex-wrap items-center gap-2">
                  <span className="font-mono text-xs text-[var(--muted)]">{run.run_id}</span>
                  <span className="rounded-full bg-emerald-50 px-2 py-1 text-[0.65rem] font-semibold uppercase tracking-[0.18em] text-emerald-950">
                    {inputModeLabel(run.input_mode)}
                  </span>
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
          className="rounded-[1.4rem] border border-emerald-950/8 bg-white/82 p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                {job.kind}
              </p>
              <h3 className="mt-1 text-sm font-semibold text-slate-950">
                {job.product_slug ? humanizeSlug(job.product_slug) : "All active products"}
              </h3>
            </div>
            <StatusBadge value={job.status} />
          </div>
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
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
          {job.warning ? (
            <p className="mt-3 rounded-2xl bg-amber-50 px-3 py-2 text-sm text-amber-950">
              {job.warning}
            </p>
          ) : null}
          {job.error ? (
            <p className="mt-3 rounded-2xl bg-rose-50 px-3 py-2 text-sm text-rose-950">
              {job.error}
            </p>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function RunDetailPanel({
  detail,
  onResendGmail,
  resending,
}: {
  detail: RunDetail;
  onResendGmail: (run: RunSummary) => void;
  resending: boolean;
}) {
  const auditPreview = JSON.stringify(detail.audit, null, 2);
  const canResendGmail = detail.run.gmail_status === "sent";

  return (
    <div className="space-y-4">
      <div className="rounded-[1.5rem] border border-emerald-950/8 bg-white/82 p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge value={detail.run.status} />
            <StatusBadge label="Stage" value={detail.run.stage} />
            <StatusBadge label="Input" value={inputModeLabel(detail.run.input_mode)} />
            {detail.run.docs_status ? (
              <StatusBadge label="Docs" value={detail.run.docs_status} />
            ) : null}
            {detail.run.gmail_status ? (
              <StatusBadge label="Gmail" value={detail.run.gmail_status} />
            ) : null}
          </div>
          <button
            type="button"
            onClick={() => {
              onResendGmail(detail.run);
            }}
            disabled={!canResendGmail || resending}
            className="inline-flex items-center gap-2 rounded-2xl border border-emerald-950/10 bg-slate-950 px-4 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-300 disabled:text-slate-600"
          >
            <MiniIcon name="delivery" />
            {resending ? "Resending..." : "Send Gmail Again"}
          </button>
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
            <span className="font-semibold text-slate-950">Input mode:</span>{" "}
            {inputModeLabel(detail.run.input_mode)}
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
            <p className="rounded-2xl bg-amber-50 px-3 py-2 text-amber-950">
              {detail.run.warning}
            </p>
          ) : null}
        </div>
      </div>

      <div className="space-y-3">
        {detail.deliveries.map((delivery) => (
          <div
            key={`${detail.run.run_id}-${delivery.target}`}
            className="rounded-[1.4rem] border border-emerald-950/8 bg-white/82 p-4"
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
                  className="inline-flex rounded-full border border-emerald-950/10 px-3 py-2 font-semibold text-slate-950 transition hover:bg-emerald-50"
                >
                  Open delivery link
                </a>
              ) : null}
            </div>
          </div>
        ))}
      </div>

      <div className="overflow-hidden rounded-[1.5rem] border border-emerald-950/8 bg-slate-950 text-slate-100">
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

function LatestRunSummary({ run }: { run: RunSummary }) {
  return (
    <div className="rounded-[1.5rem] border border-emerald-950/8 bg-white/82 p-4">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge value={run.status} />
        <StatusBadge label="Stage" value={run.stage} />
      </div>
      <div className="mt-4 space-y-2 text-sm leading-6 text-[var(--muted)]">
        <p>
          <span className="font-semibold text-slate-950">Run ID:</span>{" "}
          <span className="font-mono">{run.run_id}</span>
        </p>
        <p>
          <span className="font-semibold text-slate-950">ISO week:</span> {run.iso_week}
        </p>
        <p>
          <span className="font-semibold text-slate-950">Started:</span>{" "}
          {formatDate(run.started_at)}
        </p>
      </div>
    </div>
  );
}

function Placeholder({ copy }: { copy: string }) {
  return (
    <div className="rounded-[1.5rem] border border-dashed border-emerald-950/15 bg-emerald-50/60 px-4 py-8 text-center text-sm leading-6 text-[var(--muted)]">
      {copy}
    </div>
  );
}

function RunKeyValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-2xl bg-white/5 px-3 py-3">
      <span className="text-sm text-slate-300">{label}</span>
      <span className="text-sm font-semibold text-white">{value}</span>
    </div>
  );
}

type IconName =
  | "activity"
  | "alert"
  | "checklist"
  | "dashboard"
  | "delivery"
  | "ingestion"
  | "leaf"
  | "lock"
  | "phases"
  | "play"
  | "pulse"
  | "refresh"
  | "runs"
  | "search"
  | "servers";

function MiniIcon({
  name,
  className = "",
}: {
  name: IconName;
  className?: string;
}) {
  const shared =
    "h-[1.1rem] w-[1.1rem] shrink-0 stroke-current fill-none stroke-[1.8]";

  switch (name) {
    case "activity":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M3 12h4l2.5-5 5 10 2.5-5H21" />
        </svg>
      );
    case "alert":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M12 4 3 20h18L12 4Z" />
          <path d="M12 9v4" />
          <path d="M12 17h.01" />
        </svg>
      );
    case "checklist":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M9 6h11" />
          <path d="M9 12h11" />
          <path d="M9 18h11" />
          <path d="m4 6 1.5 1.5L7.5 5" />
          <path d="m4 12 1.5 1.5L7.5 11" />
          <path d="m4 18 1.5 1.5L7.5 17" />
        </svg>
      );
    case "dashboard":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <rect x="3" y="3" width="8" height="8" rx="1.8" />
          <rect x="13" y="3" width="8" height="5" rx="1.8" />
          <rect x="13" y="10" width="8" height="11" rx="1.8" />
          <rect x="3" y="13" width="8" height="8" rx="1.8" />
        </svg>
      );
    case "delivery":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M4 7h9v10H4z" />
          <path d="M13 10h4l3 3v4h-7" />
          <path d="M7 17h.01" />
          <path d="M18 17h.01" />
        </svg>
      );
    case "ingestion":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M12 3v18" />
          <path d="m7 8 5-5 5 5" />
          <path d="m17 16-5 5-5-5" />
        </svg>
      );
    case "leaf":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M6 19C6 9 14 5 20 5c0 10-8 14-14 14Z" />
          <path d="M8 16c1-2 3-4 7-6" />
        </svg>
      );
    case "lock":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <rect x="5" y="11" width="14" height="10" rx="2" />
          <path d="M8 11V8a4 4 0 0 1 8 0v3" />
        </svg>
      );
    case "phases":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M4 7h6v10H4z" />
          <path d="M14 4h6v16h-6z" />
        </svg>
      );
    case "play":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="m8 5 11 7-11 7Z" />
        </svg>
      );
    case "pulse":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M3 12h4l2-5 4 10 2-5h6" />
        </svg>
      );
    case "refresh":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M20 11a8 8 0 1 0 2 5.5" />
          <path d="M20 4v7h-7" />
        </svg>
      );
    case "runs":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <path d="M4 7h16" />
          <path d="M4 12h10" />
          <path d="M4 17h16" />
          <path d="M17 10l3 2-3 2" />
        </svg>
      );
    case "search":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <circle cx="11" cy="11" r="6" />
          <path d="m20 20-4.2-4.2" />
        </svg>
      );
    case "servers":
      return (
        <svg viewBox="0 0 24 24" className={`${shared} ${className}`}>
          <rect x="4" y="4" width="16" height="6" rx="1.8" />
          <rect x="4" y="14" width="16" height="6" rx="1.8" />
          <path d="M8 7h.01" />
          <path d="M8 17h.01" />
        </svg>
      );
    default:
      return null;
  }
}

