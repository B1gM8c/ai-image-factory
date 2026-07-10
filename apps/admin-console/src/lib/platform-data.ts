import {
  Activity,
  Boxes,
  CircuitBoard,
  Clock3,
  CreditCard,
  FileJson,
  Gauge,
  KeyRound,
  RadioTower,
} from "lucide-react";

export const navItems = [
  { label: "Overview", icon: Activity },
  { label: "Providers", icon: Boxes },
  { label: "Keys", icon: KeyRound },
  { label: "Billing", icon: CreditCard },
  { label: "Scheduling", icon: Clock3 },
  { label: "OpenAPI", icon: FileJson },
  { label: "Telemetry", icon: RadioTower },
];

export const metrics = [
  {
    label: "Active provider",
    value: "1",
    detail: "openai-codex",
    tone: "green" as const,
    icon: CircuitBoard,
  },
  {
    label: "Planned providers",
    value: "4",
    detail: "Midjourney, JiMeng, Grok, Seedance",
    tone: "blue" as const,
    icon: Boxes,
  },
  {
    label: "Default quota",
    value: "40 / 200",
    detail: "5h and 7d image units",
    tone: "amber" as const,
    icon: Gauge,
  },
  {
    label: "Execution mode",
    value: "sync",
    detail: "async jobs next",
    tone: "neutral" as const,
    icon: Clock3,
  },
];

export const providers = [
  {
    id: "openai-codex",
    name: "OpenAI GPT Image via Codex CLI",
    status: "Active",
    model: "gpt-image-2",
    mode: "Native CLI",
    capability: "Generate, edit, final-only SSE",
  },
  {
    id: "midjourney",
    name: "Midjourney",
    status: "Planned",
    model: "midjourney-v7",
    mode: "Managed API",
    capability: "Async generation",
  },
  {
    id: "jimeng-cli",
    name: "JiMeng CLI",
    status: "Planned",
    model: "jimeng-image, jimeng-video",
    mode: "CLI bridge",
    capability: "Async image and video generation",
  },
  {
    id: "grok-cli",
    name: "Grok CLI",
    status: "Planned",
    model: "grok-image",
    mode: "CLI bridge",
    capability: "Async generation",
  },
  {
    id: "seedance-cli",
    name: "Seedance CLI",
    status: "Planned",
    model: "seedance-video",
    mode: "CLI bridge",
    capability: "Async video generation",
  },
];

export const jobs = [
  {
    id: "req_local",
    provider: "openai-codex",
    state: "synchronous",
    units: "n image units",
    latency: "Codex bounded by request timeout",
  },
  {
    id: "job_async",
    provider: "provider registry",
    state: "planned",
    units: "reservation -> ledger",
    latency: "lease/poll/fetch",
  },
  {
    id: "otel_trace",
    provider: "platform",
    state: "active",
    units: "request id",
    latency: "OTLP traces",
  },
];
