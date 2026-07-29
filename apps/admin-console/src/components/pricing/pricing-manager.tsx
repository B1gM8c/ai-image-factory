"use client";

import { useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  ArrowRight,
  CheckCircle2,
  Download,
  ExternalLink,
  FileClock,
  Plus,
  RefreshCw,
  Search,
} from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { PageHeader } from "@/components/page-header";
import { PricingCoverageTable } from "@/components/pricing/pricing-coverage-table";
import { PriceVersionDialog } from "@/components/pricing/price-version-dialog";
import { OfficialPricingImportDialog } from "@/components/pricing/official-pricing-import-dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAdminQuery } from "@/hooks/use-admin-query";
import { formatDateTime, formatInteger, formatMoneyMicros } from "@/lib/admin/format";
import type {
  PriceBook,
  PriceBookCatalog,
  PriceBookPurpose,
  PriceBookVersion,
  PricePublishReadiness,
  PriceRollbackDraftResult,
  PricingCoverageRow,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const ENDPOINT = "/admin/v1/pricing/price-books";

type PricingView = "customer" | "provider" | "benchmark" | "all";
type PricingSection = "rules" | "coverage";
type CatalogRow = { book: PriceBook; version: PriceBookVersion | null };
type Transition = { version: PriceBookVersion; action: "publish" | "retire" };
type VersionLifecycle = "current" | "scheduled" | "ended" | "draft" | "retired" | "unconfigured";

export function PricingManager() {
  const query = useAdminQuery<PriceBookCatalog>(ENDPOINT);
  const [section, setSection] = useState<PricingSection>("rules");
  const [view, setView] = useState<PricingView>("customer");
  const [search, setSearch] = useState("");
  const [provider, setProvider] = useState("all");
  const [state, setState] = useState("all");
  const [createOpen, setCreateOpen] = useState(false);
  const [officialImportOpen, setOfficialImportOpen] = useState(false);
  const [selected, setSelected] = useState<{ bookId: string; versionId: string | null } | null>(null);
  const [editorBook, setEditorBook] = useState<PriceBook | null>(null);
  const [editorVersion, setEditorVersion] = useState<PriceBookVersion | null>(null);
  const [editorPreset, setEditorPreset] = useState<PricingCoverageRow | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [coverageRevision, setCoverageRevision] = useState(0);
  const [configuringSurfaceKey, setConfiguringSurfaceKey] = useState<string | null>(null);
  const [transition, setTransition] = useState<Transition | null>(null);

  const books = query.data?.price_books ?? [];
  const providers = useMemo(
    () => Array.from(new Set(books.flatMap((book) => [book.provider_id, ...book.versions.map((version) => version.provider_id)].filter(Boolean) as string[]))).sort(),
    [books],
  );
  const rows = useMemo(
    () =>
      filterRows(flattenCatalog(books), {
        view,
        search,
        provider,
        state,
        asOfMs: query.data?.as_of_ms,
      }),
    [books, provider, query.data?.as_of_ms, search, state, view],
  );
  const selectedBook = selected
    ? books.find((book) => book.price_book_id === selected.bookId) ?? null
    : null;
  const selectedVersion = selectedBook && selected?.versionId
    ? selectedBook.versions.find((version) => version.price_book_version_id === selected.versionId) ?? null
    : null;

  const activeCount = books.reduce(
    (total, book) =>
      total
      + book.versions.filter(
        (version) => versionLifecycle(version, query.data?.as_of_ms ?? Date.now()) === "current",
      ).length,
    0,
  );
  const scheduledCount = books.reduce(
    (total, book) =>
      total
      + book.versions.filter(
        (version) => versionLifecycle(version, query.data?.as_of_ms ?? Date.now()) === "scheduled",
      ).length,
    0,
  );
  const draftCount = books.reduce(
    (total, book) => total + book.versions.filter((version) => version.state === "draft").length,
    0,
  );
  const sourcedCount = books.reduce(
    (total, book) =>
      total
      + book.versions.filter((version) =>
        version.source_kind === "official_document" || version.source_kind === "provider_contract"
      ).length,
    0,
  );

  function openVersionEditor(
    book: PriceBook,
    version: PriceBookVersion | null = null,
    preset: PricingCoverageRow | null = null,
  ) {
    setSelected(null);
    setEditorBook(book);
    setEditorVersion(version);
    setEditorPreset(preset);
    setEditorOpen(true);
  }

  async function configureSurfacePrice(surface: PricingCoverageRow) {
    if (!surface.api_profile || !surface.public_model_id || !surface.pricing_operation) {
      toast.error("这个模型入口尚未形成完整的定价契约");
      return;
    }
    const surfaceKey = [
      surface.provider_id,
      surface.provider_model_id,
      surface.api_profile,
      surface.public_model_id,
      surface.pricing_operation,
    ].join(":");
    setConfiguringSurfaceKey(surfaceKey);
    try {
      let book = books
        .filter((candidate) =>
          candidate.purpose === "customer_sale"
          && candidate.scope_type === "platform"
          && candidate.currency === "USD"
          && (candidate.provider_id === surface.provider_id || candidate.provider_id === null)
        )
        .sort((left, right) =>
          Number(right.provider_id === surface.provider_id)
          - Number(left.provider_id === surface.provider_id)
        )[0];
      if (!book) {
        const response = await consoleFetch("/api/gateway/admin/v1/pricing/price-books", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            price_book_key: buildPriceBookKey("customer_sale", surface.provider_id),
            display_name: `${surface.provider_display_name} 标准客户售价`,
            purpose: "customer_sale",
            scope_type: "platform",
            organization_id: null,
            project_id: null,
            provider_id: surface.provider_id,
            currency: "USD",
          }),
        });
        if (!response.ok) {
          toast.error(await responseMessage(response));
          return;
        }
        book = (await response.json()) as PriceBook;
        query.retry();
      }
      const draft = book.versions.find((version) =>
        version.state === "draft"
        && version.api_profile === surface.api_profile
        && version.operation === surface.pricing_operation
        && version.provider_model_id === surface.provider_model_id
        && version.public_model_id === surface.public_model_id
        && version.media_kind === surface.media_kind
      ) ?? null;
      openVersionEditor(book, draft, draft ? null : surface);
    } catch {
      toast.error("定价配置暂时不可用，请稍后重试");
    } finally {
      setConfiguringSurfaceKey(null);
    }
  }

  async function createRollbackDraft(version: PriceBookVersion) {
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/pricing/price-book-versions/${version.price_book_version_id}/rollback-draft`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ effective_from_ms: Date.now() }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const result = (await response.json()) as PriceRollbackDraftResult;
      toast.success(`已从 v${version.version} 创建回滚草稿`);
      query.retry();
      setSelected({
        bookId: result.draft.price_book_id,
        versionId: result.draft.price_book_version_id,
      });
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "创建回滚草稿失败");
    }
  }

  return (
    <div className="min-w-0 space-y-6 overflow-x-clip">
      <PageHeader
        title="模型定价"
        description={section === "rules"
          ? "管理对外销售价、供应商成本和基准价格；已发布版本保持不可变，以便用量重放与账单审计。"
          : "核对每个 API 模型入口的路由、客户售价、计量规则与上游成本覆盖。"}
        actions={
          <>
            <Button type="button" variant="outline" size="sm" onClick={query.retry} disabled={query.refreshing}>
              <RefreshCw className={query.refreshing ? "animate-spin" : ""} aria-hidden="true" />
              刷新
            </Button>
            <Button type="button" variant="outline" size="sm" onClick={() => setOfficialImportOpen(true)}>
              <Download aria-hidden="true" />
              同步官方价格
            </Button>
            <Button type="button" size="sm" onClick={() => setCreateOpen(true)}>
              <Plus aria-hidden="true" />
              新建价格簿
            </Button>
          </>
        }
      />

      <Tabs value={section} onValueChange={(value) => setSection(value as PricingSection)}>
        <TabsList className="h-9">
          <TabsTrigger value="rules">价格规则</TabsTrigger>
          <TabsTrigger value="coverage">覆盖检查</TabsTrigger>
        </TabsList>
      </Tabs>

      {section === "coverage" ? (
            <PricingCoverageTable
              key={coverageRevision}
              onConfigurePrice={configureSurfacePrice}
              configuringSurfaceKey={configuringSurfaceKey}
            />
      ) : null}

      {section === "rules" && query.loading ? <AdminQuerySkeleton rows={9} /> : null}
      {section === "rules" && !query.loading && query.error && (!query.data || query.error.status === 403) ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}

      {section === "rules" && query.data && (!query.error || query.error.status !== 403) ? (
        <>
          <div className="flex flex-wrap items-center gap-x-6 gap-y-2 border-y py-3 text-sm">
            <SummaryValue value={activeCount} label="生效模型价格" />
            <SummaryValue value={scheduledCount} label="等待生效" />
            <SummaryValue value={draftCount} label="待发布草稿" />
            <SummaryValue value={sourcedCount} label="可复核官方/合同来源" />
            <span className="ml-auto text-xs text-muted-foreground">
              数据截至 {formatDateTime(query.data.as_of_ms)}
            </span>
          </div>

          <Tabs value={view} onValueChange={(value) => setView(value as PricingView)}>
            <TabsList className="h-9">
              <TabsTrigger value="customer">客户售价</TabsTrigger>
              <TabsTrigger value="provider">供应成本</TabsTrigger>
              <TabsTrigger value="benchmark">市场基准</TabsTrigger>
              <TabsTrigger value="all">全部</TabsTrigger>
            </TabsList>
          </Tabs>

          <div className="flex flex-col gap-3 lg:flex-row">
            <div className="relative min-w-0 flex-1">
              <Search className="pointer-events-none absolute left-3 top-2.5 size-4 text-muted-foreground" aria-hidden="true" />
              <Input
                className="pl-9"
                value={search}
                placeholder="搜索外部模型、原生模型或价格簿"
                onChange={(event) => setSearch(event.target.value)}
              />
            </div>
            <Select value={provider} onValueChange={setProvider}>
              <SelectTrigger className="w-full lg:w-48"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部供应商</SelectItem>
                {providers.map((item) => <SelectItem key={item} value={item}>{providerLabel(item)}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={state} onValueChange={setState}>
              <SelectTrigger className="w-full lg:w-40"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部状态</SelectItem>
                <SelectItem value="current">当前生效</SelectItem>
                <SelectItem value="scheduled">等待生效</SelectItem>
                <SelectItem value="draft">草稿</SelectItem>
                <SelectItem value="ended">已结束</SelectItem>
                <SelectItem value="retired">已退役</SelectItem>
                <SelectItem value="unconfigured">未配置</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="min-w-0 overflow-hidden rounded-md border">
            {rows.length === 0 ? (
              <EmptyCatalog
                hasBooks={books.length > 0}
                onCreate={() => setCreateOpen(true)}
              />
            ) : (
              <div className="overflow-x-auto">
                <Table className="min-w-[1040px]">
                  <TableHeader>
                    <TableRow>
                      <TableHead className="pl-4">模型</TableHead>
                      <TableHead>价格类型</TableHead>
                      <TableHead>供应商</TableHead>
                      <TableHead>计量与单价</TableHead>
                      <TableHead>状态</TableHead>
                      <TableHead>生效时间</TableHead>
                      <TableHead>来源</TableHead>
                      <TableHead className="w-16 pr-4 text-right">详情</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {rows.map(({ book, version }) => (
                      <TableRow key={version?.price_book_version_id ?? book.price_book_id}>
                        <TableCell className="pl-4">
                          <button
                            type="button"
                            className="max-w-64 text-left"
                            onClick={() => setSelected({ bookId: book.price_book_id, versionId: version?.price_book_version_id ?? null })}
                          >
                            <span className="block truncate font-medium">
                              {version?.public_model_id ?? book.display_name}
                            </span>
                            <span className="mt-0.5 block truncate text-xs text-muted-foreground">
                              {version
                                ? `${version.api_profile} · ${version.media_kind === "video" ? "视频" : "图片"}`
                                : "尚未添加模型价格"}
                            </span>
                          </button>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">{purposeLabel(book.purpose)}</span>
                          <span className="mt-0.5 block text-xs text-muted-foreground">{book.currency}</span>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">{providerLabel(version?.provider_id ?? book.provider_id)}</span>
                          {version?.provider_model_id ? (
                            <span className="mt-0.5 block max-w-40 truncate font-mono text-xs text-muted-foreground">
                              {version.provider_model_id}
                            </span>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          {version ? (
                            <PriceSummary version={version} currency={book.currency} />
                          ) : (
                            <Button type="button" variant="outline" size="sm" onClick={() => openVersionEditor(book)}>
                              <Plus aria-hidden="true" />
                              添加模型价格
                            </Button>
                          )}
                        </TableCell>
                        <TableCell>
                          <VersionStateBadge
                            version={version}
                            asOfMs={query.data?.as_of_ms ?? Date.now()}
                          />
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {version ? formatDateTime(version.effective_from_ms) : "--"}
                        </TableCell>
                        <TableCell>
                          {version ? <SourceSummary version={version} /> : <span className="text-muted-foreground">--</span>}
                        </TableCell>
                        <TableCell className="pr-4 text-right">
                          <Button
                            type="button"
                            size="icon"
                            variant="ghost"
                            aria-label="查看价格详情"
                            onClick={() => setSelected({ bookId: book.price_book_id, versionId: version?.price_book_version_id ?? null })}
                          >
                            <ArrowRight aria-hidden="true" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </div>
        </>
      ) : null}

      <CreatePriceBookDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        onCreated={(book) => {
          query.retry();
          openVersionEditor(book);
        }}
      />

      <OfficialPricingImportDialog
        open={officialImportOpen}
        onOpenChange={setOfficialImportOpen}
        onImported={() => {
          setView("benchmark");
          query.retry();
        }}
      />

      <PriceBookSheet
        book={selectedBook}
        version={selectedVersion}
        open={Boolean(selectedBook)}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
        onAddVersion={(book) => openVersionEditor(book)}
        onEditVersion={(book, version) => openVersionEditor(book, version)}
        onTransition={setTransition}
        onRollback={(version) => void createRollbackDraft(version)}
        asOfMs={query.data?.as_of_ms ?? Date.now()}
      />

      <PriceVersionDialog
        book={editorBook}
        version={editorVersion}
        preset={editorPreset}
        open={editorOpen}
        onOpenChange={setEditorOpen}
        onSaved={(saved) => {
          query.retry();
          setCoverageRevision((current) => current + 1);
          setSelected({ bookId: saved.price_book_id, versionId: saved.price_book_version_id });
        }}
      />

      <VersionTransitionDialog
        transition={transition}
        onOpenChange={(open) => {
          if (!open) setTransition(null);
        }}
        onCompleted={() => {
          setTransition(null);
          setSelected(null);
          query.retry();
        }}
      />
    </div>
  );
}

function PriceBookSheet({
  book,
  version,
  open,
  onOpenChange,
  onAddVersion,
  onEditVersion,
  onTransition,
  onRollback,
  asOfMs,
}: {
  book: PriceBook | null;
  version: PriceBookVersion | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAddVersion: (book: PriceBook) => void;
  onEditVersion: (book: PriceBook, version: PriceBookVersion) => void;
  onTransition: (transition: Transition) => void;
  onRollback: (version: PriceBookVersion) => void;
  asOfMs: number;
}) {
  if (!book) return null;
  const lifecycle = version ? versionLifecycle(version, asOfMs) : "unconfigured";

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="border-b px-6 py-5 pr-12">
          <div className="flex flex-wrap items-center gap-2">
            <SheetTitle>{version?.public_model_id ?? book.display_name}</SheetTitle>
            <VersionStateBadge version={version} asOfMs={asOfMs} />
          </div>
          <SheetDescription>
            {book.display_name} · {purposeLabel(book.purpose)} · {book.currency}
          </SheetDescription>
        </SheetHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
          {version ? (
            <div className="space-y-8">
              <DetailSection title="模型与路由">
                <Definition label="外部模型 ID" value={version.public_model_id} mono />
                <Definition label="原生模型 ID" value={version.provider_model_id ?? "--"} mono />
                <Definition label="API 协议" value={version.api_profile} mono />
                <Definition label="操作" value={version.operation} mono />
                <Definition label="供应商" value={providerLabel(version.provider_id ?? book.provider_id)} />
                <Definition label="执行渠道" value={executionSurfaceLabel(version.execution_surface)} />
              </DetailSection>

              <section>
                <h3 className="text-sm font-semibold">计量与价格</h3>
                <p className="mt-1 text-sm text-muted-foreground">
                  {billingModeLabel(version.billing_mode)}
                  {version.is_free ? " · 免费版本" : ""}
                </p>
                <div className="mt-4 overflow-hidden rounded-md border">
                  {version.billing_mode === "provider_reported" ? (
                    <div className="px-4 py-6 text-sm text-muted-foreground">
                      金额由供应商终态回执提供，不使用静态价格组件。
                    </div>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="pl-4">指标</TableHead>
                          <TableHead>适用结果</TableHead>
                          <TableHead>数量来源</TableHead>
                          <TableHead className="pr-4 text-right">单价</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {version.components.map((component) => (
                          <TableRow key={component.price_component_id}>
                            <TableCell className="pl-4">
                              <span className="font-medium">{metricLabel(component.metric)}</span>
                              <span className="mt-0.5 block font-mono text-xs text-muted-foreground">
                                {component.component_key}
                              </span>
                            </TableCell>
                            <TableCell>{outcomeLabel(component.outcome)}</TableCell>
                            <TableCell>{quantitySourceLabel(component.quantity_source)}</TableCell>
                            <TableCell className="pr-4 text-right font-mono tabular-nums">
                              {formatMoneyMicros(component.unit_price_micros, book.currency)}
                              <span className="block text-xs text-muted-foreground">
                                / {formatInteger(component.unit_size)} {unitLabel(component.unit)}
                              </span>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </div>
              </section>

              <DetailSection title="版本与来源">
                <Definition label="版本" value={`v${version.version}`} />
                <Definition label="生效时间" value={formatDateTime(version.effective_from_ms)} />
                <Definition
                  label="结束时间"
                  value={version.effective_until_ms ? formatDateTime(version.effective_until_ms) : "持续生效"}
                />
                <Definition label="来源类型" value={sourceKindLabel(version.source_kind)} />
                <Definition label="核验时间" value={formatDateTime(version.source_checked_at_ms)} />
                <Definition label="服务层级" value={version.service_tier} mono />
                <Definition label="控制版本" value={version.control_version} mono />
              </DetailSection>

              {version.source_url ? (
                <Button type="button" variant="outline" size="sm" asChild>
                  <a href={version.source_url} target="_blank" rel="noreferrer">
                    <ExternalLink aria-hidden="true" />
                    查看定价来源
                  </a>
                </Button>
              ) : null}
              {version.notes ? (
                <div className="rounded-md bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
                  {version.notes}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="flex min-h-72 flex-col items-center justify-center text-center">
              <FileClock className="size-8 text-muted-foreground" aria-hidden="true" />
              <h3 className="mt-4 font-medium">这个价格簿还没有模型价格</h3>
              <p className="mt-1 max-w-sm text-sm text-muted-foreground">
                添加模型、API 协议、计量单位、单价与来源后，再发布为可结算版本。
              </p>
              <Button type="button" className="mt-5" onClick={() => onAddVersion(book)}>
                <Plus aria-hidden="true" />
                添加模型价格
              </Button>
            </div>
          )}
        </div>

        {version ? (
          <div className="flex flex-wrap justify-end gap-2 border-t bg-background px-6 py-4">
            {version.state === "draft" ? (
              <>
                <Button type="button" variant="outline" onClick={() => onEditVersion(book, version)}>
                  编辑草稿
                </Button>
                <Button type="button" onClick={() => onTransition({ version, action: "publish" })}>
                  发布价格
                </Button>
              </>
            ) : null}
            {version.state === "active" && lifecycle === "scheduled" ? (
              <Button type="button" variant="outline" onClick={() => onTransition({ version, action: "retire" })}>
                取消计划
              </Button>
            ) : null}
            {lifecycle === "ended" || lifecycle === "retired" ? (
              <Button type="button" variant="outline" onClick={() => onRollback(version)}>
                从此版本回滚
              </Button>
            ) : null}
            <Button type="button" variant="outline" onClick={() => onAddVersion(book)}>
              <Plus aria-hidden="true" />
              新建版本
            </Button>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function CreatePriceBookDialog({
  open,
  onOpenChange,
  onCreated,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: (book: PriceBook) => void;
}) {
  const [displayName, setDisplayName] = useState("");
  const [purpose, setPurpose] = useState<PriceBookPurpose>("customer_sale");
  const [scope, setScope] = useState<PriceBook["scope_type"]>("platform");
  const [organizationId, setOrganizationId] = useState("");
  const [projectId, setProjectId] = useState("");
  const [providerId, setProviderId] = useState("");
  const [currency, setCurrency] = useState("USD");
  const [saving, setSaving] = useState(false);

  async function create() {
    if (!displayName.trim()) {
      toast.error("请填写价格簿名称");
      return;
    }
    if (purpose !== "customer_sale" && !providerId.trim()) {
      toast.error("供应成本与基准价格必须指定供应商");
      return;
    }
    if (scope !== "platform" && !organizationId.trim()) {
      toast.error("组织或项目级价格簿必须指定组织");
      return;
    }
    if (scope === "project" && !projectId.trim()) {
      toast.error("项目级价格簿必须指定项目");
      return;
    }

    setSaving(true);
    try {
      const response = await consoleFetch("/api/gateway/admin/v1/pricing/price-books", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          price_book_key: buildPriceBookKey(purpose, providerId),
          display_name: displayName.trim(),
          purpose,
          scope_type: scope,
          organization_id: scope === "platform" ? null : organizationId.trim(),
          project_id: scope === "project" ? projectId.trim() : null,
          provider_id: providerId.trim() || null,
          currency: currency.toUpperCase(),
        }),
      });
      if (!response.ok) throw new Error(await responseMessage(response));
      const book = (await response.json()) as PriceBook;
      toast.success("价格簿已创建，请添加第一个模型价格");
      onOpenChange(false);
      reset();
      onCreated(book);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "创建价格簿失败");
    } finally {
      setSaving(false);
    }
  }

  function reset() {
    setDisplayName("");
    setPurpose("customer_sale");
    setScope("platform");
    setOrganizationId("");
    setProjectId("");
    setProviderId("");
    setCurrency("USD");
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>新建价格簿</DialogTitle>
          <DialogDescription>
            价格簿定义币种、作用域和用途；模型与具体单价在下一步配置。
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2 sm:grid-cols-2">
          <Field label="名称" className="sm:col-span-2">
            <Input value={displayName} placeholder="例如 平台标准客户售价" onChange={(event) => setDisplayName(event.target.value)} />
          </Field>
          <Field label="用途">
            <Select value={purpose} onValueChange={(value: PriceBookPurpose) => setPurpose(value)}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="customer_sale">客户销售价</SelectItem>
                <SelectItem value="provider_actual">供应商实际成本</SelectItem>
                <SelectItem value="provider_estimated">供应商预估成本</SelectItem>
                <SelectItem value="provider_allocated">订阅/积分分摊</SelectItem>
                <SelectItem value="provider_benchmark">市场基准价</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="币种">
            <Input value={currency} maxLength={3} onChange={(event) => setCurrency(event.target.value.toUpperCase())} />
          </Field>
          <Field label="作用域">
            <Select value={scope} onValueChange={(value: PriceBook["scope_type"]) => setScope(value)}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="platform">全平台</SelectItem>
                <SelectItem value="organization">指定组织</SelectItem>
                <SelectItem value="project">指定项目</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="供应商">
            <Input value={providerId} placeholder="客户统一售价可留空" onChange={(event) => setProviderId(event.target.value)} />
          </Field>
          {scope !== "platform" ? (
            <Field label="组织 ID" className={scope === "organization" ? "sm:col-span-2" : undefined}>
              <Input value={organizationId} onChange={(event) => setOrganizationId(event.target.value)} />
            </Field>
          ) : null}
          {scope === "project" ? (
            <Field label="项目 ID">
              <Input value={projectId} onChange={(event) => setProjectId(event.target.value)} />
            </Field>
          ) : null}
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>取消</Button>
          <Button type="button" onClick={create} disabled={saving}>
            {saving ? "创建中" : "创建并配置模型"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function VersionTransitionDialog({
  transition,
  onOpenChange,
  onCompleted,
}: {
  transition: Transition | null;
  onOpenChange: (open: boolean) => void;
  onCompleted: () => void;
}) {
  const [saving, setSaving] = useState(false);
  const [readiness, setReadiness] = useState<PricePublishReadiness | null>(null);
  const [readinessLoading, setReadinessLoading] = useState(false);
  const publish = transition?.action === "publish";
  const scheduled = transition ? versionLifecycle(transition.version, Date.now()) === "scheduled" : false;

  useEffect(() => {
    if (!transition || transition.action !== "publish") {
      setReadiness(null);
      setReadinessLoading(false);
      return;
    }
    const controller = new AbortController();
    setReadiness(null);
    setReadinessLoading(true);
    void consoleFetch(
      `/api/gateway/admin/v1/pricing/price-book-versions/${transition.version.price_book_version_id}/publish-readiness`,
      { signal: controller.signal },
    )
      .then(async (response) => {
        if (!response.ok) throw new Error(await responseMessage(response));
        setReadiness((await response.json()) as PricePublishReadiness);
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          toast.error(error instanceof Error ? error.message : "发布预检失败");
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setReadinessLoading(false);
      });
    return () => controller.abort();
  }, [transition]);

  async function confirm() {
    if (!transition) return;
    if (publish && !readiness?.ready) {
      toast.error("请先修复发布预检中的阻断项");
      return;
    }
    setSaving(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/pricing/price-book-versions/${transition.version.price_book_version_id}/${transition.action}`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            expected_control_version: Number(transition.version.control_version),
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success(publish ? "价格版本已发布" : scheduled ? "生效计划已取消" : "价格版本已退役");
      onCompleted();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "价格版本状态更新失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <AlertDialog open={Boolean(transition)} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>
            {publish ? "发布这个价格版本？" : scheduled ? "取消这个生效计划？" : "退役这个价格版本？"}
          </AlertDialogTitle>
          <AlertDialogDescription>
            {publish
              ? `发布后版本不可修改；系统将在 ${formatDateTime(transition.version.effective_from_ms)} 原子切换价格，并保留历史账单使用的旧版本。`
              : scheduled
                ? "取消后这个未来版本不会参与价格解析，当前生效版本不受影响。"
                : "退役不会改写历史账单；新请求将不再选择这个版本。"}
          </AlertDialogDescription>
          {publish ? (
            <PublishReadinessSummary
              loading={readinessLoading}
              readiness={readiness}
            />
          ) : null}
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={saving}>取消</AlertDialogCancel>
          <AlertDialogAction
            disabled={saving || (publish && (readinessLoading || !readiness?.ready))}
            onClick={(event) => {
            event.preventDefault();
            void confirm();
            }}
          >
            {saving ? "处理中" : publish ? "确认发布" : "确认退役"}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}

function PublishReadinessSummary({
  loading,
  readiness,
}: {
  loading: boolean;
  readiness: PricePublishReadiness | null;
}) {
  if (loading) {
    return (
      <div className="rounded-md border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
        正在核对模型路由、计量终态和请求参数覆盖…
      </div>
    );
  }
  if (!readiness) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm text-destructive">
        无法取得发布预检结果。
      </div>
    );
  }
  return (
    <div
      className={readiness.ready
        ? "rounded-md border bg-muted/30 px-4 py-3 text-sm"
        : "rounded-md border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm"}
    >
      <div className="flex items-center gap-2 font-medium">
        {readiness.ready
          ? <CheckCircle2 className="size-4" aria-hidden="true" />
          : <AlertCircle className="size-4 text-destructive" aria-hidden="true" />}
        {readiness.ready ? "发布预检通过" : "发布预检未通过"}
      </div>
      <p className="mt-1 text-xs text-muted-foreground">
        匹配 {readiness.matching_surface_count} 个平台模型入口
        {readiness.request_dimensions.length > 0
          ? ` · 请求维度 ${readiness.request_dimensions.join("、")}`
          : ""}
      </p>
      {readiness.blocking_reasons.length > 0 ? (
        <ul className="mt-2 space-y-1 text-xs text-destructive">
          {readiness.blocking_reasons.map((reason) => (
            <li key={reason}>{publishReadinessLabel(reason)}</li>
          ))}
        </ul>
      ) : null}
      {readiness.warnings.length > 0 ? (
        <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
          {readiness.warnings.map((warning) => (
            <li key={warning}>{publishReadinessLabel(warning)}</li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

function DetailSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h3 className="text-sm font-semibold">{title}</h3>
      <dl className="mt-4 grid gap-x-8 gap-y-4 sm:grid-cols-2">{children}</dl>
    </section>
  );
}

function Definition({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className={`mt-1 truncate text-sm ${mono ? "font-mono" : ""}`}>{value}</dd>
    </div>
  );
}

function Field({
  label,
  className,
  children,
}: {
  label: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={className}>
      <Label className="mb-2 block">{label}</Label>
      {children}
    </div>
  );
}

function SummaryValue({ value, label }: { value: number; label: string }) {
  return (
    <span>
      <strong className="font-semibold tabular-nums">{value.toLocaleString("zh-CN")}</strong>
      <span className="ml-1.5 text-muted-foreground">{label}</span>
    </span>
  );
}

function PriceSummary({ version, currency }: { version: PriceBookVersion; currency: string }) {
  if (version.billing_mode === "provider_reported") {
    return <span className="text-sm text-muted-foreground">采用供应商回执金额</span>;
  }
  if (version.components.length === 0) {
    return <span className="text-sm text-muted-foreground">未配置计量项</span>;
  }
  const components = [...version.components].sort(
    (left, right) => outcomePriority(left.outcome) - outcomePriority(right.outcome),
  );
  return (
    <div className="space-y-1">
      {components.slice(0, 2).map((component) => (
        <p key={component.price_component_id} className="whitespace-nowrap text-sm">
          <span className="font-medium">{formatMoneyMicros(component.unit_price_micros, currency)}</span>
          <span className="text-muted-foreground">
            {" "}/ {formatInteger(component.unit_size)} {unitLabel(component.unit)}
            {" · "}{outcomeLabel(component.outcome)}
          </span>
        </p>
      ))}
      {version.components.length > 2 ? (
        <p className="text-xs text-muted-foreground">另有 {version.components.length - 2} 个计量项</p>
      ) : null}
    </div>
  );
}

function outcomePriority(outcome: string) {
  return ({
    succeeded: 0,
    any: 1,
    failed: 2,
    no_effect: 3,
  } as Record<string, number>)[outcome] ?? 4;
}

function SourceSummary({ version }: { version: PriceBookVersion }) {
  return (
    <div className="max-w-44">
      <span className="block text-sm">{sourceKindLabel(version.source_kind)}</span>
      <span className="mt-0.5 block truncate text-xs text-muted-foreground">
        {version.source_checked_at_ms ? `核验 ${formatDateTime(version.source_checked_at_ms)}` : "未记录核验时间"}
      </span>
    </div>
  );
}

function VersionStateBadge({
  version,
  asOfMs,
}: {
  version: PriceBookVersion | null;
  asOfMs: number;
}) {
  const state = versionLifecycle(version, asOfMs);
  const labels = {
    current: "当前生效",
    scheduled: "等待生效",
    ended: "已结束",
    draft: "草稿",
    retired: "已退役",
    unconfigured: "未配置",
  };
  return (
    <Badge variant={state === "current" ? "default" : "outline"} className="font-normal">
      {labels[state]}
    </Badge>
  );
}

function versionLifecycle(
  version: PriceBookVersion | null,
  asOfMs: number,
): VersionLifecycle {
  if (!version) return "unconfigured";
  if (version.state === "draft" || version.state === "retired") return version.state;
  if (version.effective_from_ms > asOfMs) return "scheduled";
  if (version.effective_until_ms !== null && asOfMs >= version.effective_until_ms) return "ended";
  return "current";
}

function EmptyCatalog({ hasBooks, onCreate }: { hasBooks: boolean; onCreate: () => void }) {
  return (
    <div className="flex min-h-72 flex-col items-center justify-center px-6 text-center">
      <FileClock className="size-8 text-muted-foreground" aria-hidden="true" />
      <h3 className="mt-4 font-medium">{hasBooks ? "没有符合条件的价格" : "还没有价格簿"}</h3>
      <p className="mt-1 max-w-md text-sm text-muted-foreground">
        {hasBooks
          ? "调整价格类型、供应商、状态或搜索条件后重试。"
          : "先建立一个用途与币种明确的价格簿，再为模型添加版本化单价。"}
      </p>
      {!hasBooks ? (
        <Button type="button" className="mt-5" onClick={onCreate}>
          <Plus aria-hidden="true" />
          新建价格簿
        </Button>
      ) : null}
    </div>
  );
}

function flattenCatalog(books: PriceBook[]): CatalogRow[] {
  return books.flatMap<CatalogRow>((book) =>
    book.versions.length > 0
      ? book.versions.map((version) => ({ book, version }))
      : [{ book, version: null }]
  );
}

function filterRows(
  rows: CatalogRow[],
  filters: {
    view: PricingView;
    search: string;
    provider: string;
    state: string;
    asOfMs?: number;
  },
) {
  const query = filters.search.trim().toLowerCase();
  return rows.filter(({ book, version }) => {
    if (filters.view === "customer" && book.purpose !== "customer_sale") return false;
    if (
      filters.view === "provider"
      && !["provider_actual", "provider_estimated", "provider_allocated"].includes(book.purpose)
    ) return false;
    if (filters.view === "benchmark" && book.purpose !== "provider_benchmark") return false;
    const rowProvider = version?.provider_id ?? book.provider_id;
    if (filters.provider !== "all" && rowProvider !== filters.provider) return false;
    if (
      filters.state !== "all"
      && versionLifecycle(version, filters.asOfMs ?? Date.now()) !== filters.state
    ) return false;
    if (!query) return true;
    return [
      book.display_name,
      book.price_book_key,
      book.provider_id,
      version?.public_model_id,
      version?.provider_model_id,
      version?.api_profile,
    ].some((value) => value?.toLowerCase().includes(query));
  });
}

function purposeLabel(purpose: PriceBookPurpose) {
  const labels: Record<PriceBookPurpose, string> = {
    customer_sale: "客户销售价",
    provider_actual: "供应商实际成本",
    provider_estimated: "供应商预估成本",
    provider_allocated: "订阅/积分分摊",
    provider_benchmark: "市场基准价",
  };
  return labels[purpose];
}

function providerLabel(provider: string | null | undefined) {
  if (!provider) return "统一价格";
  const labels: Record<string, string> = {
    codex: "Codex",
    openai: "OpenAI",
    grok: "Grok",
    xai: "xAI",
    dreamina: "即梦",
    ark: "火山方舟",
  };
  return labels[provider.toLowerCase()] ?? provider;
}

function billingModeLabel(mode: PriceBookVersion["billing_mode"]) {
  const labels: Record<PriceBookVersion["billing_mode"], string> = {
    customer_rate: "客户销售价",
    provider_reported: "供应商回执金额",
    published_rate: "官方公开价",
    contract_rate: "合同价",
    subscription_allocation: "订阅成本分摊",
    membership_points: "会员积分",
  };
  return labels[mode];
}

function sourceKindLabel(kind: PriceBookVersion["source_kind"]) {
  const labels: Record<PriceBookVersion["source_kind"], string> = {
    manual: "平台人工配置",
    official_document: "官方文档",
    provider_contract: "供应商合同",
    imported: "批量导入",
  };
  return labels[kind];
}

function executionSurfaceLabel(surface: PriceBookVersion["execution_surface"]) {
  const labels: Record<PriceBookVersion["execution_surface"], string> = {
    provider_api: "供应商 API",
    provider_cli: "供应商 CLI",
    manual_import: "人工导入",
  };
  return labels[surface];
}

function metricLabel(metric: string) {
  const labels: Record<string, string> = {
    request: "请求",
    image_input: "输入图片",
    image_output: "输出图片",
    text_input_token: "文本输入 token",
    cached_text_input_token: "缓存文本输入 token",
    image_input_token: "图片输入 token",
    cached_image_input_token: "缓存图片输入 token",
    image_output_token: "图片输出 token",
    video_input_token: "视频输入 token",
    video_output_token: "视频输出 token",
    video_input_second: "输入视频秒数",
    video_requested_second: "请求视频秒数",
    video_output_second: "实际输出视频秒数",
    membership_point: "会员积分",
  };
  return labels[metric] ?? metric;
}

function outcomeLabel(outcome: string) {
  const labels: Record<string, string> = {
    succeeded: "成功",
    failed: "失败",
    no_effect: "无产出",
    any: "全部",
  };
  return labels[outcome] ?? outcome;
}

function quantitySourceLabel(source: string) {
  const labels: Record<string, string> = {
    provider_reported: "供应商回执",
    request_derived: "请求推导",
    media_inspected: "媒体实测",
    official_lookup: "官方查表",
    operator_adjustment: "人工调整",
  };
  return labels[source] ?? source;
}

function unitLabel(unit: string) {
  const labels: Record<string, string> = {
    request: "次请求",
    image: "张",
    token: "token",
    second: "秒",
    point: "积分",
  };
  return labels[unit] ?? unit;
}

function publishReadinessLabel(reason: string) {
  const labels: Record<string, string> = {
    version_not_draft: "只有草稿版本可以发布",
    source_evidence_missing: "官方文档或合同价格缺少来源地址和核验时间",
    billing_mode_mismatch: "计费模式与价格簿用途不一致",
    execution_surface_mismatch: "客户售价必须用于当前 CLI 执行路径",
    provider_reported_components_present: "供应商回执金额模式不能再配置静态单价",
    price_components_missing: "尚未配置计量项",
    component_price_invalid: "计量项金额格式无效",
    free_price_has_nonzero_component: "免费版本仍包含非零单价",
    paid_price_has_no_positive_success_rate: "付费版本缺少大于零的成功单价",
    customer_currency_not_usd: "当前生产请求使用 USD，客户售价币种必须为 USD",
    platform_surface_missing: "没有匹配的平台模型路由",
    metering_contract_incompatible: "计量指标、数量来源或终态与执行事实不兼容",
    component_dimensions_invalid: "计价维度必须是对象",
    component_dimension_unsupported: "计价规则包含该模型请求不会产生的维度",
    component_selector_ambiguous: "存在相同优先级的重复计价选择器",
    request_dimension_fallback_missing: "参数化价格缺少覆盖全部请求值的基础规则",
    active_price_resolution_conflict: "另一价格簿存在同优先级生效规则，会造成价格解析冲突",
    maker_checker_required: "官方价格草稿必须由另一位平台负责人复核并发布",
    metering_uses_official_estimate: "该模型按官方换算表估算计量数量",
    all_outcomes_share_rate: "成功、失败和无产出共用同一价格",
    dimension_overrides_present: "包含按请求参数细分的覆盖价格",
  };
  return labels[reason] ?? reason;
}

function buildPriceBookKey(purpose: PriceBookPurpose, providerId: string) {
  const provider = providerId.trim().toLowerCase().replace(/[^a-z0-9_.-]/g, "") || "all";
  return `${purpose}.${provider}.${Date.now().toString(36)}`;
}

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as { error?: string | { message?: string } };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Fall through to a stable UI message.
  }
  return `请求失败 (${response.status})`;
}
