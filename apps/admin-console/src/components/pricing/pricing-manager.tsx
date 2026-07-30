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
import { useI18n } from "@/i18n/locale-provider";
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
type Translate = ReturnType<typeof useI18n>["t"];

export function PricingManager() {
  const { locale, t } = useI18n();
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
      toast.error(t({
        en: "This model endpoint does not yet have a complete pricing contract",
        "zh-CN": "这个模型入口尚未形成完整的定价契约",
        ja: "このモデルエンドポイントには完全な価格契約がまだありません",
        ko: "이 모델 엔드포인트에는 아직 완전한 가격 계약이 없습니다",
      }));
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
            display_name: t({
              en: "{provider} standard customer pricing",
              "zh-CN": "{provider} 标准客户售价",
              ja: "{provider} 標準顧客価格",
              ko: "{provider} 표준 고객 가격",
            }, { provider: surface.provider_display_name }),
            purpose: "customer_sale",
            scope_type: "platform",
            organization_id: null,
            project_id: null,
            provider_id: surface.provider_id,
            currency: "USD",
          }),
        });
        if (!response.ok) {
          toast.error(await responseMessage(response, t({
            en: "Failed to create a price book",
            "zh-CN": "创建价格簿失败",
            ja: "価格表を作成できませんでした",
            ko: "가격표를 만들지 못했습니다",
          })));
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
      toast.error(t({
        en: "Pricing configuration is temporarily unavailable. Try again later.",
        "zh-CN": "定价配置暂时不可用，请稍后重试",
        ja: "価格設定は一時的に利用できません。後でもう一度お試しください。",
        ko: "가격 설정을 일시적으로 사용할 수 없습니다. 나중에 다시 시도하세요.",
      }));
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
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to create rollback draft",
          "zh-CN": "创建回滚草稿失败",
          ja: "ロールバック下書きを作成できませんでした",
          ko: "롤백 초안을 만들지 못했습니다",
        })));
      }
      const result = (await response.json()) as PriceRollbackDraftResult;
      toast.success(t({
        en: "Rollback draft created from v{version}",
        "zh-CN": "已从 v{version} 创建回滚草稿",
        ja: "v{version} からロールバック下書きを作成しました",
        ko: "v{version}에서 롤백 초안을 만들었습니다",
      }, { version: version.version }));
      query.retry();
      setSelected({
        bookId: result.draft.price_book_id,
        versionId: result.draft.price_book_version_id,
      });
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t({
        en: "Failed to create rollback draft",
        "zh-CN": "创建回滚草稿失败",
        ja: "ロールバック下書きを作成できませんでした",
        ko: "롤백 초안을 만들지 못했습니다",
      }));
    }
  }

  return (
    <div className="min-w-0 space-y-6 overflow-x-clip">
      <PageHeader
        title={t({ en: "Model pricing", "zh-CN": "模型定价", ja: "モデル価格", ko: "모델 가격" })}
        description={section === "rules"
          ? t({
            en: "Manage customer prices, provider costs, and benchmark prices. Published versions remain immutable for usage replay and billing audits.",
            "zh-CN": "管理对外销售价、供应商成本和基准价格；已发布版本保持不可变，以便用量重放与账单审计。",
            ja: "顧客価格、プロバイダーコスト、ベンチマーク価格を管理します。公開済みバージョンは、使用量の再現と請求監査のため変更されません。",
            ko: "고객 판매가, 제공업체 비용 및 기준 가격을 관리합니다. 게시된 버전은 사용량 재현과 청구 감사를 위해 변경되지 않습니다.",
          })
          : t({
            en: "Verify routing, customer pricing, metering rules, and upstream cost coverage for every API model endpoint.",
            "zh-CN": "核对每个 API 模型入口的路由、客户售价、计量规则与上游成本覆盖。",
            ja: "各 API モデルエンドポイントのルーティング、顧客価格、計量ルール、上流コストのカバレッジを確認します。",
            ko: "각 API 모델 엔드포인트의 라우팅, 고객 가격, 계량 규칙 및 상위 비용 적용 범위를 확인합니다.",
          })}
        actions={
          <>
            <Button type="button" variant="outline" size="sm" onClick={query.retry} disabled={query.refreshing}>
              <RefreshCw className={query.refreshing ? "animate-spin" : ""} aria-hidden="true" />
              {t({ en: "Refresh", "zh-CN": "刷新", ja: "更新", ko: "새로고침" })}
            </Button>
            <Button type="button" variant="outline" size="sm" onClick={() => setOfficialImportOpen(true)}>
              <Download aria-hidden="true" />
              {t({ en: "Sync official pricing", "zh-CN": "同步官方价格", ja: "公式価格を同期", ko: "공식 가격 동기화" })}
            </Button>
            <Button type="button" size="sm" onClick={() => setCreateOpen(true)}>
              <Plus aria-hidden="true" />
              {t({ en: "New price book", "zh-CN": "新建价格簿", ja: "価格表を作成", ko: "새 가격표" })}
            </Button>
          </>
        }
      />

      <Tabs value={section} onValueChange={(value) => setSection(value as PricingSection)}>
        <TabsList className="h-9">
          <TabsTrigger value="rules">{t({ en: "Pricing rules", "zh-CN": "价格规则", ja: "価格ルール", ko: "가격 규칙" })}</TabsTrigger>
          <TabsTrigger value="coverage">{t({ en: "Coverage", "zh-CN": "覆盖检查", ja: "カバレッジ", ko: "적용 범위" })}</TabsTrigger>
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
            <SummaryValue value={activeCount} label={t({ en: "active model prices", "zh-CN": "生效模型价格", ja: "有効なモデル価格", ko: "활성 모델 가격" })} locale={locale} />
            <SummaryValue value={scheduledCount} label={t({ en: "scheduled", "zh-CN": "等待生效", ja: "適用待ち", ko: "적용 예정" })} locale={locale} />
            <SummaryValue value={draftCount} label={t({ en: "drafts awaiting publication", "zh-CN": "待发布草稿", ja: "公開待ちの下書き", ko: "게시 대기 초안" })} locale={locale} />
            <SummaryValue value={sourcedCount} label={t({ en: "verifiable official or contract sources", "zh-CN": "可复核官方/合同来源", ja: "検証可能な公式・契約ソース", ko: "검증 가능한 공식/계약 출처" })} locale={locale} />
            <span className="ml-auto text-xs text-muted-foreground">
              {t({
                en: "Data as of {time}",
                "zh-CN": "数据截至 {time}",
                ja: "データ時点 {time}",
                ko: "데이터 기준 {time}",
              }, { time: formatDateTime(query.data.as_of_ms) })}
            </span>
          </div>

          <Tabs value={view} onValueChange={(value) => setView(value as PricingView)}>
            <TabsList className="h-9">
              <TabsTrigger value="customer">{t({ en: "Customer pricing", "zh-CN": "客户售价", ja: "顧客価格", ko: "고객 가격" })}</TabsTrigger>
              <TabsTrigger value="provider">{t({ en: "Provider costs", "zh-CN": "供应成本", ja: "プロバイダーコスト", ko: "제공업체 비용" })}</TabsTrigger>
              <TabsTrigger value="benchmark">{t({ en: "Market benchmarks", "zh-CN": "市场基准", ja: "市場ベンチマーク", ko: "시장 기준" })}</TabsTrigger>
              <TabsTrigger value="all">{t({ en: "All", "zh-CN": "全部", ja: "すべて", ko: "전체" })}</TabsTrigger>
            </TabsList>
          </Tabs>

          <div className="flex flex-col gap-3 lg:flex-row">
            <div className="relative min-w-0 flex-1">
              <Search className="pointer-events-none absolute left-3 top-2.5 size-4 text-muted-foreground" aria-hidden="true" />
              <Input
                className="pl-9"
                value={search}
                placeholder={t({
                  en: "Search public models, provider models, or price books",
                  "zh-CN": "搜索外部模型、原生模型或价格簿",
                  ja: "公開モデル、プロバイダーモデル、価格表を検索",
                  ko: "외부 모델, 제공업체 모델 또는 가격표 검색",
                })}
                onChange={(event) => setSearch(event.target.value)}
              />
            </div>
            <Select value={provider} onValueChange={setProvider}>
              <SelectTrigger className="w-full lg:w-48"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t({ en: "All providers", "zh-CN": "全部供应商", ja: "すべてのプロバイダー", ko: "모든 제공업체" })}</SelectItem>
                {providers.map((item) => <SelectItem key={item} value={item}>{providerLabel(item, t)}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={state} onValueChange={setState}>
              <SelectTrigger className="w-full lg:w-40"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t({ en: "All statuses", "zh-CN": "全部状态", ja: "すべての状態", ko: "모든 상태" })}</SelectItem>
                <SelectItem value="current">{versionLifecycleLabel("current", t)}</SelectItem>
                <SelectItem value="scheduled">{versionLifecycleLabel("scheduled", t)}</SelectItem>
                <SelectItem value="draft">{versionLifecycleLabel("draft", t)}</SelectItem>
                <SelectItem value="ended">{versionLifecycleLabel("ended", t)}</SelectItem>
                <SelectItem value="retired">{versionLifecycleLabel("retired", t)}</SelectItem>
                <SelectItem value="unconfigured">{versionLifecycleLabel("unconfigured", t)}</SelectItem>
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
                      <TableHead className="pl-4">{t({ en: "Model", "zh-CN": "模型", ja: "モデル", ko: "모델" })}</TableHead>
                      <TableHead>{t({ en: "Price type", "zh-CN": "价格类型", ja: "価格種別", ko: "가격 유형" })}</TableHead>
                      <TableHead>{t({ en: "Provider", "zh-CN": "供应商", ja: "プロバイダー", ko: "제공업체" })}</TableHead>
                      <TableHead>{t({ en: "Metering and price", "zh-CN": "计量与单价", ja: "計量と単価", ko: "계량 및 단가" })}</TableHead>
                      <TableHead>{t({ en: "Status", "zh-CN": "状态", ja: "状態", ko: "상태" })}</TableHead>
                      <TableHead>{t({ en: "Effective from", "zh-CN": "生效时间", ja: "適用開始", ko: "적용 시작" })}</TableHead>
                      <TableHead>{t({ en: "Source", "zh-CN": "来源", ja: "ソース", ko: "출처" })}</TableHead>
                      <TableHead className="w-16 pr-4 text-right">{t({ en: "Details", "zh-CN": "详情", ja: "詳細", ko: "세부정보" })}</TableHead>
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
                                ? `${version.api_profile} · ${version.media_kind === "video"
                                  ? t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })
                                  : t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })}`
                                : t({
                                  en: "No model pricing added",
                                  "zh-CN": "尚未添加模型价格",
                                  ja: "モデル価格が未登録",
                                  ko: "모델 가격이 추가되지 않음",
                                })}
                            </span>
                          </button>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">{purposeLabel(book.purpose, t)}</span>
                          <span className="mt-0.5 block text-xs text-muted-foreground">{book.currency}</span>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">{providerLabel(version?.provider_id ?? book.provider_id, t)}</span>
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
                              {t({ en: "Add model pricing", "zh-CN": "添加模型价格", ja: "モデル価格を追加", ko: "모델 가격 추가" })}
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
                            aria-label={t({ en: "View pricing details", "zh-CN": "查看价格详情", ja: "価格の詳細を表示", ko: "가격 세부정보 보기" })}
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
  const { t } = useI18n();
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
            {book.display_name} · {purposeLabel(book.purpose, t)} · {book.currency}
          </SheetDescription>
        </SheetHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
          {version ? (
            <div className="space-y-8">
              <DetailSection title={t({ en: "Model and routing", "zh-CN": "模型与路由", ja: "モデルとルーティング", ko: "모델 및 라우팅" })}>
                <Definition label={t({ en: "Public model ID", "zh-CN": "外部模型 ID", ja: "公開モデル ID", ko: "외부 모델 ID" })} value={version.public_model_id} mono />
                <Definition label={t({ en: "Provider model ID", "zh-CN": "原生模型 ID", ja: "プロバイダーモデル ID", ko: "제공업체 모델 ID" })} value={version.provider_model_id ?? "--"} mono />
                <Definition label={t({ en: "API protocol", "zh-CN": "API 协议", ja: "API プロトコル", ko: "API 프로토콜" })} value={version.api_profile} mono />
                <Definition label={t({ en: "Operation", "zh-CN": "操作", ja: "操作", ko: "작업" })} value={version.operation} mono />
                <Definition label={t({ en: "Provider", "zh-CN": "供应商", ja: "プロバイダー", ko: "제공업체" })} value={providerLabel(version.provider_id ?? book.provider_id, t)} />
                <Definition label={t({ en: "Execution channel", "zh-CN": "执行渠道", ja: "実行チャネル", ko: "실행 채널" })} value={executionSurfaceLabel(version.execution_surface, t)} />
              </DetailSection>

              <section>
                <h3 className="text-sm font-semibold">
                  {t({ en: "Metering and pricing", "zh-CN": "计量与价格", ja: "計量と価格", ko: "계량 및 가격" })}
                </h3>
                <p className="mt-1 text-sm text-muted-foreground">
                  {billingModeLabel(version.billing_mode, t)}
                  {version.is_free
                    ? ` · ${t({ en: "Free version", "zh-CN": "免费版本", ja: "無料バージョン", ko: "무료 버전" })}`
                    : ""}
                </p>
                <div className="mt-4 overflow-hidden rounded-md border">
                  {version.billing_mode === "provider_reported" ? (
                    <div className="px-4 py-6 text-sm text-muted-foreground">
                      {t({
                        en: "The amount comes from the provider's terminal response; static pricing components are not used.",
                        "zh-CN": "金额由供应商终态回执提供，不使用静态价格组件。",
                        ja: "金額はプロバイダーの終端応答から取得され、固定価格コンポーネントは使用しません。",
                        ko: "금액은 제공업체의 최종 응답에서 가져오며 정적 가격 구성 요소를 사용하지 않습니다.",
                      })}
                    </div>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="pl-4">{t({ en: "Metric", "zh-CN": "指标", ja: "指標", ko: "지표" })}</TableHead>
                          <TableHead>{t({ en: "Applicable outcome", "zh-CN": "适用结果", ja: "適用結果", ko: "적용 결과" })}</TableHead>
                          <TableHead>{t({ en: "Quantity source", "zh-CN": "数量来源", ja: "数量ソース", ko: "수량 출처" })}</TableHead>
                          <TableHead className="pr-4 text-right">{t({ en: "Unit price", "zh-CN": "单价", ja: "単価", ko: "단가" })}</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {version.components.map((component) => (
                          <TableRow key={component.price_component_id}>
                            <TableCell className="pl-4">
                              <span className="font-medium">{metricLabel(component.metric, t)}</span>
                              <span className="mt-0.5 block font-mono text-xs text-muted-foreground">
                                {component.component_key}
                              </span>
                            </TableCell>
                            <TableCell>{outcomeLabel(component.outcome, t)}</TableCell>
                            <TableCell>{quantitySourceLabel(component.quantity_source, t)}</TableCell>
                            <TableCell className="pr-4 text-right font-mono tabular-nums">
                              {formatMoneyMicros(component.unit_price_micros, book.currency)}
                              <span className="block text-xs text-muted-foreground">
                                / {formatInteger(component.unit_size)} {unitLabel(component.unit, t)}
                              </span>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </div>
              </section>

              <DetailSection title={t({ en: "Version and source", "zh-CN": "版本与来源", ja: "バージョンとソース", ko: "버전 및 출처" })}>
                <Definition label={t({ en: "Version", "zh-CN": "版本", ja: "バージョン", ko: "버전" })} value={`v${version.version}`} />
                <Definition label={t({ en: "Effective from", "zh-CN": "生效时间", ja: "適用開始", ko: "적용 시작" })} value={formatDateTime(version.effective_from_ms)} />
                <Definition
                  label={t({ en: "Effective until", "zh-CN": "结束时间", ja: "適用終了", ko: "적용 종료" })}
                  value={version.effective_until_ms
                    ? formatDateTime(version.effective_until_ms)
                    : t({ en: "No end date", "zh-CN": "持续生效", ja: "期限なし", ko: "종료일 없음" })}
                />
                <Definition label={t({ en: "Source type", "zh-CN": "来源类型", ja: "ソース種別", ko: "출처 유형" })} value={sourceKindLabel(version.source_kind, t)} />
                <Definition label={t({ en: "Verified at", "zh-CN": "核验时间", ja: "検証日時", ko: "검증 시간" })} value={formatDateTime(version.source_checked_at_ms)} />
                <Definition label={t({ en: "Service tier", "zh-CN": "服务层级", ja: "サービス階層", ko: "서비스 등급" })} value={version.service_tier} mono />
                <Definition label={t({ en: "Control version", "zh-CN": "控制版本", ja: "制御バージョン", ko: "제어 버전" })} value={version.control_version} mono />
              </DetailSection>

              {version.source_url ? (
                <Button type="button" variant="outline" size="sm" asChild>
                  <a href={version.source_url} target="_blank" rel="noreferrer">
                    <ExternalLink aria-hidden="true" />
                    {t({ en: "View pricing source", "zh-CN": "查看定价来源", ja: "価格ソースを表示", ko: "가격 출처 보기" })}
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
              <h3 className="mt-4 font-medium">
                {t({ en: "This price book has no model pricing yet", "zh-CN": "这个价格簿还没有模型价格", ja: "この価格表にはモデル価格がまだありません", ko: "이 가격표에는 아직 모델 가격이 없습니다" })}
              </h3>
              <p className="mt-1 max-w-sm text-sm text-muted-foreground">
                {t({
                  en: "Add a model, API protocol, metering units, unit prices, and a source before publishing a billable version.",
                  "zh-CN": "添加模型、API 协议、计量单位、单价与来源后，再发布为可结算版本。",
                  ja: "モデル、API プロトコル、計量単位、単価、ソースを追加してから、請求可能なバージョンを公開してください。",
                  ko: "모델, API 프로토콜, 계량 단위, 단가 및 출처를 추가한 후 청구 가능한 버전으로 게시하세요.",
                })}
              </p>
              <Button type="button" className="mt-5" onClick={() => onAddVersion(book)}>
                <Plus aria-hidden="true" />
                {t({ en: "Add model pricing", "zh-CN": "添加模型价格", ja: "モデル価格を追加", ko: "모델 가격 추가" })}
              </Button>
            </div>
          )}
        </div>

        {version ? (
          <div className="flex flex-wrap justify-end gap-2 border-t bg-background px-6 py-4">
            {version.state === "draft" ? (
              <>
                <Button type="button" variant="outline" onClick={() => onEditVersion(book, version)}>
                  {t({ en: "Edit draft", "zh-CN": "编辑草稿", ja: "下書きを編集", ko: "초안 편집" })}
                </Button>
                <Button type="button" onClick={() => onTransition({ version, action: "publish" })}>
                  {t({ en: "Publish pricing", "zh-CN": "发布价格", ja: "価格を公開", ko: "가격 게시" })}
                </Button>
              </>
            ) : null}
            {version.state === "active" && lifecycle === "scheduled" ? (
              <Button type="button" variant="outline" onClick={() => onTransition({ version, action: "retire" })}>
                {t({ en: "Cancel schedule", "zh-CN": "取消计划", ja: "スケジュールをキャンセル", ko: "일정 취소" })}
              </Button>
            ) : null}
            {lifecycle === "ended" || lifecycle === "retired" ? (
              <Button type="button" variant="outline" onClick={() => onRollback(version)}>
                {t({ en: "Roll back from this version", "zh-CN": "从此版本回滚", ja: "このバージョンからロールバック", ko: "이 버전에서 롤백" })}
              </Button>
            ) : null}
            <Button type="button" variant="outline" onClick={() => onAddVersion(book)}>
              <Plus aria-hidden="true" />
              {t({ en: "New version", "zh-CN": "新建版本", ja: "新しいバージョン", ko: "새 버전" })}
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
  const { t } = useI18n();
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
      toast.error(t({ en: "Enter a price book name", "zh-CN": "请填写价格簿名称", ja: "価格表名を入力してください", ko: "가격표 이름을 입력하세요" }));
      return;
    }
    if (purpose !== "customer_sale" && !providerId.trim()) {
      toast.error(t({
        en: "Provider cost and benchmark price books require a provider",
        "zh-CN": "供应成本与基准价格必须指定供应商",
        ja: "プロバイダーコストとベンチマーク価格にはプロバイダーの指定が必要です",
        ko: "제공업체 비용 및 기준 가격에는 제공업체를 지정해야 합니다",
      }));
      return;
    }
    if (scope !== "platform" && !organizationId.trim()) {
      toast.error(t({
        en: "Organization- and project-scoped price books require an organization",
        "zh-CN": "组织或项目级价格簿必须指定组织",
        ja: "組織またはプロジェクト範囲の価格表には組織の指定が必要です",
        ko: "조직 또는 프로젝트 범위 가격표에는 조직을 지정해야 합니다",
      }));
      return;
    }
    if (scope === "project" && !projectId.trim()) {
      toast.error(t({
        en: "Project-scoped price books require a project",
        "zh-CN": "项目级价格簿必须指定项目",
        ja: "プロジェクト範囲の価格表にはプロジェクトの指定が必要です",
        ko: "프로젝트 범위 가격표에는 프로젝트를 지정해야 합니다",
      }));
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
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to create price book",
          "zh-CN": "创建价格簿失败",
          ja: "価格表を作成できませんでした",
          ko: "가격표를 만들지 못했습니다",
        })));
      }
      const book = (await response.json()) as PriceBook;
      toast.success(t({
        en: "Price book created. Add its first model price.",
        "zh-CN": "价格簿已创建，请添加第一个模型价格",
        ja: "価格表を作成しました。最初のモデル価格を追加してください。",
        ko: "가격표를 만들었습니다. 첫 모델 가격을 추가하세요.",
      }));
      onOpenChange(false);
      reset();
      onCreated(book);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t({
        en: "Failed to create price book",
        "zh-CN": "创建价格簿失败",
        ja: "価格表を作成できませんでした",
        ko: "가격표를 만들지 못했습니다",
      }));
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
          <DialogTitle>{t({ en: "New price book", "zh-CN": "新建价格簿", ja: "価格表を作成", ko: "새 가격표" })}</DialogTitle>
          <DialogDescription>
            {t({
              en: "A price book defines its currency, scope, and purpose. Configure models and unit prices in the next step.",
              "zh-CN": "价格簿定义币种、作用域和用途；模型与具体单价在下一步配置。",
              ja: "価格表では通貨、範囲、用途を定義します。モデルと単価は次の手順で設定します。",
              ko: "가격표는 통화, 범위 및 용도를 정의합니다. 모델과 단가는 다음 단계에서 설정합니다.",
            })}
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2 sm:grid-cols-2">
          <Field label={t({ en: "Name", "zh-CN": "名称", ja: "名前", ko: "이름" })} className="sm:col-span-2">
            <Input
              value={displayName}
              placeholder={t({
                en: "e.g. Platform standard customer pricing",
                "zh-CN": "例如 平台标准客户售价",
                ja: "例: プラットフォーム標準顧客価格",
                ko: "예: 플랫폼 표준 고객 가격",
              })}
              onChange={(event) => setDisplayName(event.target.value)}
            />
          </Field>
          <Field label={t({ en: "Purpose", "zh-CN": "用途", ja: "用途", ko: "용도" })}>
            <Select value={purpose} onValueChange={(value: PriceBookPurpose) => setPurpose(value)}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="customer_sale">{purposeLabel("customer_sale", t)}</SelectItem>
                <SelectItem value="provider_actual">{purposeLabel("provider_actual", t)}</SelectItem>
                <SelectItem value="provider_estimated">{purposeLabel("provider_estimated", t)}</SelectItem>
                <SelectItem value="provider_allocated">{purposeLabel("provider_allocated", t)}</SelectItem>
                <SelectItem value="provider_benchmark">{purposeLabel("provider_benchmark", t)}</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label={t({ en: "Currency", "zh-CN": "币种", ja: "通貨", ko: "통화" })}>
            <Input value={currency} maxLength={3} onChange={(event) => setCurrency(event.target.value.toUpperCase())} />
          </Field>
          <Field label={t({ en: "Scope", "zh-CN": "作用域", ja: "範囲", ko: "범위" })}>
            <Select value={scope} onValueChange={(value: PriceBook["scope_type"]) => setScope(value)}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="platform">{t({ en: "Platform", "zh-CN": "全平台", ja: "プラットフォーム", ko: "플랫폼" })}</SelectItem>
                <SelectItem value="organization">{t({ en: "Specific organization", "zh-CN": "指定组织", ja: "特定の組織", ko: "특정 조직" })}</SelectItem>
                <SelectItem value="project">{t({ en: "Specific project", "zh-CN": "指定项目", ja: "特定のプロジェクト", ko: "특정 프로젝트" })}</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label={t({ en: "Provider", "zh-CN": "供应商", ja: "プロバイダー", ko: "제공업체" })}>
            <Input
              value={providerId}
              placeholder={t({
                en: "Optional for unified customer pricing",
                "zh-CN": "客户统一售价可留空",
                ja: "統一顧客価格の場合は空欄可",
                ko: "통합 고객 가격의 경우 비워 둘 수 있습니다",
              })}
              onChange={(event) => setProviderId(event.target.value)}
            />
          </Field>
          {scope !== "platform" ? (
            <Field label={t({ en: "Organization ID", "zh-CN": "组织 ID", ja: "組織 ID", ko: "조직 ID" })} className={scope === "organization" ? "sm:col-span-2" : undefined}>
              <Input value={organizationId} onChange={(event) => setOrganizationId(event.target.value)} />
            </Field>
          ) : null}
          {scope === "project" ? (
            <Field label={t({ en: "Project ID", "zh-CN": "项目 ID", ja: "プロジェクト ID", ko: "프로젝트 ID" })}>
              <Input value={projectId} onChange={(event) => setProjectId(event.target.value)} />
            </Field>
          ) : null}
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
          </Button>
          <Button type="button" onClick={create} disabled={saving}>
            {saving
              ? t({ en: "Creating", "zh-CN": "创建中", ja: "作成中", ko: "생성 중" })
              : t({ en: "Create and configure models", "zh-CN": "创建并配置模型", ja: "作成してモデルを設定", ko: "생성 후 모델 설정" })}
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
  const { t } = useI18n();
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
        if (!response.ok) {
          throw new Error(await responseMessage(response, t({
            en: "Publish readiness check failed",
            "zh-CN": "发布预检失败",
            ja: "公開前チェックに失敗しました",
            ko: "게시 사전 점검에 실패했습니다",
          })));
        }
        setReadiness((await response.json()) as PricePublishReadiness);
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          toast.error(error instanceof Error ? error.message : t({
            en: "Publish readiness check failed",
            "zh-CN": "发布预检失败",
            ja: "公開前チェックに失敗しました",
            ko: "게시 사전 점검에 실패했습니다",
          }));
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setReadinessLoading(false);
    });
    return () => controller.abort();
  }, [t, transition]);

  async function confirm() {
    if (!transition) return;
    if (publish && !readiness?.ready) {
      toast.error(t({
        en: "Resolve the blockers in the publish readiness check first",
        "zh-CN": "请先修复发布预检中的阻断项",
        ja: "公開前チェックのブロッカーを先に解決してください",
        ko: "게시 사전 점검의 차단 항목을 먼저 해결하세요",
      }));
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
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to update pricing version status",
          "zh-CN": "价格版本状态更新失败",
          ja: "価格バージョンの状態を更新できませんでした",
          ko: "가격 버전 상태를 업데이트하지 못했습니다",
        })));
      }
      toast.success(publish
        ? t({ en: "Pricing version published", "zh-CN": "价格版本已发布", ja: "価格バージョンを公開しました", ko: "가격 버전을 게시했습니다" })
        : scheduled
          ? t({ en: "Effective schedule canceled", "zh-CN": "生效计划已取消", ja: "適用スケジュールをキャンセルしました", ko: "적용 일정을 취소했습니다" })
          : t({ en: "Pricing version retired", "zh-CN": "价格版本已退役", ja: "価格バージョンを廃止しました", ko: "가격 버전을 사용 중지했습니다" }));
      onCompleted();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t({
        en: "Failed to update pricing version status",
        "zh-CN": "价格版本状态更新失败",
        ja: "価格バージョンの状態を更新できませんでした",
        ko: "가격 버전 상태를 업데이트하지 못했습니다",
      }));
    } finally {
      setSaving(false);
    }
  }

  return (
    <AlertDialog open={Boolean(transition)} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>
            {publish
              ? t({ en: "Publish this pricing version?", "zh-CN": "发布这个价格版本？", ja: "この価格バージョンを公開しますか？", ko: "이 가격 버전을 게시할까요?" })
              : scheduled
                ? t({ en: "Cancel this effective schedule?", "zh-CN": "取消这个生效计划？", ja: "この適用スケジュールをキャンセルしますか？", ko: "이 적용 일정을 취소할까요?" })
                : t({ en: "Retire this pricing version?", "zh-CN": "退役这个价格版本？", ja: "この価格バージョンを廃止しますか？", ko: "이 가격 버전을 사용 중지할까요?" })}
          </AlertDialogTitle>
          <AlertDialogDescription>
            {publish
              ? t({
                en: "Published versions cannot be edited. Pricing will switch atomically at {time}, while previous versions remain available for historical billing.",
                "zh-CN": "发布后版本不可修改；系统将在 {time} 原子切换价格，并保留历史账单使用的旧版本。",
                ja: "公開後のバージョンは編集できません。{time} に価格をアトミックに切り替え、過去の請求で使用した旧バージョンは保持されます。",
                ko: "게시된 버전은 수정할 수 없습니다. {time}에 가격이 원자적으로 전환되며 이전 청구에 사용된 버전은 유지됩니다.",
              }, { time: formatDateTime(transition.version.effective_from_ms) })
              : scheduled
                ? t({
                  en: "After cancellation, this future version will not participate in price resolution. The currently active version is unaffected.",
                  "zh-CN": "取消后这个未来版本不会参与价格解析，当前生效版本不受影响。",
                  ja: "キャンセル後、この将来バージョンは価格解決に使用されません。現在有効なバージョンには影響しません。",
                  ko: "취소하면 이 향후 버전은 가격 결정에 사용되지 않으며 현재 활성 버전에는 영향을 주지 않습니다.",
                })
                : t({
                  en: "Retiring does not rewrite historical billing. New requests will no longer select this version.",
                  "zh-CN": "退役不会改写历史账单；新请求将不再选择这个版本。",
                  ja: "廃止しても過去の請求は書き換えられません。新しいリクエストではこのバージョンが選択されなくなります。",
                  ko: "사용 중지해도 이전 청구는 변경되지 않습니다. 새 요청에서는 이 버전이 더 이상 선택되지 않습니다.",
                })}
          </AlertDialogDescription>
          {publish ? (
            <PublishReadinessSummary
              loading={readinessLoading}
              readiness={readiness}
            />
          ) : null}
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={saving}>{t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}</AlertDialogCancel>
          <AlertDialogAction
            disabled={saving || (publish && (readinessLoading || !readiness?.ready))}
            onClick={(event) => {
            event.preventDefault();
            void confirm();
            }}
          >
            {saving
              ? t({ en: "Processing", "zh-CN": "处理中", ja: "処理中", ko: "처리 중" })
              : publish
                ? t({ en: "Publish", "zh-CN": "确认发布", ja: "公開", ko: "게시" })
                : scheduled
                  ? t({ en: "Cancel schedule", "zh-CN": "确认取消", ja: "スケジュールをキャンセル", ko: "일정 취소" })
                  : t({ en: "Retire", "zh-CN": "确认退役", ja: "廃止", ko: "사용 중지" })}
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
  const { t } = useI18n();
  if (loading) {
    return (
      <div className="rounded-md border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
        {t({
          en: "Checking model routes, terminal metering outcomes, and request parameter coverage…",
          "zh-CN": "正在核对模型路由、计量终态和请求参数覆盖…",
          ja: "モデルルート、計量終端状態、リクエストパラメータのカバレッジを確認しています…",
          ko: "모델 경로, 계량 최종 상태 및 요청 매개변수 적용 범위를 확인하는 중…",
        })}
      </div>
    );
  }
  if (!readiness) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm text-destructive">
        {t({
          en: "Publish readiness results are unavailable.",
          "zh-CN": "无法取得发布预检结果。",
          ja: "公開前チェックの結果を取得できません。",
          ko: "게시 사전 점검 결과를 가져올 수 없습니다.",
        })}
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
        {readiness.ready
          ? t({ en: "Publish readiness check passed", "zh-CN": "发布预检通过", ja: "公開前チェックに合格", ko: "게시 사전 점검 통과" })
          : t({ en: "Publish readiness check failed", "zh-CN": "发布预检未通过", ja: "公開前チェックに不合格", ko: "게시 사전 점검 실패" })}
      </div>
      <p className="mt-1 text-xs text-muted-foreground">
        {t({
          en: "Matched {count} platform model endpoints",
          "zh-CN": "匹配 {count} 个平台模型入口",
          ja: "{count}件のプラットフォームモデルエンドポイントに一致",
          ko: "플랫폼 모델 엔드포인트 {count}개 일치",
        }, { count: readiness.matching_surface_count })}
        {readiness.request_dimensions.length > 0
          ? t({
            en: " · Request dimensions: {dimensions}",
            "zh-CN": " · 请求维度 {dimensions}",
            ja: " · リクエストディメンション: {dimensions}",
            ko: " · 요청 차원: {dimensions}",
          }, { dimensions: readiness.request_dimensions.join(", ") })
          : ""}
      </p>
      {readiness.blocking_reasons.length > 0 ? (
        <ul className="mt-2 space-y-1 text-xs text-destructive">
          {readiness.blocking_reasons.map((reason) => (
            <li key={reason}>{publishReadinessLabel(reason, t)}</li>
          ))}
        </ul>
      ) : null}
      {readiness.warnings.length > 0 ? (
        <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
          {readiness.warnings.map((warning) => (
            <li key={warning}>{publishReadinessLabel(warning, t)}</li>
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

function SummaryValue({ value, label, locale }: { value: number; label: string; locale: string }) {
  return (
    <span>
      <strong className="font-semibold tabular-nums">{value.toLocaleString(locale)}</strong>
      <span className="ml-1.5 text-muted-foreground">{label}</span>
    </span>
  );
}

function PriceSummary({ version, currency }: { version: PriceBookVersion; currency: string }) {
  const { t } = useI18n();
  if (version.billing_mode === "provider_reported") {
    return (
      <span className="text-sm text-muted-foreground">
        {t({
          en: "Uses the provider-reported amount",
          "zh-CN": "采用供应商回执金额",
          ja: "プロバイダー報告金額を使用",
          ko: "제공업체 보고 금액 사용",
        })}
      </span>
    );
  }
  if (version.components.length === 0) {
    return (
      <span className="text-sm text-muted-foreground">
        {t({ en: "No metered items configured", "zh-CN": "未配置计量项", ja: "計量項目が未設定", ko: "계량 항목이 설정되지 않음" })}
      </span>
    );
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
            {" "}/ {formatInteger(component.unit_size)} {unitLabel(component.unit, t)}
            {" · "}{outcomeLabel(component.outcome, t)}
          </span>
        </p>
      ))}
      {version.components.length > 2 ? (
        <p className="text-xs text-muted-foreground">
          {t({
            en: "{count} more metered items",
            "zh-CN": "另有 {count} 个计量项",
            ja: "ほかに{count}件の計量項目",
            ko: "계량 항목 {count}개 더 있음",
          }, { count: version.components.length - 2 })}
        </p>
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
  const { t } = useI18n();
  return (
    <div className="max-w-44">
      <span className="block text-sm">{sourceKindLabel(version.source_kind, t)}</span>
      <span className="mt-0.5 block truncate text-xs text-muted-foreground">
        {version.source_checked_at_ms
          ? t({
            en: "Verified {time}",
            "zh-CN": "核验 {time}",
            ja: "検証 {time}",
            ko: "검증 {time}",
          }, { time: formatDateTime(version.source_checked_at_ms) })
          : t({
            en: "Verification time not recorded",
            "zh-CN": "未记录核验时间",
            ja: "検証日時の記録なし",
            ko: "검증 시간이 기록되지 않음",
          })}
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
  const { t } = useI18n();
  const state = versionLifecycle(version, asOfMs);
  return (
    <Badge variant={state === "current" ? "default" : "outline"} className="font-normal">
      {versionLifecycleLabel(state, t)}
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
  const { t } = useI18n();
  return (
    <div className="flex min-h-72 flex-col items-center justify-center px-6 text-center">
      <FileClock className="size-8 text-muted-foreground" aria-hidden="true" />
      <h3 className="mt-4 font-medium">
        {hasBooks
          ? t({ en: "No matching prices", "zh-CN": "没有符合条件的价格", ja: "条件に一致する価格がありません", ko: "조건에 맞는 가격이 없습니다" })
          : t({ en: "No price books yet", "zh-CN": "还没有价格簿", ja: "価格表がまだありません", ko: "아직 가격표가 없습니다" })}
      </h3>
      <p className="mt-1 max-w-md text-sm text-muted-foreground">
        {hasBooks
          ? t({
            en: "Adjust the price type, provider, status, or search criteria and try again.",
            "zh-CN": "调整价格类型、供应商、状态或搜索条件后重试。",
            ja: "価格種別、プロバイダー、状態、検索条件を変更してもう一度お試しください。",
            ko: "가격 유형, 제공업체, 상태 또는 검색 조건을 조정한 후 다시 시도하세요.",
          })
          : t({
            en: "Create a price book with a clear purpose and currency, then add versioned model prices.",
            "zh-CN": "先建立一个用途与币种明确的价格簿，再为模型添加版本化单价。",
            ja: "用途と通貨が明確な価格表を作成してから、バージョン付きモデル価格を追加してください。",
            ko: "용도와 통화가 명확한 가격표를 만든 후 버전별 모델 가격을 추가하세요.",
          })}
      </p>
      {!hasBooks ? (
        <Button type="button" className="mt-5" onClick={onCreate}>
          <Plus aria-hidden="true" />
          {t({ en: "New price book", "zh-CN": "新建价格簿", ja: "価格表を作成", ko: "새 가격표" })}
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

function purposeLabel(purpose: PriceBookPurpose, t: Translate) {
  const labels: Record<PriceBookPurpose, string> = {
    customer_sale: t({ en: "Customer price", "zh-CN": "客户销售价", ja: "顧客販売価格", ko: "고객 판매가" }),
    provider_actual: t({ en: "Actual provider cost", "zh-CN": "供应商实际成本", ja: "プロバイダー実コスト", ko: "제공업체 실제 비용" }),
    provider_estimated: t({ en: "Estimated provider cost", "zh-CN": "供应商预估成本", ja: "プロバイダー推定コスト", ko: "제공업체 예상 비용" }),
    provider_allocated: t({ en: "Subscription or points allocation", "zh-CN": "订阅/积分分摊", ja: "サブスクリプション・ポイント配賦", ko: "구독/포인트 배분" }),
    provider_benchmark: t({ en: "Market benchmark price", "zh-CN": "市场基准价", ja: "市場ベンチマーク価格", ko: "시장 기준 가격" }),
  };
  return labels[purpose];
}

function providerLabel(provider: string | null | undefined, t: Translate) {
  if (!provider) {
    return t({ en: "Unified pricing", "zh-CN": "统一价格", ja: "統一価格", ko: "통합 가격" });
  }
  const labels: Record<string, string> = {
    codex: "Codex",
    openai: "OpenAI",
    grok: "Grok",
    xai: "xAI",
    dreamina: t({ en: "Dreamina", "zh-CN": "即梦", ja: "Dreamina", ko: "Dreamina" }),
    ark: t({ en: "Volcengine Ark", "zh-CN": "火山方舟", ja: "Volcengine Ark", ko: "Volcengine Ark" }),
  };
  return labels[provider.toLowerCase()] ?? provider;
}

function billingModeLabel(mode: PriceBookVersion["billing_mode"], t: Translate) {
  const labels: Record<PriceBookVersion["billing_mode"], string> = {
    customer_rate: t({ en: "Customer price", "zh-CN": "客户销售价", ja: "顧客販売価格", ko: "고객 판매가" }),
    provider_reported: t({ en: "Provider-reported amount", "zh-CN": "供应商回执金额", ja: "プロバイダー報告金額", ko: "제공업체 보고 금액" }),
    published_rate: t({ en: "Official published price", "zh-CN": "官方公开价", ja: "公式公開価格", ko: "공식 공개 가격" }),
    contract_rate: t({ en: "Contract price", "zh-CN": "合同价", ja: "契約価格", ko: "계약 가격" }),
    subscription_allocation: t({ en: "Subscription cost allocation", "zh-CN": "订阅成本分摊", ja: "サブスクリプション費用配賦", ko: "구독 비용 배분" }),
    membership_points: t({ en: "Membership points", "zh-CN": "会员积分", ja: "会員ポイント", ko: "멤버십 포인트" }),
  };
  return labels[mode];
}

function sourceKindLabel(kind: PriceBookVersion["source_kind"], t: Translate) {
  const labels: Record<PriceBookVersion["source_kind"], string> = {
    manual: t({ en: "Manual platform configuration", "zh-CN": "平台人工配置", ja: "プラットフォーム手動設定", ko: "플랫폼 수동 설정" }),
    official_document: t({ en: "Official documentation", "zh-CN": "官方文档", ja: "公式ドキュメント", ko: "공식 문서" }),
    provider_contract: t({ en: "Provider contract", "zh-CN": "供应商合同", ja: "プロバイダー契約", ko: "제공업체 계약" }),
    imported: t({ en: "Bulk import", "zh-CN": "批量导入", ja: "一括インポート", ko: "일괄 가져오기" }),
  };
  return labels[kind];
}

function executionSurfaceLabel(surface: PriceBookVersion["execution_surface"], t: Translate) {
  const labels: Record<PriceBookVersion["execution_surface"], string> = {
    provider_api: t({ en: "Provider API", "zh-CN": "供应商 API", ja: "プロバイダー API", ko: "제공업체 API" }),
    provider_cli: t({ en: "Provider CLI", "zh-CN": "供应商 CLI", ja: "プロバイダー CLI", ko: "제공업체 CLI" }),
    manual_import: t({ en: "Manual import", "zh-CN": "人工导入", ja: "手動インポート", ko: "수동 가져오기" }),
  };
  return labels[surface];
}

function metricLabel(metric: string, t: Translate) {
  const labels: Record<string, string> = {
    request: t({ en: "Request", "zh-CN": "请求", ja: "リクエスト", ko: "요청" }),
    image_input: t({ en: "Input image", "zh-CN": "输入图片", ja: "入力画像", ko: "입력 이미지" }),
    image_output: t({ en: "Output image", "zh-CN": "输出图片", ja: "出力画像", ko: "출력 이미지" }),
    text_input_token: t({ en: "Text input tokens", "zh-CN": "文本输入 token", ja: "テキスト入力トークン", ko: "텍스트 입력 토큰" }),
    cached_text_input_token: t({ en: "Cached text input tokens", "zh-CN": "缓存文本输入 token", ja: "キャッシュ済みテキスト入力トークン", ko: "캐시된 텍스트 입력 토큰" }),
    image_input_token: t({ en: "Image input tokens", "zh-CN": "图片输入 token", ja: "画像入力トークン", ko: "이미지 입력 토큰" }),
    cached_image_input_token: t({ en: "Cached image input tokens", "zh-CN": "缓存图片输入 token", ja: "キャッシュ済み画像入力トークン", ko: "캐시된 이미지 입력 토큰" }),
    image_output_token: t({ en: "Image output tokens", "zh-CN": "图片输出 token", ja: "画像出力トークン", ko: "이미지 출력 토큰" }),
    video_input_token: t({ en: "Video input tokens", "zh-CN": "视频输入 token", ja: "動画入力トークン", ko: "동영상 입력 토큰" }),
    video_output_token: t({ en: "Video output tokens", "zh-CN": "视频输出 token", ja: "動画出力トークン", ko: "동영상 출력 토큰" }),
    video_input_second: t({ en: "Video input seconds", "zh-CN": "输入视频秒数", ja: "動画入力秒数", ko: "동영상 입력 초" }),
    video_requested_second: t({ en: "Requested video seconds", "zh-CN": "请求视频秒数", ja: "リクエスト動画秒数", ko: "요청 동영상 초" }),
    video_output_second: t({ en: "Actual video output seconds", "zh-CN": "实际输出视频秒数", ja: "実際の動画出力秒数", ko: "실제 동영상 출력 초" }),
    membership_point: t({ en: "Membership points", "zh-CN": "会员积分", ja: "会員ポイント", ko: "멤버십 포인트" }),
  };
  return labels[metric] ?? metric;
}

function outcomeLabel(outcome: string, t: Translate) {
  const labels: Record<string, string> = {
    succeeded: t({ en: "Succeeded", "zh-CN": "成功", ja: "成功", ko: "성공" }),
    failed: t({ en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" }),
    no_effect: t({ en: "No output", "zh-CN": "无产出", ja: "出力なし", ko: "출력 없음" }),
    any: t({ en: "Any", "zh-CN": "全部", ja: "すべて", ko: "전체" }),
  };
  return labels[outcome] ?? outcome;
}

function quantitySourceLabel(source: string, t: Translate) {
  const labels: Record<string, string> = {
    provider_reported: t({ en: "Provider reported", "zh-CN": "供应商回执", ja: "プロバイダー報告", ko: "제공업체 보고" }),
    request_derived: t({ en: "Derived from request", "zh-CN": "请求推导", ja: "リクエストから算出", ko: "요청에서 산출" }),
    media_inspected: t({ en: "Media inspection", "zh-CN": "媒体实测", ja: "メディア実測", ko: "미디어 실측" }),
    official_lookup: t({ en: "Official lookup", "zh-CN": "官方查表", ja: "公式参照表", ko: "공식 조회표" }),
    operator_adjustment: t({ en: "Manual adjustment", "zh-CN": "人工调整", ja: "手動調整", ko: "수동 조정" }),
  };
  return labels[source] ?? source;
}

function unitLabel(unit: string, t: Translate) {
  const labels: Record<string, string> = {
    request: t({ en: "requests", "zh-CN": "次请求", ja: "リクエスト", ko: "회 요청" }),
    image: t({ en: "images", "zh-CN": "张", ja: "枚", ko: "장" }),
    token: "token",
    second: t({ en: "seconds", "zh-CN": "秒", ja: "秒", ko: "초" }),
    point: t({ en: "points", "zh-CN": "积分", ja: "ポイント", ko: "포인트" }),
  };
  return labels[unit] ?? unit;
}

function versionLifecycleLabel(state: VersionLifecycle, t: Translate) {
  const labels: Record<VersionLifecycle, string> = {
    current: t({ en: "Active", "zh-CN": "当前生效", ja: "有効", ko: "활성" }),
    scheduled: t({ en: "Scheduled", "zh-CN": "等待生效", ja: "適用待ち", ko: "적용 예정" }),
    ended: t({ en: "Ended", "zh-CN": "已结束", ja: "終了", ko: "종료됨" }),
    draft: t({ en: "Draft", "zh-CN": "草稿", ja: "下書き", ko: "초안" }),
    retired: t({ en: "Retired", "zh-CN": "已退役", ja: "廃止", ko: "사용 중지됨" }),
    unconfigured: t({ en: "Not configured", "zh-CN": "未配置", ja: "未設定", ko: "설정되지 않음" }),
  };
  return labels[state];
}

function publishReadinessLabel(reason: string, t: Translate) {
  const labels: Record<string, string> = {
    version_not_draft: t({ en: "Only draft versions can be published", "zh-CN": "只有草稿版本可以发布", ja: "公開できるのは下書きバージョンのみです", ko: "초안 버전만 게시할 수 있습니다" }),
    source_evidence_missing: t({ en: "Official or contract pricing is missing a source URL or verification time", "zh-CN": "官方文档或合同价格缺少来源地址和核验时间", ja: "公式または契約価格にソース URL または検証日時がありません", ko: "공식 또는 계약 가격에 출처 URL이나 검증 시간이 없습니다" }),
    billing_mode_mismatch: t({ en: "The billing mode does not match the price book purpose", "zh-CN": "计费模式与价格簿用途不一致", ja: "課金モードが価格表の用途と一致しません", ko: "과금 방식이 가격표 용도와 일치하지 않습니다" }),
    execution_surface_mismatch: t({ en: "Customer pricing must apply to the current CLI execution path", "zh-CN": "客户售价必须用于当前 CLI 执行路径", ja: "顧客価格は現在の CLI 実行パスに適用する必要があります", ko: "고객 가격은 현재 CLI 실행 경로에 적용되어야 합니다" }),
    provider_reported_components_present: t({ en: "Provider-reported billing cannot include static pricing components", "zh-CN": "供应商回执金额模式不能再配置静态单价", ja: "プロバイダー報告金額モードには固定価格コンポーネントを設定できません", ko: "제공업체 보고 금액 방식에는 정적 단가를 설정할 수 없습니다" }),
    price_components_missing: t({ en: "No metered items are configured", "zh-CN": "尚未配置计量项", ja: "計量項目が設定されていません", ko: "계량 항목이 설정되지 않았습니다" }),
    component_price_invalid: t({ en: "A metered item has an invalid amount", "zh-CN": "计量项金额格式无效", ja: "計量項目の金額形式が無効です", ko: "계량 항목 금액 형식이 잘못되었습니다" }),
    free_price_has_nonzero_component: t({ en: "A free version still contains a nonzero price", "zh-CN": "免费版本仍包含非零单价", ja: "無料バージョンにゼロ以外の単価が含まれています", ko: "무료 버전에 0이 아닌 단가가 포함되어 있습니다" }),
    paid_price_has_no_positive_success_rate: t({ en: "The paid version has no positive success price", "zh-CN": "付费版本缺少大于零的成功单价", ja: "有料バージョンに0より大きい成功単価がありません", ko: "유료 버전에 0보다 큰 성공 단가가 없습니다" }),
    customer_currency_not_usd: t({ en: "Production requests use USD, so customer pricing must use USD", "zh-CN": "当前生产请求使用 USD，客户售价币种必须为 USD", ja: "本番リクエストは USD を使用するため、顧客価格の通貨も USD である必要があります", ko: "운영 요청은 USD를 사용하므로 고객 가격 통화도 USD여야 합니다" }),
    platform_surface_missing: t({ en: "No matching platform model route was found", "zh-CN": "没有匹配的平台模型路由", ja: "一致するプラットフォームモデルルートがありません", ko: "일치하는 플랫폼 모델 경로가 없습니다" }),
    metering_contract_incompatible: t({ en: "The metric, quantity source, or outcome is incompatible with execution facts", "zh-CN": "计量指标、数量来源或终态与执行事实不兼容", ja: "計量指標、数量ソース、または結果が実行実績と互換性がありません", ko: "계량 지표, 수량 출처 또는 결과가 실행 사실과 호환되지 않습니다" }),
    component_dimensions_invalid: t({ en: "Pricing dimensions must be an object", "zh-CN": "计价维度必须是对象", ja: "価格ディメンションはオブジェクトである必要があります", ko: "가격 차원은 객체여야 합니다" }),
    component_dimension_unsupported: t({ en: "A pricing rule contains dimensions this model request cannot produce", "zh-CN": "计价规则包含该模型请求不会产生的维度", ja: "価格ルールに、このモデルリクエストでは生成されないディメンションが含まれています", ko: "가격 규칙에 이 모델 요청에서 생성되지 않는 차원이 포함되어 있습니다" }),
    component_selector_ambiguous: t({ en: "Duplicate pricing selectors have the same priority", "zh-CN": "存在相同优先级的重复计价选择器", ja: "同じ優先度の重複した価格セレクターがあります", ko: "동일한 우선순위의 중복 가격 선택기가 있습니다" }),
    request_dimension_fallback_missing: t({ en: "Parameterized pricing lacks a base rule covering all request values", "zh-CN": "参数化价格缺少覆盖全部请求值的基础规则", ja: "パラメーター価格に全リクエスト値をカバーする基本ルールがありません", ko: "매개변수화된 가격에 모든 요청 값을 포함하는 기본 규칙이 없습니다" }),
    active_price_resolution_conflict: t({ en: "Another price book has an active rule at the same priority, causing a resolution conflict", "zh-CN": "另一价格簿存在同优先级生效规则，会造成价格解析冲突", ja: "別の価格表に同じ優先度の有効ルールがあり、価格解決が競合します", ko: "다른 가격표에 동일 우선순위의 활성 규칙이 있어 가격 결정 충돌이 발생합니다" }),
    maker_checker_required: t({ en: "Another platform owner must review and publish official pricing drafts", "zh-CN": "官方价格草稿必须由另一位平台负责人复核并发布", ja: "公式価格の下書きは別のプラットフォーム責任者が確認して公開する必要があります", ko: "공식 가격 초안은 다른 플랫폼 책임자가 검토하고 게시해야 합니다" }),
    metering_uses_official_estimate: t({ en: "This model estimates metered quantities using an official conversion table", "zh-CN": "该模型按官方换算表估算计量数量", ja: "このモデルは公式換算表で計量数量を推定します", ko: "이 모델은 공식 환산표를 사용해 계량 수량을 추정합니다" }),
    all_outcomes_share_rate: t({ en: "Success, failure, and no-output outcomes share one price", "zh-CN": "成功、失败和无产出共用同一价格", ja: "成功、失敗、出力なしで同じ価格を使用します", ko: "성공, 실패 및 출력 없음 결과가 동일한 가격을 사용합니다" }),
    dimension_overrides_present: t({ en: "Contains price overrides by request parameter", "zh-CN": "包含按请求参数细分的覆盖价格", ja: "リクエストパラメーター別の価格上書きを含みます", ko: "요청 매개변수별 가격 재정의를 포함합니다" }),
  };
  return labels[reason] ?? reason;
}

function buildPriceBookKey(purpose: PriceBookPurpose, providerId: string) {
  const provider = providerId.trim().toLowerCase().replace(/[^a-z0-9_.-]/g, "") || "all";
  return `${purpose}.${provider}.${Date.now().toString(36)}`;
}

async function responseMessage(response: Response, fallback: string) {
  try {
    const payload = (await response.json()) as { error?: string | { message?: string } };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Fall through to a stable UI message.
  }
  return `${fallback} (${response.status})`;
}
