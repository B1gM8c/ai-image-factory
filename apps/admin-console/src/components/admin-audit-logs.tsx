"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import {
  ChevronLeft,
  ChevronRight,
  Clock3,
  LoaderCircle,
  RefreshCw,
  Search,
  ShieldCheck,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
import { useI18n } from "@/i18n/locale-provider";
import { consoleFetch } from "@/lib/auth/client";

type Translate = ReturnType<typeof useI18n>["t"];

type AuditLogActor = {
  type: "user" | "system";
  user_id: string | null;
  email: string | null;
  display_name: string | null;
  session_id: string | null;
  ip_address: string | null;
};

type AuditLogProject = {
  id: string;
  name: string | null;
  organization_id: string | null;
};

type AuditLogResource = {
  type: string | null;
  id: string | null;
};

type AuditLogItem = {
  id: string;
  object: "audit_log";
  type: string;
  effective_at: number;
  actor: AuditLogActor;
  project: AuditLogProject | null;
  resource: AuditLogResource;
  request_id: string | null;
  outcome: "success" | "denied" | "failure";
  reason_code: string | null;
  details: Record<string, unknown>;
};

type AuditLogsSnapshot = {
  object: "list";
  as_of_ms: number;
  from_ms: number;
  to_ms: number;
  data: AuditLogItem[];
  first_id: string | null;
  last_id: string | null;
  has_more: boolean;
  next_after: string | null;
};

type Filters = {
  window: "24h" | "7d" | "30d" | "90d";
  outcome: "all" | AuditLogItem["outcome"];
  eventType: string;
  query: string;
};

type PageAnchor = {
  after: string | null;
  toMs: number | null;
};

const DEFAULT_FILTERS: Filters = {
  window: "7d",
  outcome: "all",
  eventType: "all",
  query: "",
};

export function AdminAuditLogs() {
  const { locale, t } = useI18n();
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState<PageAnchor>({ after: null, toMs: null });
  const [previousPages, setPreviousPages] = useState<PageAnchor[]>([]);
  const [data, setData] = useState<AuditLogsSnapshot | null>(null);
  const [selected, setSelected] = useState<AuditLogItem | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        window: filters.window,
        limit: "50",
      });
      if (filters.outcome !== "all") params.set("outcome", filters.outcome);
      if (filters.eventType !== "all") {
        params.set("event_type", filters.eventType);
      }
      if (filters.query) params.set("q", filters.query);
      if (page.after) params.set("after", page.after);
      if (page.toMs) params.set("to_ms", page.toMs.toString());
      const response = await consoleFetch(
        `/api/gateway/v1/organization/audit_logs?${params.toString()}`,
        { signal },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      setData((await response.json()) as AuditLogsSnapshot);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === "AbortError") return;
      setError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Could not load audit logs",
              "zh-CN": "审计日志加载失败",
              ja: "監査ログを読み込めませんでした",
              ko: "감사 로그를 불러올 수 없습니다",
            }),
      );
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [filters, page, t]);

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load]);

  const eventTypes = useMemo(() => {
    const values = new Set(data?.data.map((item) => item.type) ?? []);
    if (filters.eventType !== "all") values.add(filters.eventType);
    return [...values].sort((left, right) => left.localeCompare(right));
  }, [data?.data, filters.eventType]);

  function resetPage(nextFilters: Filters) {
    setFilters(nextFilters);
    setPage({ after: null, toMs: null });
    setPreviousPages([]);
  }

  function applySearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    resetPage({ ...filters, query: search.trim() });
  }

  function nextPage() {
    if (!data?.next_after) return;
    setPreviousPages((current) => [...current, page]);
    setPage({ after: data.next_after, toMs: data.to_ms });
  }

  function previousPage() {
    const previous = previousPages.at(-1);
    if (!previous) return;
    setPreviousPages((current) => current.slice(0, -1));
    setPage(previous);
  }

  return (
    <>
      <section className="min-w-0 overflow-hidden rounded-lg border bg-background">
        <div className="flex flex-col gap-3 border-b p-4 xl:flex-row xl:items-center">
          <form className="relative min-w-0 flex-1" onSubmit={applySearch}>
            <Search
              className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              className="pl-9"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder={t({
                en: "Search events, users, Request IDs, or resources",
                "zh-CN": "搜索事件、用户、Request ID 或资源",
                ja: "イベント、ユーザー、Request ID、リソースを検索",
                ko: "이벤트, 사용자, Request ID 또는 리소스 검색",
              })}
              aria-label={t({ en: "Search audit logs", "zh-CN": "搜索审计日志", ja: "監査ログを検索", ko: "감사 로그 검색" })}
            />
          </form>
          <div className="grid grid-cols-2 gap-2 sm:flex">
            <Select
              value={filters.window}
              onValueChange={(value) =>
                resetPage({ ...filters, window: value as Filters["window"] })
              }
            >
              <SelectTrigger className="w-full sm:w-36" aria-label={t({ en: "Time range", "zh-CN": "时间范围", ja: "期間", ko: "기간" })}>
                <Clock3 aria-hidden="true" />
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="24h">{t({ en: "Last 24 hours", "zh-CN": "最近 24 小时", ja: "過去 24 時間", ko: "최근 24시간" })}</SelectItem>
                <SelectItem value="7d">{t({ en: "Last 7 days", "zh-CN": "最近 7 天", ja: "過去 7 日間", ko: "최근 7일" })}</SelectItem>
                <SelectItem value="30d">{t({ en: "Last 30 days", "zh-CN": "最近 30 天", ja: "過去 30 日間", ko: "최근 30일" })}</SelectItem>
                <SelectItem value="90d">{t({ en: "Last 90 days", "zh-CN": "最近 90 天", ja: "過去 90 日間", ko: "최근 90일" })}</SelectItem>
              </SelectContent>
            </Select>
            <Select
              value={filters.outcome}
              onValueChange={(value) =>
                resetPage({
                  ...filters,
                  outcome: value as Filters["outcome"],
                })
              }
            >
              <SelectTrigger className="w-full sm:w-32" aria-label={t({ en: "Outcome", "zh-CN": "操作结果", ja: "結果", ko: "결과" })}>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t({ en: "All outcomes", "zh-CN": "全部结果", ja: "すべての結果", ko: "모든 결과" })}</SelectItem>
                <SelectItem value="success">{t({ en: "Success", "zh-CN": "成功", ja: "成功", ko: "성공" })}</SelectItem>
                <SelectItem value="denied">{t({ en: "Denied", "zh-CN": "已拒绝", ja: "拒否", ko: "거부됨" })}</SelectItem>
                <SelectItem value="failure">{t({ en: "Failure", "zh-CN": "失败", ja: "失敗", ko: "실패" })}</SelectItem>
              </SelectContent>
            </Select>
            <Select
              value={filters.eventType}
              onValueChange={(value) =>
                resetPage({ ...filters, eventType: value })
              }
            >
              <SelectTrigger
                className="col-span-2 w-full sm:w-64"
                aria-label={t({ en: "Event type", "zh-CN": "事件类型", ja: "イベント種別", ko: "이벤트 유형" })}
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t({ en: "All events", "zh-CN": "全部事件", ja: "すべてのイベント", ko: "모든 이벤트" })}</SelectItem>
                {eventTypes.map((eventType) => (
                  <SelectItem key={eventType} value={eventType}>
                    {formatEventType(t, eventType)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              type="button"
              variant="outline"
              size="icon"
              disabled={loading}
              onClick={() => void load()}
              title={t({ en: "Refresh", "zh-CN": "刷新", ja: "更新", ko: "새로 고침" })}
              aria-label={t({ en: "Refresh audit logs", "zh-CN": "刷新审计日志", ja: "監査ログを更新", ko: "감사 로그 새로 고침" })}
            >
              <RefreshCw
                className={loading ? "animate-spin" : undefined}
                aria-hidden="true"
              />
            </Button>
          </div>
        </div>

        {error ? (
          <div
            className="flex min-h-64 flex-col items-center justify-center gap-3 px-6 text-center"
            role="alert"
          >
            <ShieldCheck className="size-8 text-muted-foreground" aria-hidden="true" />
            <div>
              <p className="font-medium">{t({ en: "Could not load audit logs", "zh-CN": "审计日志加载失败", ja: "監査ログを読み込めませんでした", ko: "감사 로그를 불러올 수 없습니다" })}</p>
              <p className="mt-1 text-sm text-muted-foreground">{error}</p>
            </div>
            <Button variant="outline" size="sm" onClick={() => void load()}>
              <RefreshCw aria-hidden="true" />
              {t({ en: "Reload", "zh-CN": "重新加载", ja: "再読み込み", ko: "다시 불러오기" })}
            </Button>
          </div>
        ) : (
          <div className="max-w-full overflow-x-auto">
            <Table className="min-w-[920px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-44 pl-4">{t({ en: "Time", "zh-CN": "时间", ja: "時刻", ko: "시간" })}</TableHead>
                  <TableHead>{t({ en: "Event", "zh-CN": "事件", ja: "イベント", ko: "이벤트" })}</TableHead>
                  <TableHead>{t({ en: "Actor", "zh-CN": "操作者", ja: "実行者", ko: "작업자" })}</TableHead>
                  <TableHead>{t({ en: "Project", "zh-CN": "项目", ja: "プロジェクト", ko: "프로젝트" })}</TableHead>
                  <TableHead className="w-28">{t({ en: "Outcome", "zh-CN": "结果", ja: "結果", ko: "결과" })}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && !data ? (
                  <TableRow>
                    <TableCell
                      colSpan={5}
                      className="h-64 text-center text-muted-foreground"
                    >
                      <LoaderCircle
                        className="mx-auto mb-2 size-5 animate-spin"
                        aria-hidden="true"
                      />
                      {t({ en: "Loading audit logs", "zh-CN": "正在加载审计日志", ja: "監査ログを読み込み中", ko: "감사 로그 불러오는 중" })}
                    </TableCell>
                  </TableRow>
                ) : null}
                {!loading && data?.data.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="h-64 text-center">
                      <ShieldCheck
                        className="mx-auto mb-3 size-8 text-muted-foreground"
                        aria-hidden="true"
                      />
                      <p className="font-medium">{t({ en: "No matching audit events", "zh-CN": "没有匹配的审计事件", ja: "一致する監査イベントはありません", ko: "일치하는 감사 이벤트가 없습니다" })}</p>
                      <p className="mt-1 text-sm text-muted-foreground">
                        {t({ en: "Adjust the time range or filters and try again.", "zh-CN": "调整时间范围或筛选条件后再试。", ja: "期間またはフィルターを調整して再試行してください。", ko: "기간 또는 필터를 조정한 후 다시 시도하세요." })}
                      </p>
                    </TableCell>
                  </TableRow>
                ) : null}
                {data?.data.map((item) => (
                  <TableRow
                    key={item.id}
                    className="cursor-pointer"
                    tabIndex={0}
                    onClick={() => setSelected(item)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        setSelected(item);
                      }
                    }}
                  >
                    <TableCell className="pl-4 text-sm text-muted-foreground">
                      {formatDateTime(item.effective_at, locale)}
                    </TableCell>
                    <TableCell>
                      <p className="font-medium">{formatEventType(t, item.type)}</p>
                      <p className="mt-0.5 max-w-80 truncate font-mono text-xs text-muted-foreground">
                        {item.type}
                      </p>
                    </TableCell>
                    <TableCell>
                      <p className="max-w-56 truncate">
                        {item.actor.display_name ??
                          item.actor.email ??
                          (item.actor.type === "system"
                            ? t({ en: "System", "zh-CN": "系统", ja: "システム", ko: "시스템" })
                            : t({ en: "Unknown user", "zh-CN": "未知用户", ja: "不明なユーザー", ko: "알 수 없는 사용자" }))}
                      </p>
                      {item.actor.display_name && item.actor.email ? (
                        <p className="mt-0.5 max-w-56 truncate text-xs text-muted-foreground">
                          {item.actor.email}
                        </p>
                      ) : null}
                    </TableCell>
                    <TableCell>
                      <p className="max-w-52 truncate">
                        {item.project?.name ?? item.project?.id ?? t({ en: "Global", "zh-CN": "全局", ja: "グローバル", ko: "전역" })}
                      </p>
                    </TableCell>
                    <TableCell>
                      <OutcomeBadge outcome={item.outcome} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}

        <div className="flex items-center justify-between border-t px-4 py-3">
          <p className="text-sm text-muted-foreground">
            {data
              ? t(
                  { en: "Updated {time}", "zh-CN": "更新于 {time}", ja: "更新日時 {time}", ko: "업데이트 {time}" },
                  { time: formatTimestamp(data.as_of_ms, locale) },
                )
              : " "}
          </p>
          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={previousPages.length === 0 || loading}
              onClick={previousPage}
            >
              <ChevronLeft aria-hidden="true" />
              {t({ en: "Previous", "zh-CN": "上一页", ja: "前へ", ko: "이전" })}
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!data?.has_more || loading}
              onClick={nextPage}
            >
              {t({ en: "Next", "zh-CN": "下一页", ja: "次へ", ko: "다음" })}
              <ChevronRight aria-hidden="true" />
            </Button>
          </div>
        </div>
      </section>

      <AuditLogDetails item={selected} onOpenChange={(open) => !open && setSelected(null)} />
    </>
  );
}

function AuditLogDetails({
  item,
  onOpenChange,
}: {
  item: AuditLogItem | null;
  onOpenChange: (open: boolean) => void;
}) {
  const { locale, t } = useI18n();

  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
        <SheetHeader className="text-left">
          <SheetTitle>{item ? formatEventType(t, item.type) : t({ en: "Audit event", "zh-CN": "审计事件", ja: "監査イベント", ko: "감사 이벤트" })}</SheetTitle>
          <SheetDescription>
            {item ? formatDateTime(item.effective_at, locale) : ""}
          </SheetDescription>
        </SheetHeader>
        {item ? (
          <div className="mt-6 space-y-6">
            <div className="grid grid-cols-[7rem_minmax(0,1fr)] gap-x-4 gap-y-3 text-sm">
              <span className="text-muted-foreground">{t({ en: "Outcome", "zh-CN": "结果", ja: "結果", ko: "결과" })}</span>
              <OutcomeBadge outcome={item.outcome} />
              <span className="text-muted-foreground">{t({ en: "Event type", "zh-CN": "事件类型", ja: "イベント種別", ko: "이벤트 유형" })}</span>
              <code className="break-all text-xs">{item.type}</code>
              <span className="text-muted-foreground">{t({ en: "Actor", "zh-CN": "操作者", ja: "実行者", ko: "작업자" })}</span>
              <span className="min-w-0 break-words">
                {item.actor.display_name ?? item.actor.email ?? item.actor.type}
              </span>
              <span className="text-muted-foreground">{t({ en: "Project", "zh-CN": "项目", ja: "プロジェクト", ko: "프로젝트" })}</span>
              <span className="min-w-0 break-all">
                {item.project?.name ?? item.project?.id ?? t({ en: "Global", "zh-CN": "全局", ja: "グローバル", ko: "전역" })}
              </span>
              <span className="text-muted-foreground">{t({ en: "Resource", "zh-CN": "资源", ja: "リソース", ko: "리소스" })}</span>
              <span className="min-w-0 break-all">
                {[item.resource.type, item.resource.id].filter(Boolean).join(" · ") ||
                  t({ en: "None", "zh-CN": "无", ja: "なし", ko: "없음" })}
              </span>
              <span className="text-muted-foreground">
                {t({
                  en: "Request ID",
                  "zh-CN": "请求 ID",
                  ja: "リクエスト ID",
                  ko: "요청 ID",
                })}
              </span>
              <code className="min-w-0 break-all text-xs">
                {item.request_id ?? t({ en: "None", "zh-CN": "无", ja: "なし", ko: "없음" })}
              </code>
              <span className="text-muted-foreground">
                {t({
                  en: "Session ID",
                  "zh-CN": "会话 ID",
                  ja: "セッション ID",
                  ko: "세션 ID",
                })}
              </span>
              <code className="min-w-0 break-all text-xs">
                {item.actor.session_id ?? t({ en: "None", "zh-CN": "无", ja: "なし", ko: "없음" })}
              </code>
              <span className="text-muted-foreground">{t({ en: "Event ID", "zh-CN": "事件 ID", ja: "イベント ID", ko: "이벤트 ID" })}</span>
              <code className="min-w-0 break-all text-xs">{item.id}</code>
            </div>
            <div>
              <h3 className="text-sm font-medium">{t({ en: "Event details", "zh-CN": "事件详情", ja: "イベント詳細", ko: "이벤트 세부 정보" })}</h3>
              <pre className="mt-3 max-h-80 overflow-auto rounded-md bg-muted p-4 text-xs leading-5">
                {JSON.stringify(item.details, null, 2)}
              </pre>
            </div>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function OutcomeBadge({ outcome }: { outcome: AuditLogItem["outcome"] }) {
  const { t } = useI18n();

  return (
    <Badge variant={outcome === "success" ? "outline" : "secondary"}>
      {outcome === "success"
        ? t({ en: "Success", "zh-CN": "成功", ja: "成功", ko: "성공" })
        : outcome === "denied"
          ? t({ en: "Denied", "zh-CN": "已拒绝", ja: "拒否", ko: "거부됨" })
          : t({ en: "Failure", "zh-CN": "失败", ja: "失敗", ko: "실패" })}
    </Badge>
  );
}

function formatEventType(t: Translate, value: string) {
  const known: Record<string, Parameters<Translate>[0]> = {
    "billing.credit_grant.issue": { en: "Issue promotional credit", "zh-CN": "发放赠送额度", ja: "プロモーションクレジットを付与", ko: "프로모션 크레딧 지급" },
    "billing.credit_grant.revoke": { en: "Revoke promotional credit", "zh-CN": "撤销赠送额度", ja: "プロモーションクレジットを取消", ko: "프로모션 크레딧 회수" },
    "billing.integrity.run": { en: "Run billing integrity check", "zh-CN": "执行计费完整性校验", ja: "請求整合性チェックを実行", ko: "결제 무결성 검사 실행" },
    "identity.bootstrap": { en: "Initialize administrator", "zh-CN": "初始化管理员", ja: "管理者を初期化", ko: "관리자 초기화" },
    "identity.login": { en: "User sign-in", "zh-CN": "用户登录", ja: "ユーザーサインイン", ko: "사용자 로그인" },
    "identity.login.succeeded": { en: "User sign-in", "zh-CN": "用户登录", ja: "ユーザーサインイン", ko: "사용자 로그인" },
    "identity.login.failed": { en: "Sign-in failed", "zh-CN": "登录失败", ja: "サインイン失敗", ko: "로그인 실패" },
    "identity.refresh": { en: "Refresh session", "zh-CN": "刷新会话", ja: "セッションを更新", ko: "세션 새로 고침" },
    "identity.logout": { en: "User sign-out", "zh-CN": "用户退出", ja: "ユーザーサインアウト", ko: "사용자 로그아웃" },
    "pricing.official_source.sync": { en: "Sync official pricing", "zh-CN": "同步官方价格", ja: "公式価格を同期", ko: "공식 가격 동기화" },
    "pricing.price_book_version.publish": { en: "Publish price-book version", "zh-CN": "发布价格版本", ja: "価格表バージョンを公開", ko: "가격표 버전 게시" },
    "project.create": { en: "Create project", "zh-CN": "创建项目", ja: "プロジェクトを作成", ko: "프로젝트 생성" },
    "project.settings.update": { en: "Update project settings", "zh-CN": "更新项目设置", ja: "プロジェクト設定を更新", ko: "프로젝트 설정 업데이트" },
    "project.member.add": { en: "Add project member", "zh-CN": "添加项目成员", ja: "プロジェクトメンバーを追加", ko: "프로젝트 멤버 추가" },
    "project.member.update": { en: "Update project member", "zh-CN": "更新项目成员", ja: "プロジェクトメンバーを更新", ko: "프로젝트 멤버 업데이트" },
    "project.member.remove": { en: "Remove project member", "zh-CN": "移除项目成员", ja: "プロジェクトメンバーを削除", ko: "프로젝트 멤버 제거" },
    "project.api_key.create": { en: "Create API Key", "zh-CN": "创建 API Key", ja: "API Key を作成", ko: "API Key 생성" },
    "project.api_key.update": { en: "Update API Key", "zh-CN": "更新 API Key", ja: "API Key を更新", ko: "API Key 업데이트" },
    "project.api_key.revoke": { en: "Revoke API Key", "zh-CN": "撤销 API Key", ja: "API Key を失効", ko: "API Key 폐기" },
    "project.service_account.create": { en: "Create service account", "zh-CN": "创建服务账号", ja: "サービスアカウントを作成", ko: "서비스 계정 생성" },
    "project.service_account.delete": { en: "Delete service account", "zh-CN": "删除服务账号", ja: "サービスアカウントを削除", ko: "서비스 계정 삭제" },
    "project.model_policy.updated": { en: "Update model policy", "zh-CN": "更新模型策略", ja: "モデルポリシーを更新", ko: "모델 정책 업데이트" },
  };
  return known[value] ? t(known[value]) : value;
}

function formatDateTime(unixSeconds: number, locale: string) {
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(unixSeconds * 1_000));
}

function formatTimestamp(timestampMs: number, locale: string) {
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestampMs));
}

async function responseMessage(response: Response, t: Translate) {
  const body = (await response.json().catch(() => null)) as
    | { error?: { message?: string } }
    | null;
  return (
    body?.error?.message ??
    t(
      {
        en: "Request failed ({status})",
        "zh-CN": "请求失败（{status}）",
        ja: "リクエスト失敗（{status}）",
        ko: "요청 실패({status})",
      },
      { status: response.status },
    )
  );
}
