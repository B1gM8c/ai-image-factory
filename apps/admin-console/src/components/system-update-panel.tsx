"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  BookOpen,
  Braces,
  CheckCircle2,
  Database,
  GitBranch,
  HeartPulse,
  LoaderCircle,
  Network,
  RefreshCw,
  ServerCog,
  ShieldCheck,
} from "lucide-react";
import { toast } from "sonner";
import { MetricCard } from "@/components/metric-card";
import { PageHeader } from "@/components/page-header";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useI18n } from "@/i18n/locale-provider";
import { formatDateTime } from "@/lib/admin/format";
import {
  applySystemUpdate,
  checkSystemUpdate,
  getSystemUpdate,
  type SystemUpdateCommand,
  type SystemUpdateSnapshot,
} from "@/lib/admin/system-updates";
import type { GatewaySnapshot } from "@/lib/gateway/server";

const POLL_INTERVAL_MS = 2_500;
const RECENT_COMMAND_LIMIT = 6;
type Translate = ReturnType<typeof useI18n>["t"];

export function SystemStatusView({ snapshot }: { snapshot: GatewaySnapshot }) {
  const { locale, t } = useI18n();
  const profiles = snapshot.providerProfiles;

  return (
    <div className="space-y-6">
      <PageHeader
        title={t({ en: "System status", "zh-CN": "系统状态", ja: "システム状態", ko: "시스템 상태" })}
        description={t({
          en: "Gateway probes, Provider Runtime aggregation, and API contract entry points.",
          "zh-CN": "Gateway 探针、Provider Runtime 聚合和 API 契约入口。",
          ja: "Gateway プローブ、Provider Runtime 集約、API コントラクトのエントリーポイント。",
          ko: "Gateway 프로브, Provider Runtime 집계 및 API 계약 진입점입니다.",
        })}
        actions={
          <>
            <Button variant="outline" size="sm" asChild>
              <Link href="/api/gateway/openapi.json" target="_blank">
                <Braces aria-hidden="true" />
                OpenAPI
              </Link>
            </Button>
            <Button size="sm" asChild>
              <Link href="/api/gateway/openapi.json" target="_blank">
                <BookOpen aria-hidden="true" />
                OpenAPI JSON
              </Link>
            </Button>
          </>
        }
      />
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t({ en: "Liveness", "zh-CN": "存活状态", ja: "稼働状態", ko: "가동 상태" })}
          value={
            snapshot.health === "ok"
              ? t({ en: "Healthy", "zh-CN": "正常", ja: "正常", ko: "정상" })
              : t({ en: "Unreachable", "zh-CN": "不可达", ja: "到達不能", ko: "연결할 수 없음" })
          }
          detail="Gateway `/healthz`"
          icon={HeartPulse}
          tone={snapshot.health === "ok" ? "success" : "danger"}
        />
        <MetricCard
          label={t({ en: "Readiness", "zh-CN": "就绪状态", ja: "準備状態", ko: "준비 상태" })}
          value={
            snapshot.readiness === "ready"
              ? t({ en: "Ready", "zh-CN": "就绪", ja: "準備完了", ko: "준비됨" })
              : snapshot.readiness === "not_ready"
                ? t({ en: "Not ready", "zh-CN": "未就绪", ja: "準備未完了", ko: "준비되지 않음" })
                : t({ en: "Unreachable", "zh-CN": "不可达", ja: "到達不能", ko: "연결할 수 없음" })
          }
          detail="Gateway `/readyz`"
          icon={Network}
          tone={snapshot.readiness === "ready" ? "success" : "danger"}
        />
        <MetricCard
          label={t({ en: "Active profiles", "zh-CN": "可用配置", ja: "有効なプロファイル", ko: "활성 프로필" })}
          value={profiles ? String(profiles.active) : "--"}
          detail={t({
            en: "Provider readiness projection",
            "zh-CN": "供应商就绪状态投影",
            ja: "プロバイダー準備状態の集計",
            ko: "공급자 준비 상태 집계",
          })}
          icon={Database}
          tone={profiles?.active ? "info" : "neutral"}
        />
        <MetricCard
          label={t({ en: "Blocked profiles", "zh-CN": "受阻配置", ja: "ブロック中のプロファイル", ko: "차단된 프로필" })}
          value={profiles ? String(profiles.blocked) : "--"}
          detail={t({
            en: "Credential details are not exposed",
            "zh-CN": "不包含具体凭据原因",
            ja: "認証情報の詳細は表示されません",
            ko: "자격 증명 세부 정보는 표시되지 않습니다",
          })}
          icon={Database}
          tone={profiles?.blocked ? "danger" : "success"}
        />
      </section>
      <SystemUpdatePanel />
      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t({ en: "Probe details", "zh-CN": "探针详情", ja: "プローブ詳細", ko: "프로브 세부 정보" })}</CardTitle>
          <CardDescription>
            {t(
              { en: "Checked {time}", "zh-CN": "检查时间 {time}", ja: "確認日時 {time}", ko: "확인 시간 {time}" },
              {
                time: new Date(snapshot.checkedAt).toLocaleString(locale, {
                  hour12: false,
                }),
              },
            )}
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 sm:grid-cols-2">
          <SystemStatusRow
            label={t({ en: "Gateway process", "zh-CN": "Gateway 进程", ja: "Gateway プロセス", ko: "Gateway 프로세스" })}
            ok={snapshot.health === "ok"}
          />
          <SystemStatusRow
            label={t({ en: "Database and Profile projection", "zh-CN": "数据库与 Profile 投影", ja: "データベースと Profile 投影", ko: "데이터베이스 및 Profile 프로젝션" })}
            ok={snapshot.readiness === "ready"}
          />
          <SystemStatusRow
            label={t({ en: "Provider Profile data", "zh-CN": "Provider Profile 数据", ja: "Provider Profile データ", ko: "Provider Profile 데이터" })}
            ok={Boolean(profiles)}
          />
          <div className="flex items-center justify-between border px-3 py-2.5 text-sm">
            <span>{t({ en: "Per-instance executor status", "zh-CN": "执行器逐实例状态", ja: "実行ワーカーのインスタンス別状態", ko: "실행기 인스턴스별 상태" })}</span>
            <Badge variant="outline">{t({ en: "Awaiting Read API", "zh-CN": "等待 Read API", ja: "Read API 待ち", ko: "Read API 대기" })}</Badge>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function SystemStatusRow({ label, ok }: { label: string; ok: boolean }) {
  const { t } = useI18n();

  return (
    <div className="flex items-center justify-between border px-3 py-2.5 text-sm">
      <span>{label}</span>
      <Badge className={ok ? "bg-muted/50" : ""} variant={ok ? "outline" : "destructive"}>
        {ok
          ? t({ en: "Healthy", "zh-CN": "正常", ja: "正常", ko: "정상" })
          : t({ en: "Issue", "zh-CN": "异常", ja: "異常", ko: "이상" })}
      </Badge>
    </div>
  );
}

export function SystemUpdatePanel() {
  const { t } = useI18n();
  const [snapshot, setSnapshot] = useState<SystemUpdateSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [mutation, setMutation] = useState<"check" | "apply" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const refresh = useCallback(async (signal?: AbortSignal, silent = false) => {
    if (!silent) setLoading(true);
    try {
      const next = await getSystemUpdate(signal);
      setSnapshot(next);
      setError(null);
    } catch (reason) {
      if (!isAbortError(reason)) {
        setError(
          reason instanceof Error
            ? localizedSystemUpdateError(t, reason.message)
            : t({ en: "Could not load system update status", "zh-CN": "系统更新状态加载失败", ja: "システム更新状態を読み込めませんでした", ko: "시스템 업데이트 상태를 불러올 수 없습니다" }),
        );
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }, [t]);

  useEffect(() => {
    const controller = new AbortController();
    void refresh(controller.signal);
    return () => controller.abort();
  }, [refresh]);

  const pollingCommandId =
    snapshot?.active_command &&
    ["queued", "running", "restoring"].includes(snapshot.active_command.status)
      ? snapshot.active_command.command_id
      : null;
  useEffect(() => {
    if (!pollingCommandId) return;

    let disposed = false;
    let timer: number | undefined;
    let controller: AbortController | undefined;

    async function poll() {
      controller = new AbortController();
      try {
        const next = await getSystemUpdate(controller.signal);
        if (!disposed) {
          setSnapshot(next);
          setError(null);
        }
      } catch (reason) {
        if (!disposed && !isAbortError(reason)) {
          setError(
            reason instanceof Error
              ? localizedSystemUpdateError(t, reason.message)
              : t({ en: "Could not refresh system update status", "zh-CN": "系统更新状态刷新失败", ja: "システム更新状態を更新できませんでした", ko: "시스템 업데이트 상태를 새로 고칠 수 없습니다" }),
          );
        }
      } finally {
        if (!disposed) timer = window.setTimeout(poll, POLL_INTERVAL_MS);
      }
    }

    timer = window.setTimeout(poll, POLL_INTERVAL_MS);
    return () => {
      disposed = true;
      if (timer !== undefined) window.clearTimeout(timer);
      controller?.abort();
    };
  }, [pollingCommandId, t]);

  const installEnabled = Boolean(
    snapshot?.apply_enabled === true &&
      snapshot.latest_verified === true &&
      snapshot.update_available === true &&
      snapshot.latest_version &&
      !snapshot.active_command &&
      !mutation,
  );

  const recentCommands = useMemo(
    () => snapshot?.recent_commands.slice(0, RECENT_COMMAND_LIMIT) ?? [],
    [snapshot?.recent_commands],
  );

  async function requestCheck() {
    setMutation("check");
    setError(null);
    try {
      const command = await checkSystemUpdate();
      mergeCommand(command);
      toast.success(t({ en: "Update check queued", "zh-CN": "更新检查已加入队列", ja: "更新チェックをキューに追加しました", ko: "업데이트 확인이 대기열에 추가되었습니다" }));
    } catch (reason) {
      const message = reason instanceof Error
        ? localizedSystemUpdateError(t, reason.message)
        : t({ en: "Could not check for updates", "zh-CN": "无法检查更新", ja: "更新を確認できませんでした", ko: "업데이트를 확인할 수 없습니다" });
      setError(message);
      toast.error(message);
    } finally {
      setMutation(null);
    }
  }

  async function requestApply() {
    if (!snapshot?.latest_version || !installEnabled) return;
    setMutation("apply");
    setError(null);
    try {
      const command = await applySystemUpdate(snapshot.latest_version);
      mergeCommand(command);
      setConfirmOpen(false);
      toast.success(
        t(
          { en: "Install command submitted for {version}", "zh-CN": "已提交 {version} 安装命令", ja: "{version} のインストールコマンドを送信しました", ko: "{version} 설치 명령을 제출했습니다" },
          { version: snapshot.latest_version },
        ),
      );
    } catch (reason) {
      const message = reason instanceof Error
        ? localizedSystemUpdateError(t, reason.message)
        : t({ en: "Could not install the update", "zh-CN": "无法安装更新", ja: "更新をインストールできませんでした", ko: "업데이트를 설치할 수 없습니다" });
      setError(message);
      toast.error(message);
    } finally {
      setMutation(null);
    }
  }

  function mergeCommand(command: SystemUpdateCommand) {
    setSnapshot((current) => {
      if (!current) return current;
      return {
        ...current,
        active_command: command,
        recent_commands: [
          command,
          ...current.recent_commands.filter(
            (item) => item.command_id !== command.command_id,
          ),
        ],
      };
    });
  }

  if (loading && !snapshot) {
    return (
      <Card aria-label={t({ en: "Loading system update status", "zh-CN": "正在加载系统更新状态", ja: "システム更新状態を読み込み中", ko: "시스템 업데이트 상태 불러오는 중" })}>
        <CardContent className="flex h-28 items-center justify-center">
          <LoaderCircle className="size-5 animate-spin text-muted-foreground" aria-hidden="true" />
        </CardContent>
      </Card>
    );
  }

  if (!snapshot) {
    return (
      <Card>
        <CardContent className="flex min-h-32 flex-col items-center justify-center gap-3 text-center">
          <AlertCircle className="size-5 text-muted-foreground" aria-hidden="true" />
          <p className="text-sm text-muted-foreground">{error ?? t({ en: "System update status is unavailable", "zh-CN": "系统更新状态不可用", ja: "システム更新状態を利用できません", ko: "시스템 업데이트 상태를 사용할 수 없습니다" })}</p>
          <Button variant="outline" size="sm" onClick={() => void refresh()}>
            <RefreshCw aria-hidden="true" />
            {t({ en: "Retry", "zh-CN": "重试", ja: "再試行", ko: "다시 시도" })}
          </Button>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="border-b has-data-[slot=card-action]:grid-cols-1 sm:has-data-[slot=card-action]:grid-cols-[1fr_auto]">
        <CardTitle className="flex items-center gap-2 text-base">
          <ServerCog className="size-4" aria-hidden="true" />
          {t({ en: "System updates", "zh-CN": "系统更新", ja: "システム更新", ko: "시스템 업데이트" })}
        </CardTitle>
        <CardDescription>
          {t({
            en: "Only GitHub Releases that pass release-integrity verification can be installed.",
            "zh-CN": "仅安装通过发布完整性验证的 GitHub Release。",
            ja: "リリース整合性検証に合格した GitHub Release のみインストールできます。",
            ko: "릴리스 무결성 검증을 통과한 GitHub Release만 설치할 수 있습니다.",
          })}
        </CardDescription>
        <CardAction className="col-start-1 row-span-1 row-start-3 flex w-full items-center gap-2 justify-self-stretch sm:col-start-2 sm:row-span-2 sm:row-start-1 sm:w-auto sm:justify-self-end">
          <Button
            variant="outline"
            size="sm"
            disabled={!snapshot.configured || Boolean(snapshot.active_command) || mutation !== null}
            onClick={() => void requestCheck()}
          >
            {mutation === "check" ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RefreshCw aria-hidden="true" />
            )}
            {t({ en: "Check for updates", "zh-CN": "检查更新", ja: "更新を確認", ko: "업데이트 확인" })}
          </Button>
          <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
            <AlertDialogTrigger asChild>
              <Button size="sm" disabled={!installEnabled}>
                {mutation === "apply" ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <ShieldCheck aria-hidden="true" />
                )}
                {t({ en: "Install update", "zh-CN": "安装更新", ja: "更新をインストール", ko: "업데이트 설치" })}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>
                  {t(
                    { en: "Install {version}?", "zh-CN": "安装 {version}？", ja: "{version} をインストールしますか？", ko: "{version}을(를) 설치할까요?" },
                    { version: snapshot.latest_version ?? "--" },
                  )}
                </AlertDialogTitle>
                <AlertDialogDescription>
                  {t({
                    en: "The system will drain jobs, create a recovery point, run database migrations, and atomically switch the complete release. The service may briefly enter maintenance mode.",
                    "zh-CN": "系统将排空任务、创建恢复点、执行数据库迁移并原子切换完整发布版本。更新期间服务可能短暂进入维护状态。",
                    ja: "ジョブをドレインし、復旧ポイントを作成し、データベース移行を実行して、完全なリリースへアトミックに切り替えます。更新中は一時的にメンテナンス状態になる場合があります。",
                    ko: "시스템이 작업을 드레이닝하고 복구 지점을 만든 뒤 데이터베이스 마이그레이션을 실행하고 전체 릴리스로 원자적으로 전환합니다. 업데이트 중 잠시 유지 관리 상태가 될 수 있습니다.",
                  })}
                </AlertDialogDescription>
              </AlertDialogHeader>
              <div className="grid gap-2 border bg-muted/30 p-3 text-sm sm:grid-cols-2">
                <Definition label={t({ en: "Current version", "zh-CN": "当前版本", ja: "現在のバージョン", ko: "현재 버전" })} value={snapshot.current_version} />
                <Definition label={t({ en: "Target version", "zh-CN": "目标版本", ja: "対象バージョン", ko: "대상 버전" })} value={snapshot.latest_version ?? "--"} />
              </div>
              <AlertDialogFooter>
                <AlertDialogCancel disabled={mutation === "apply"}>{t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}</AlertDialogCancel>
                <AlertDialogAction
                  disabled={!installEnabled || mutation === "apply"}
                  onClick={(event) => {
                    event.preventDefault();
                    void requestApply();
                  }}
                >
                  {mutation === "apply" ? (
                    <LoaderCircle className="animate-spin" aria-hidden="true" />
                  ) : (
                    <ShieldCheck aria-hidden="true" />
                  )}
                  {t({ en: "Install", "zh-CN": "确认安装", ja: "インストール", ko: "설치" })}
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </CardAction>
      </CardHeader>

      <CardContent className="space-y-5">
        <div className="grid gap-x-8 gap-y-4 sm:grid-cols-2 xl:grid-cols-4">
          <ReleaseVersion
            label={t({ en: "Current version", "zh-CN": "当前版本", ja: "現在のバージョン", ko: "현재 버전" })}
            version={snapshot.current_version}
            commit={snapshot.current_commit_sha}
            icon={<GitBranch className="size-4" aria-hidden="true" />}
          />
          <ReleaseVersion
            label={t({ en: "Latest version", "zh-CN": "最新版本", ja: "最新バージョン", ko: "최신 버전" })}
            version={snapshot.latest_version ?? t({ en: "Not checked", "zh-CN": "尚未检查", ja: "未確認", ko: "확인하지 않음" })}
            commit={snapshot.latest_commit_sha}
            icon={
              snapshot.latest_verified ? (
                <CheckCircle2 className="size-4" aria-hidden="true" />
              ) : (
                <GitBranch className="size-4" aria-hidden="true" />
              )
            }
          />
          <Definition
            label={t({ en: "Release verification", "zh-CN": "发布验证", ja: "リリース検証", ko: "릴리스 검증" })}
            value={
              <Badge variant={snapshot.latest_verified ? "outline" : "secondary"}>
                {snapshot.latest_verified
                  ? t({ en: "Verified", "zh-CN": "已验证", ja: "検証済み", ko: "검증됨" })
                  : t({ en: "Unverified", "zh-CN": "未验证", ja: "未検証", ko: "검증되지 않음" })}
              </Badge>
            }
          />
          <Definition
            label={t({ en: "Update status", "zh-CN": "更新状态", ja: "更新状態", ko: "업데이트 상태" })}
            value={
              <Badge variant={snapshot.update_available ? "default" : "outline"}>
                {releaseStateLabel(t, snapshot)}
              </Badge>
            }
          />
        </div>

        <div className="grid gap-3 border-t pt-4 text-sm sm:grid-cols-2 xl:grid-cols-4">
          <Definition label={t({ en: "Release repository", "zh-CN": "Release 仓库", ja: "Release リポジトリ", ko: "Release 저장소" })} value={snapshot.repository ?? t({ en: "Not configured", "zh-CN": "未配置", ja: "未設定", ko: "구성되지 않음" })} mono />
          <Definition label={t({ en: "Runtime target", "zh-CN": "运行目标", ja: "実行ターゲット", ko: "런타임 대상" })} value={snapshot.target_triple} mono />
          <Definition label={t({ en: "Last checked", "zh-CN": "最近检查", ja: "最終確認", ko: "최근 확인" })} value={formatDateTime(snapshot.last_checked_at_ms)} />
          <Definition label={t({ en: "Last installed", "zh-CN": "最近安装", ja: "最終インストール", ko: "최근 설치" })} value={formatDateTime(snapshot.last_applied_at_ms)} />
        </div>

        {!snapshot.configured ? (
          <Notice>
            {t({
              en: "No GitHub Release repository is configured. Set",
              "zh-CN": "尚未配置 GitHub Release 仓库。请设置服务端",
              ja: "GitHub Release リポジトリが設定されていません。サーバー側で",
              ko: "GitHub Release 저장소가 구성되지 않았습니다. 서버에서",
            })}
            <code className="mx-1 font-mono text-xs">AIF_UPDATE_GITHUB_REPOSITORY</code>
            {t({
              en: "on the server before checking for updates.",
              "zh-CN": "后才可检查更新。",
              ja: "を設定してから更新を確認してください。",
              ko: "을(를) 설정한 후 업데이트를 확인하세요.",
            })}
          </Notice>
        ) : null}
        {snapshot.configured && !snapshot.apply_enabled ? (
          <Notice>
            {t({
              en: "Update checks are available, but the automatic-install gate is not enabled.",
              "zh-CN": "当前仅开放更新检查；自动安装门禁尚未启用。",
              ja: "更新チェックは利用できますが、自動インストールのゲートは有効になっていません。",
              ko: "업데이트 확인은 가능하지만 자동 설치 게이트는 활성화되지 않았습니다.",
            })}
          </Notice>
        ) : null}
        {error || snapshot.last_error_message ? (
          <div className="flex gap-3 border border-destructive/30 bg-destructive/5 p-3 text-sm">
            <AlertCircle className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden="true" />
            <div className="min-w-0">
              <p className="font-medium text-destructive">{t({ en: "Update service error", "zh-CN": "更新服务异常", ja: "更新サービスエラー", ko: "업데이트 서비스 오류" })}</p>
              <p className="mt-1 break-words text-muted-foreground">
                {localizedSystemUpdateError(t, error ?? snapshot.last_error_message ?? "")}
              </p>
            </div>
          </div>
        ) : null}

        {snapshot.active_command ? <ActiveCommand command={snapshot.active_command} /> : null}

        <section className="space-y-3" aria-labelledby="recent-system-updates">
          <div>
            <h3 id="recent-system-updates" className="text-sm font-medium">{t({ en: "Recent commands", "zh-CN": "最近命令", ja: "最近のコマンド", ko: "최근 명령" })}</h3>
            <p className="text-xs text-muted-foreground">
              {t(
                { en: "The latest {count} check and install operations.", "zh-CN": "显示最近 {count} 次检查与安装操作。", ja: "直近 {count} 件の確認とインストール操作。", ko: "최근 확인 및 설치 작업 {count}개." },
                { count: RECENT_COMMAND_LIMIT },
              )}
            </p>
          </div>
          {recentCommands.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t({ en: "Action", "zh-CN": "操作", ja: "操作", ko: "작업" })}</TableHead>
                  <TableHead>{t({ en: "Version", "zh-CN": "版本", ja: "バージョン", ko: "버전" })}</TableHead>
                  <TableHead>{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</TableHead>
                  <TableHead>{t({ en: "Phase", "zh-CN": "阶段", ja: "フェーズ", ko: "단계" })}</TableHead>
                  <TableHead>{t({ en: "Submitted", "zh-CN": "提交时间", ja: "送信日時", ko: "제출 시간" })}</TableHead>
                  <TableHead className="text-right">{t({ en: "Result", "zh-CN": "结果", ja: "結果", ko: "결과" })}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {recentCommands.map((command) => (
                  <TableRow key={command.command_id}>
                    <TableCell className="font-medium">{actionLabel(t, command.action)}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {command.target_version ?? "--"}
                    </TableCell>
                    <TableCell><CommandStatus status={command.status} /></TableCell>
                    <TableCell>{phaseLabel(t, command.phase)}</TableCell>
                    <TableCell>{formatDateTime(command.requested_at_ms)}</TableCell>
                    <TableCell className="max-w-64 text-right text-muted-foreground">
                      <span className="block truncate" title={command.failure_message ?? undefined}>
                        {command.failure_message ?? formatDateTime(command.completed_at_ms)}
                      </span>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
              {t({ en: "No update commands yet", "zh-CN": "暂无更新命令", ja: "更新コマンドはまだありません", ko: "아직 업데이트 명령이 없습니다" })}
            </div>
          )}
        </section>
      </CardContent>
    </Card>
  );
}

function ActiveCommand({ command }: { command: SystemUpdateCommand }) {
  const { t } = useI18n();

  if (command.status === "restore_required") {
    return (
      <section
        className="flex gap-3 border border-destructive/30 bg-destructive/5 p-4"
        aria-live="assertive"
      >
        <AlertCircle
          className="mt-0.5 size-4 shrink-0 text-destructive"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <p className="text-sm font-medium text-destructive">{t({ en: "System updates are frozen", "zh-CN": "系统更新已冻结", ja: "システム更新は凍結されています", ko: "시스템 업데이트가 중지되었습니다" })}</p>
          <p className="mt-1 text-sm text-muted-foreground">
            {t({
              en: "Automatic recovery did not complete. New update commands are blocked until the recovery runbook is completed and the running version and database state are verified.",
              "zh-CN": "自动恢复未能完成。平台已阻断新的更新命令，请先按恢复手册处理并确认运行版本和数据库状态。",
              ja: "自動復旧を完了できませんでした。復旧手順を実行し、稼働バージョンとデータベース状態を確認するまで、新しい更新コマンドはブロックされます。",
              ko: "자동 복구가 완료되지 않았습니다. 복구 절차를 수행하고 실행 버전과 데이터베이스 상태를 확인할 때까지 새 업데이트 명령이 차단됩니다.",
            })}
          </p>
          {command.failure_message ? (
            <p className="mt-2 break-words font-mono text-xs text-muted-foreground">
              {command.failure_message}
            </p>
          ) : null}
        </div>
      </section>
    );
  }
  const progress = numericProgress(command.progress);
  return (
    <section className="space-y-3 border bg-muted/20 p-4" aria-live="polite">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium">
            {t(
              { en: "{action} in progress", "zh-CN": "{action}进行中", ja: "{action}を実行中", ko: "{action} 진행 중" },
              { action: actionLabel(t, command.action) },
            )}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            {t(
              { en: "{phase} · Updated {time}", "zh-CN": "{phase} · 更新于 {time}", ja: "{phase} · 更新日時 {time}", ko: "{phase} · 업데이트 {time}" },
              { phase: phaseLabel(t, command.phase), time: formatDateTime(command.updated_at_ms) },
            )}
          </p>
        </div>
        <CommandStatus status={command.status} />
      </div>
      <div
        className="h-1.5 overflow-hidden rounded-full bg-muted"
        role={progress === null ? "status" : "progressbar"}
        aria-label={t({ en: "System update progress", "zh-CN": "系统更新进度", ja: "システム更新の進捗", ko: "시스템 업데이트 진행률" })}
        aria-valuemin={progress === null ? undefined : 0}
        aria-valuemax={progress === null ? undefined : 100}
        aria-valuenow={progress ?? undefined}
      >
        <div
          className={`h-full rounded-full bg-foreground transition-[width] ${
            progress === null ? "w-1/3 animate-pulse" : ""
          }`}
          style={progress === null ? undefined : { width: `${progress}%` }}
        />
      </div>
    </section>
  );
}

function ReleaseVersion({
  label,
  version,
  commit,
  icon,
}: {
  label: string;
  version: string;
  commit: string | null;
  icon: React.ReactNode;
}) {
  return (
    <div className="min-w-0">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        {icon}
        {label}
      </div>
      <p className="mt-2 truncate text-lg font-semibold" title={version}>{version}</p>
      <p className="mt-1 truncate font-mono text-xs text-muted-foreground" title={commit ?? undefined}>
        {commit ? commit.slice(0, 12) : "--"}
      </p>
    </div>
  );
}

function Definition({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground">{label}</p>
      <div className={`mt-1.5 truncate text-sm ${mono ? "font-mono text-xs" : ""}`} title={typeof value === "string" ? value : undefined}>
        {value}
      </div>
    </div>
  );
}

function Notice({ children }: { children: React.ReactNode }) {
  return (
    <div className="border bg-muted/30 px-3 py-2.5 text-sm text-muted-foreground">
      {children}
    </div>
  );
}

function CommandStatus({ status }: { status: string }) {
  const { t } = useI18n();
  const variant =
    status === "failed" || status === "restore_required"
      ? "destructive"
      : status === "succeeded" || status === "restored"
        ? "outline"
        : "secondary";
  return <Badge variant={variant}>{statusLabel(t, status)}</Badge>;
}

function actionLabel(t: Translate, action: string) {
  return action === "apply"
    ? t({ en: "Install update", "zh-CN": "安装更新", ja: "更新をインストール", ko: "업데이트 설치" })
    : action === "check"
      ? t({ en: "Check for updates", "zh-CN": "检查更新", ja: "更新を確認", ko: "업데이트 확인" })
      : action;
}

function statusLabel(t: Translate, status: string) {
  const labels: Record<string, Parameters<Translate>[0]> = {
    queued: { en: "Queued", "zh-CN": "排队中", ja: "キュー待ち", ko: "대기열에 있음" },
    running: { en: "Running", "zh-CN": "执行中", ja: "実行中", ko: "실행 중" },
    succeeded: { en: "Completed", "zh-CN": "已完成", ja: "完了", ko: "완료" },
    failed: { en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" },
    restoring: { en: "Restoring", "zh-CN": "恢复中", ja: "復旧中", ko: "복구 중" },
    restored: { en: "Restored", "zh-CN": "已恢复", ja: "復旧済み", ko: "복구됨" },
    restore_required: { en: "Recovery required", "zh-CN": "需要恢复", ja: "復旧が必要", ko: "복구 필요" },
  };
  return labels[status] ? t(labels[status]) : status;
}

function phaseLabel(t: Translate, phase: string) {
  const labels: Record<string, Parameters<Translate>[0]> = {
    queued: { en: "Awaiting execution", "zh-CN": "等待执行", ja: "実行待ち", ko: "실행 대기" },
    preflight: { en: "Release preflight", "zh-CN": "发布预检", ja: "リリース事前検証", ko: "릴리스 사전 점검" },
    staged: { en: "Artifact staged", "zh-CN": "制品已暂存", ja: "成果物をステージ済み", ko: "아티팩트 스테이징됨" },
    quiescing: { en: "Draining traffic", "zh-CN": "正在排空流量", ja: "トラフィックをドレイン中", ko: "트래픽 드레이닝 중" },
    quiesced: { en: "Traffic drained", "zh-CN": "流量已排空", ja: "トラフィックのドレイン完了", ko: "트래픽 드레이닝 완료" },
    recovery_ready: { en: "Recovery point ready", "zh-CN": "恢复点已就绪", ja: "復旧ポイント準備完了", ko: "복구 지점 준비됨" },
    migrated: { en: "Database migrated", "zh-CN": "数据库已迁移", ja: "データベース移行済み", ko: "데이터베이스 마이그레이션됨" },
    switched: { en: "Version switched", "zh-CN": "版本已切换", ja: "バージョン切替済み", ko: "버전 전환됨" },
    verified: { en: "Health verification", "zh-CN": "健康验证", ja: "ヘルス検証", ko: "상태 검증" },
    restoring: { en: "Restoring", "zh-CN": "正在恢复", ja: "復旧中", ko: "복구 중" },
    restored: { en: "Recovery complete", "zh-CN": "恢复完成", ja: "復旧完了", ko: "복구 완료" },
    failed: { en: "Execution failed", "zh-CN": "执行失败", ja: "実行失敗", ko: "실행 실패" },
  };
  return labels[phase] ? t(labels[phase]) : phase;
}

function releaseStateLabel(t: Translate, snapshot: SystemUpdateSnapshot) {
  if (!snapshot.latest_version) return t({ en: "Not checked", "zh-CN": "待检查", ja: "未確認", ko: "확인 필요" });
  if (!snapshot.latest_verified) return t({ en: "Awaiting verification", "zh-CN": "等待验证", ja: "検証待ち", ko: "검증 대기" });
  return snapshot.update_available
    ? t({ en: "Update available", "zh-CN": "有可用更新", ja: "更新あり", ko: "업데이트 가능" })
    : t({ en: "Up to date", "zh-CN": "已是最新", ja: "最新です", ko: "최신 상태" });
}

function numericProgress(progress: Record<string, unknown>) {
  const value = progress.percent;
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.min(100, Math.max(0, value));
}

function isAbortError(reason: unknown) {
  return reason instanceof DOMException && reason.name === "AbortError";
}

function localizedSystemUpdateError(t: Translate, message: string) {
  switch (message) {
    case "This account cannot manage system updates":
      return t({
        en: message,
        "zh-CN": "当前账户无权管理系统更新",
        ja: "このアカウントにはシステム更新を管理する権限がありません",
        ko: "이 계정에는 시스템 업데이트 관리 권한이 없습니다",
      });
    case "A system update command is already running":
      return t({
        en: message,
        "zh-CN": "已有系统更新命令正在执行",
        ja: "システム更新コマンドがすでに実行中です",
        ko: "시스템 업데이트 명령이 이미 실행 중입니다",
      });
    case "The system update service is temporarily unavailable":
      return t({
        en: message,
        "zh-CN": "系统更新服务暂时不可用",
        ja: "システム更新サービスは一時的に利用できません",
        ko: "시스템 업데이트 서비스를 일시적으로 사용할 수 없습니다",
      });
    default:
      return message;
  }
}
