"use client";

import { Copy, FileText, ReceiptText, ShieldCheck, TriangleAlert } from "lucide-react";
import { toast } from "sonner";
import {
  EconomicsDetails,
  type EconomicsSnapshot,
} from "@/components/activity-job-sheet";
import {
  ActivityStatusBadge,
  formatActivityOperation,
  formatActivityStatus,
} from "@/components/activity-status-badge";
import {
  AdminQueryError,
  AdminQuerySkeleton,
} from "@/components/admin-query-state";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { useAdminQuery } from "@/hooks/use-admin-query";
import {
  formatDateTime,
  formatDurationMs,
  formatInteger,
} from "@/lib/admin/format";
import type { RequestLogItem } from "@/lib/admin/types";
import { useI18n } from "@/i18n/locale-provider";

type Translate = ReturnType<typeof useI18n>["t"];

export function ActivityRequestSheet({
  item,
  economicsPath,
  onOpenChange,
}: {
  item: RequestLogItem | null;
  economicsPath: string | null;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-2xl lg:max-w-3xl">
        {item ? (
          <RequestDetails item={item} economicsPath={economicsPath} />
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function RequestDetails({
  item,
  economicsPath,
}: {
  item: RequestLogItem;
  economicsPath: string | null;
}) {
  const { t } = useI18n();
  const economics = useAdminQuery<EconomicsSnapshot>(
    economicsPath ?? "",
    Boolean(economicsPath),
  );
  const state = requestState(item);

  return (
    <>
      <SheetHeader className="border-b px-5 py-5 pr-12 text-left sm:px-6">
        <div className="flex flex-wrap items-center gap-2">
          <ActivityStatusBadge state={state} />
          <Badge variant="outline">{localizedSourceLabel(t, item.source)}</Badge>
          <Badge variant="secondary">{item.status_code}</Badge>
        </div>
        <SheetTitle className="pt-1 text-xl">
          {t({
            en: "Request details",
            "zh-CN": "请求详情",
            ja: "リクエスト詳細",
            ko: "요청 상세",
          })}
        </SheetTitle>
        <SheetDescription className="break-all font-mono text-xs">
          {item.request_id}
        </SheetDescription>
      </SheetHeader>

      <Tabs defaultValue="request" className="flex min-h-0 flex-1 flex-col">
        <div className="shrink-0 overflow-x-auto border-b px-5 sm:px-6">
          <TabsList variant="line">
            <TabsTrigger value="request" variant="line">
              <FileText className="size-4" aria-hidden="true" />
              {t({
                en: "Request",
                "zh-CN": "请求",
                ja: "リクエスト",
                ko: "요청",
              })}
            </TabsTrigger>
            <TabsTrigger value="economics" variant="line" disabled={!item.job_id}>
              <ReceiptText className="size-4" aria-hidden="true" />
              {t({
                en: "Billing",
                "zh-CN": "计费",
                ja: "請求",
                ko: "청구",
              })}
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent
          value="request"
          className="m-0 min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6"
        >
          {item.error_code ? (
            <div className="mb-7 flex gap-3 border border-destructive/30 bg-destructive/5 p-4 text-sm">
              <TriangleAlert
                className="mt-0.5 size-4 shrink-0 text-destructive"
                aria-hidden="true"
              />
              <div className="min-w-0">
                <p className="font-medium text-destructive">
                  {t({
                    en: "The request did not complete successfully",
                    "zh-CN": "请求未成功完成",
                    ja: "リクエストは正常に完了しませんでした",
                    ko: "요청이 정상적으로 완료되지 않았습니다",
                  })}
                </p>
                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                  {item.error_code}
                </p>
              </div>
            </div>
          ) : null}

          <div className="space-y-8">
            <DetailSection
              title={t({
                en: "Overview",
                "zh-CN": "概览",
                ja: "概要",
                ko: "개요",
              })}
            >
              <div className="grid gap-x-8 gap-y-5 sm:grid-cols-2">
                <DetailItem
                  label={t({
                    en: "Status",
                    "zh-CN": "状态",
                    ja: "状態",
                    ko: "상태",
                  })}
                >
                  <ActivityStatusBadge state={state} />
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Duration",
                    "zh-CN": "耗时",
                    ja: "所要時間",
                    ko: "소요 시간",
                  })}
                >
                  {formatDurationMs(item.duration_ms)}
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "API",
                    "zh-CN": "API",
                    ja: "API",
                    ko: "API",
                  })}
                >
                  <span className="font-mono text-xs">
                    {item.method} {item.route_pattern}
                  </span>
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "HTTP status",
                    "zh-CN": "HTTP 状态",
                    ja: "HTTP ステータス",
                    ko: "HTTP 상태",
                  })}
                >
                  {item.status_code}
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Started",
                    "zh-CN": "开始时间",
                    ja: "開始時刻",
                    ko: "시작 시간",
                  })}
                >
                  {formatDateTime(item.created_at_ms)}
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Completed",
                    "zh-CN": "完成时间",
                    ja: "完了時刻",
                    ko: "완료 시간",
                  })}
                >
                  {formatDateTime(item.completed_at_ms)}
                </DetailItem>
              </div>
            </DetailSection>

            <DetailSection
              title={t({
                en: "Identifiers",
                "zh-CN": "标识",
                ja: "識別子",
                ko: "식별자",
              })}
            >
              <DetailRows>
                <CopyRow
                  label={t({
                    en: "Request ID",
                    "zh-CN": "请求 ID",
                    ja: "リクエスト ID",
                    ko: "요청 ID",
                  })}
                  value={item.request_id}
                />
                <CopyRow
                  label={t({
                    en: "Job ID",
                    "zh-CN": "任务 ID",
                    ja: "ジョブ ID",
                    ko: "작업 ID",
                  })}
                  value={item.job_id}
                />
                <CopyRow
                  label={t({
                    en: "Idempotency fingerprint",
                    "zh-CN": "Idempotency 指纹",
                    ja: "冪等性フィンガープリント",
                    ko: "멱등성 지문",
                  })}
                  value={item.idempotency_key_digest}
                />
                <DetailRow
                  label={t({
                    en: "Request path",
                    "zh-CN": "请求路径",
                    ja: "リクエストパス",
                    ko: "요청 경로",
                  })}
                >
                  <span className="break-all font-mono text-xs">
                    {item.request_path}
                  </span>
                </DetailRow>
              </DetailRows>
            </DetailSection>

            <DetailSection
              title={t({
                en: "Request ownership",
                "zh-CN": "调用归属",
                ja: "リクエスト所有者",
                ko: "요청 소유권",
              })}
            >
              <DetailRows>
                <DetailRow
                  label={t({
                    en: "Project ID",
                    "zh-CN": "项目 ID",
                    ja: "プロジェクト ID",
                    ko: "프로젝트 ID",
                  })}
                >
                  {item.project_id ??
                    t({
                      en: "No project linked",
                      "zh-CN": "未关联项目",
                      ja: "プロジェクト未関連付け",
                      ko: "연결된 프로젝트 없음",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "API Key ID",
                    "zh-CN": "API 密钥 ID",
                    ja: "API キー ID",
                    ko: "API 키 ID",
                  })}
                >
                  {item.api_key_id ??
                    t({
                      en: "No API Key used",
                      "zh-CN": "未使用 API Key",
                      ja: "API Key 未使用",
                      ko: "API Key 미사용",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Service Account",
                    "zh-CN": "服务账户",
                    ja: "サービスアカウント",
                    ko: "서비스 계정",
                  })}
                >
                  {item.service_account_id ??
                    t({
                      en: "No service account used",
                      "zh-CN": "未使用服务账户",
                      ja: "サービスアカウント未使用",
                      ko: "서비스 계정 미사용",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "User",
                    "zh-CN": "用户",
                    ja: "ユーザー",
                    ko: "사용자",
                  })}
                >
                  {item.actor_user_id ??
                    t({
                      en: "Non-user session",
                      "zh-CN": "非用户会话",
                      ja: "ユーザー以外のセッション",
                      ko: "비사용자 세션",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Authentication",
                    "zh-CN": "认证方式",
                    ja: "認証方式",
                    ko: "인증 방식",
                  })}
                >
                  {item.auth_kind
                    ? formatActivityStatus(t, item.auth_kind)
                    : t({
                        en: "Unrecognized",
                        "zh-CN": "未识别",
                        ja: "未認識",
                        ko: "인식되지 않음",
                      })}
                </DetailRow>
              </DetailRows>
            </DetailSection>

            {item.job_id ? (
              <DetailSection
                title={t({
                  en: "Execution",
                  "zh-CN": "执行",
                  ja: "実行",
                  ko: "실행",
                })}
              >
                <DetailRows>
                  <DetailRow
                    label={t({
                      en: "Operation",
                      "zh-CN": "操作",
                      ja: "操作",
                      ko: "작업",
                    })}
                  >
                    {item.operation
                      ? formatActivityOperation(t, item.operation)
                      : t({
                          en: "Not recorded",
                          "zh-CN": "未记录",
                          ja: "記録なし",
                          ko: "기록되지 않음",
                        })}
                  </DetailRow>
                  <DetailRow
                    label={t({
                      en: "Provider",
                      "zh-CN": "供应商",
                      ja: "プロバイダー",
                      ko: "공급자",
                    })}
                  >
                    {item.provider_id ??
                      t({
                        en: "Not selected",
                        "zh-CN": "尚未选择",
                        ja: "未選択",
                        ko: "선택되지 않음",
                      })}
                  </DetailRow>
                  <DetailRow
                    label={t({
                      en: "Model",
                      "zh-CN": "模型",
                      ja: "モデル",
                      ko: "모델",
                    })}
                  >
                    {item.model ? (
                      <span className="break-all font-mono text-xs">
                        {item.model}
                      </span>
                    ) : (
                      t({
                        en: "Not selected",
                        "zh-CN": "尚未选择",
                        ja: "未選択",
                        ko: "선택되지 않음",
                      })
                    )}
                  </DetailRow>
                  <DetailRow
                    label={t({
                      en: "Service tier",
                      "zh-CN": "服务层级",
                      ja: "サービス階層",
                      ko: "서비스 등급",
                    })}
                  >
                    {item.effective_service_tier ? (
                      <span>
                        {serviceTierLabel(item.effective_service_tier)}
                        {item.requested_service_tier ? (
                          <span className="ml-2 text-muted-foreground">
                            {t(
                              {
                                en: "Requested {tier}",
                                "zh-CN": "请求 {tier}",
                                ja: "リクエスト: {tier}",
                                ko: "요청됨: {tier}",
                              },
                              {
                                tier: serviceTierLabel(
                                  item.requested_service_tier,
                                ),
                              },
                            )}
                          </span>
                        ) : null}
                      </span>
                    ) : (
                      t({
                        en: "Not recorded",
                        "zh-CN": "未记录",
                        ja: "記録なし",
                        ko: "기록되지 않음",
                      })
                    )}
                  </DetailRow>
                  {item.service_tier_fallback_reason ? (
                    <DetailRow
                      label={t({
                        en: "Tier fallback",
                        "zh-CN": "层级回退",
                        ja: "階層フォールバック",
                        ko: "등급 폴백",
                      })}
                    >
                      {t(
                        {
                          en: "This model does not support the project's {tier} tier. The request was executed and billed at Default.",
                          "zh-CN":
                            "当前模型不支持项目选择的 {tier}，已按 Default 执行和计费。",
                          ja: "このモデルはプロジェクトで選択された {tier} 階層に対応していないため、Default で実行および請求されました。",
                          ko: "이 모델은 프로젝트에서 선택한 {tier} 등급을 지원하지 않아 Default로 실행 및 청구되었습니다.",
                        },
                        {
                          tier: serviceTierLabel(
                            item.project_service_tier ?? "default",
                          ),
                        },
                      )}
                    </DetailRow>
                  ) : null}
                  <DetailRow
                    label={t({
                      en: "Job status",
                      "zh-CN": "任务状态",
                      ja: "ジョブ状態",
                      ko: "작업 상태",
                    })}
                  >
                    {item.job_state
                      ? formatActivityStatus(t, item.job_state)
                      : t({
                          en: "No job created",
                          "zh-CN": "未创建任务",
                          ja: "ジョブ未作成",
                          ko: "생성된 작업 없음",
                        })}
                  </DetailRow>
                  <DetailRow
                    label={t({
                      en: "Queue status",
                      "zh-CN": "队列状态",
                      ja: "キュー状態",
                      ko: "대기열 상태",
                    })}
                  >
                    {item.work_state
                      ? formatActivityStatus(t, item.work_state)
                      : t({
                          en: "No work item created",
                          "zh-CN": "未创建工作项",
                          ja: "作業項目未作成",
                          ko: "생성된 작업 항목 없음",
                        })}
                  </DetailRow>
                  <DetailRow
                    label={t({
                      en: "Output / metering",
                      "zh-CN": "输出 / 计量",
                      ja: "出力 / 計測",
                      ko: "출력 / 계측",
                    })}
                  >
                    {item.output_count !== null
                      ? `${formatInteger(item.output_count)} / ${formatInteger(
                          item.billable_units ?? "0",
                        )} ${item.billing_unit ?? ""}`
                      : t({
                          en: "No terminal metering yet",
                          "zh-CN": "尚无终态计量",
                          ja: "最終計測はまだありません",
                          ko: "아직 최종 계측 없음",
                        })}
                  </DetailRow>
                </DetailRows>
              </DetailSection>
            ) : null}

            <DetailSection
              title={t({
                en: "Request content",
                "zh-CN": "请求内容",
                ja: "リクエスト内容",
                ko: "요청 내용",
              })}
            >
              <div className="flex gap-3 bg-muted/40 p-4 text-sm">
                <ShieldCheck
                  className="mt-0.5 size-4 shrink-0 text-muted-foreground"
                  aria-hidden="true"
                />
                <div>
                  <p className="font-medium">
                    {item.content_captured
                      ? t({
                          en: "Request content is stored under the retention policy",
                          "zh-CN": "请求内容按保留策略受控存储",
                          ja: "リクエスト内容は保持ポリシーに従って管理保存されています",
                          ko: "요청 내용은 보존 정책에 따라 관리 저장됩니다",
                        })
                      : t({
                          en: "Request content was not captured",
                          "zh-CN": "未采集请求内容",
                          ja: "リクエスト内容は取得されていません",
                          ko: "요청 내용을 수집하지 않음",
                        })}
                  </p>
                  <p className="mt-1 text-muted-foreground">
                    {t({
                      en: "This log records only the route, status, duration, and request ownership. Prompts, input images, and generated outputs are not stored.",
                      "zh-CN":
                        "当前日志只记录路由、状态、耗时和调用归属，不保存提示词、输入图片或生成结果。",
                      ja: "このログにはルート、状態、所要時間、リクエスト所有者のみが記録され、プロンプト、入力画像、生成結果は保存されません。",
                      ko: "이 로그에는 경로, 상태, 소요 시간 및 요청 소유권만 기록되며 프롬프트, 입력 이미지 또는 생성 결과는 저장되지 않습니다.",
                    })}
                  </p>
                </div>
              </div>
            </DetailSection>
          </div>
        </TabsContent>

        <TabsContent
          value="economics"
          className="m-0 min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6"
        >
          {economics.loading ? <AdminQuerySkeleton rows={7} /> : null}
          {!economics.loading && economics.error && !economics.data ? (
            <AdminQueryError error={economics.error} retry={economics.retry} />
          ) : null}
          {economics.data ? (
            <EconomicsDetails
              snapshot={economics.data}
              stale={Boolean(economics.error)}
            />
          ) : null}
        </TabsContent>
      </Tabs>
    </>
  );
}

function DetailSection({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section>
      <h3 className="mb-4 text-sm font-semibold">{title}</h3>
      {children}
    </section>
  );
}

function serviceTierLabel(value: string): string {
  const labels: Record<string, string> = {
    auto: "Auto",
    default: "Default",
    flex: "Flex",
    priority: "Priority",
    standard: "Default",
  };
  return labels[value] ?? value;
}

function DetailRows({ children }: { children: React.ReactNode }) {
  return <dl className="space-y-4">{children}</dl>;
}

function DetailRow({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid gap-1 text-sm sm:grid-cols-[150px_minmax(0,1fr)] sm:gap-6">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0 sm:text-right">{children}</dd>
    </div>
  );
}

function CopyRow({
  label,
  value,
}: {
  label: string;
  value: string | null;
}) {
  const { t } = useI18n();

  return (
    <DetailRow label={label}>
      {value ? (
        <span className="inline-flex max-w-full items-center gap-1">
          <span className="truncate font-mono text-xs">{value}</span>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7 shrink-0"
            aria-label={t(
              {
                en: "Copy {label}",
                "zh-CN": "复制 {label}",
                ja: "{label} をコピー",
                ko: "{label} 복사",
              },
              { label },
            )}
            title={t(
              {
                en: "Copy {label}",
                "zh-CN": "复制 {label}",
                ja: "{label} をコピー",
                ko: "{label} 복사",
              },
              { label },
            )}
            onClick={() => {
              void navigator.clipboard.writeText(value);
              toast.success(
                t(
                  {
                    en: "{label} copied",
                    "zh-CN": "{label} 已复制",
                    ja: "{label} をコピーしました",
                    ko: "{label} 복사됨",
                  },
                  { label },
                ),
              );
            }}
          >
            <Copy className="size-3.5" aria-hidden="true" />
          </Button>
        </span>
      ) : (
        t({
          en: "Not generated",
          "zh-CN": "未生成",
          ja: "未生成",
          ko: "생성되지 않음",
        })
      )}
    </DetailRow>
  );
}

function DetailItem({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <div className="mt-1.5 text-sm font-medium">{children}</div>
    </div>
  );
}

export function requestState(item: RequestLogItem): string {
  if (
    item.status_code >= 400 ||
    item.job_state === "failed" ||
    item.job_state === "uncertain"
  ) {
    return "failed";
  }
  if (
    item.status_code === 202 ||
    item.job_state === "queued" ||
    item.job_state === "running"
  ) {
    return "running";
  }
  return "succeeded";
}

export function sourceLabel(source: RequestLogItem["source"]): string {
  const labels: Record<RequestLogItem["source"], string> = {
    models: "Models",
    images: "Images",
    videos: "Videos",
    files: "Files",
    batches: "Batches",
  };
  return labels[source];
}

function localizedSourceLabel(
  t: Translate,
  source: RequestLogItem["source"],
): string {
  const labels: Record<
    RequestLogItem["source"],
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    models: { en: "Models", "zh-CN": "模型", ja: "モデル", ko: "모델" },
    images: { en: "Images", "zh-CN": "图片", ja: "画像", ko: "이미지" },
    videos: { en: "Videos", "zh-CN": "视频", ja: "動画", ko: "동영상" },
    files: { en: "Files", "zh-CN": "文件", ja: "ファイル", ko: "파일" },
    batches: { en: "Batches", "zh-CN": "批处理", ja: "バッチ", ko: "배치" },
  };
  return t(labels[source]);
}
