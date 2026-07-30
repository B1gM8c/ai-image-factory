"use client";

import { useEffect, useMemo, useState } from "react";
import { Plus, Save, Trash2 } from "lucide-react";
import { toast } from "sonner";
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
import { Textarea } from "@/components/ui/textarea";
import { useI18n } from "@/i18n/locale-provider";
import { consoleFetch } from "@/lib/auth/client";
import type {
  PriceBook,
  PriceBookVersion,
  PriceBookVersionDraft,
  PriceComponentDraft,
  PricingCoverageRow,
} from "@/lib/admin/types";

type Translate = ReturnType<typeof useI18n>["t"];

type EditableComponent = {
  component_key: string;
  metric: string;
  unit_size: string;
  unit_price: string;
  outcome: string;
  quantity_source: string;
  required_confidence: string;
  rounding_mode: string;
  dimensions: string;
};

type VersionForm = {
  api_profile: string;
  operation: string;
  provider_id: string;
  provider_model_id: string;
  public_model_id: string;
  media_kind: "image" | "video";
  service_tier: "standard" | "flex" | "priority";
  execution_surface: PriceBookVersion["execution_surface"];
  billing_mode: PriceBookVersion["billing_mode"];
  is_free: boolean;
  effective_from: string;
  source_kind: PriceBookVersion["source_kind"];
  source_url: string;
  source_checked_at: string;
  notes: string;
  components: EditableComponent[];
};

const METRICS = [
  "request",
  "image_input",
  "image_output",
  "text_input_token",
  "cached_text_input_token",
  "image_input_token",
  "cached_image_input_token",
  "image_output_token",
  "video_input_token",
  "video_output_token",
  "video_input_second",
  "video_requested_second",
  "video_output_second",
  "membership_point",
] as const;

const BILLING_MODES: Record<PriceBook["purpose"], PriceBookVersion["billing_mode"][]> = {
  customer_sale: ["customer_rate"],
  provider_actual: ["provider_reported", "contract_rate"],
  provider_estimated: ["published_rate", "contract_rate"],
  provider_allocated: ["subscription_allocation", "membership_points"],
  provider_benchmark: ["published_rate"],
};

export function PriceVersionDialog({
  book,
  version,
  preset = null,
  open,
  onOpenChange,
  onSaved,
}: {
  book: PriceBook | null;
  version: PriceBookVersion | null;
  preset?: PricingCoverageRow | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSaved: (version: PriceBookVersion) => void;
}) {
  const { t } = useI18n();
  const [form, setForm] = useState<VersionForm>(() => emptyForm(book, preset));
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) setForm(version ? formFromVersion(version) : emptyForm(book, preset));
  }, [book, open, preset, version]);

  const providerReported = form.billing_mode === "provider_reported";
  const officialSource =
    form.source_kind === "official_document" || form.source_kind === "provider_contract";
  const title = version
    ? t({
      en: "Edit pricing draft",
      "zh-CN": "编辑价格草稿",
      ja: "価格下書きを編集",
      ko: "가격 초안 편집",
    })
    : t({
      en: "Add model pricing",
      "zh-CN": "添加模型价格",
      ja: "モデル価格を追加",
      ko: "모델 가격 추가",
    });
  const identityLocked = Boolean(preset && !version);

  const validation = useMemo(() => validateForm(form, book, t), [book, form, t]);

  if (!book) return null;
  const activeBook = book;

  async function save() {
    if (validation) {
      toast.error(validation);
      return;
    }

    const draft = buildDraft(form);
    const path = version
      ? `/api/gateway/admin/v1/pricing/price-book-versions/${version.price_book_version_id}`
      : `/api/gateway/admin/v1/pricing/price-books/${activeBook.price_book_id}/versions`;
    const body = version
      ? { expected_control_version: Number(version.control_version), ...draft }
      : draft;

    setSaving(true);
    try {
      const response = await consoleFetch(path, {
        method: version ? "PUT" : "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to save pricing draft",
          "zh-CN": "保存价格草稿失败",
          ja: "価格下書きを保存できませんでした",
          ko: "가격 초안을 저장하지 못했습니다",
        })));
      }
      const saved = (await response.json()) as PriceBookVersion;
      toast.success(version
        ? t({
          en: "Pricing draft saved",
          "zh-CN": "价格草稿已保存",
          ja: "価格下書きを保存しました",
          ko: "가격 초안을 저장했습니다",
        })
        : t({
          en: "Model pricing draft created",
          "zh-CN": "模型价格草稿已创建",
          ja: "モデル価格の下書きを作成しました",
          ko: "모델 가격 초안을 만들었습니다",
        }));
      onSaved(saved);
      onOpenChange(false);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t({
        en: "Failed to save pricing draft",
        "zh-CN": "保存价格草稿失败",
        ja: "価格下書きを保存できませんでした",
        ko: "가격 초안을 저장하지 못했습니다",
      }));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[92vh] max-w-4xl overflow-y-auto p-0">
        <DialogHeader className="border-b px-6 py-5">
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>
            {t({
              en: "{name} · {currency}. Saved changes remain a draft and can be edited until published.",
              "zh-CN": "{name} · {currency}。保存后仍是草稿，发布前可以继续修改。",
              ja: "{name} · {currency}。保存後も下書きのままで、公開するまで編集できます。",
              ko: "{name} · {currency}. 저장 후에도 초안으로 유지되며 게시 전까지 수정할 수 있습니다.",
            }, { name: book.display_name, currency: book.currency })}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 px-6 py-1">
          <section className="space-y-4">
            <SectionTitle
              title={t({ en: "Model and API", "zh-CN": "模型与接口", ja: "モデルと API", ko: "모델 및 API" })}
              description={identityLocked
                ? t({
                  en: "The model identity comes from an enabled platform route. Internal contract fields are locked by the system.",
                  "zh-CN": "模型身份来自已启用的平台路由，内部契约字段已由系统锁定。",
                  ja: "モデル ID は有効なプラットフォームルートから取得され、内部契約フィールドはシステムによってロックされています。",
                  ko: "모델 ID는 활성화된 플랫폼 경로에서 가져오며 내부 계약 필드는 시스템에서 잠급니다.",
                })
                : t({
                  en: "The public model ID is exposed to API users. The provider model ID is used for execution and cost attribution.",
                  "zh-CN": "外部模型 ID 面向 API 用户，原生模型 ID 用于供应商执行与成本归因。",
                  ja: "公開モデル ID は API ユーザー向けです。プロバイダーモデル ID は実行とコスト帰属に使用されます。",
                  ko: "외부 모델 ID는 API 사용자에게 노출되고, 제공업체 모델 ID는 실행 및 비용 귀속에 사용됩니다.",
                })}
            />
            <div className="grid gap-4 md:grid-cols-2">
              <Field label={t({ en: "Public model ID", "zh-CN": "外部模型 ID", ja: "公開モデル ID", ko: "외부 모델 ID" })}>
                <Input
                  value={form.public_model_id}
                  disabled={identityLocked}
                  placeholder={t({ en: "e.g. gpt-image-2", "zh-CN": "例如 gpt-image-2", ja: "例: gpt-image-2", ko: "예: gpt-image-2" })}
                  onChange={(event) => setField(setForm, "public_model_id", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Provider model ID", "zh-CN": "供应商原生模型 ID", ja: "プロバイダーモデル ID", ko: "제공업체 모델 ID" })}>
                <Input
                  value={form.provider_model_id}
                  disabled={identityLocked}
                  placeholder={t({
                    en: "Leave blank if there is no separate provider model ID",
                    "zh-CN": "没有独立原生 ID 时可留空",
                    ja: "個別のプロバイダーモデル ID がない場合は空欄にできます",
                    ko: "별도의 제공업체 모델 ID가 없으면 비워 둘 수 있습니다",
                  })}
                  onChange={(event) => setField(setForm, "provider_model_id", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "API protocol", "zh-CN": "API 协议", ja: "API プロトコル", ko: "API 프로토콜" })}>
                <Input
                  value={form.api_profile}
                  disabled={identityLocked}
                  placeholder={t({ en: "e.g. openai.images.v1", "zh-CN": "例如 openai.images.v1", ja: "例: openai.images.v1", ko: "예: openai.images.v1" })}
                  onChange={(event) => setField(setForm, "api_profile", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Operation", "zh-CN": "操作", ja: "操作", ko: "작업" })}>
                <Input
                  value={form.operation}
                  disabled={identityLocked}
                  placeholder={t({ en: "e.g. generation", "zh-CN": "例如 generation", ja: "例: generation", ko: "예: generation" })}
                  onChange={(event) => setField(setForm, "operation", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Media type", "zh-CN": "媒体类型", ja: "メディア種類", ko: "미디어 유형" })}>
                <Select
                  value={form.media_kind}
                  disabled={identityLocked}
                  onValueChange={(value: "image" | "video") => {
                    setForm((current) => ({
                      ...current,
                      media_kind: value,
                      operation: value === "video" ? "video_generation" : "generation",
                      components: defaultComponents(value, book.purpose, preset),
                    }));
                  }}
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="image">{t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })}</SelectItem>
                    <SelectItem value="video">{t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label={t({ en: "Service tier", "zh-CN": "服务层级", ja: "サービス階層", ko: "서비스 등급" })}>
                <Select
                  value={form.service_tier}
                  disabled={identityLocked}
                  onValueChange={(value: VersionForm["service_tier"]) =>
                    setField(setForm, "service_tier", value)
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="standard">{t({ en: "Default", "zh-CN": "默认", ja: "デフォルト", ko: "기본" })}</SelectItem>
                    <SelectItem value="flex">Flex</SelectItem>
                    <SelectItem value="priority">Priority</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            </div>
          </section>

          <section className="space-y-4 border-t pt-6">
            <SectionTitle
              title={t({ en: "Pricing method", "zh-CN": "计价方式", ja: "価格設定方法", ko: "가격 책정 방식" })}
              description={t({
                en: "Each metered item is stored in integer micros. Enter decimal prices in the current currency.",
                "zh-CN": "每个计量项使用整数微单位存储，界面按当前币种输入十进制单价。",
                ja: "各計量項目は整数のマイクロ単位で保存されます。現在の通貨で小数価格を入力してください。",
                ko: "각 계량 항목은 정수 마이크로 단위로 저장됩니다. 현재 통화 기준의 소수 가격을 입력하세요.",
              })}
            />
            <div className="grid gap-4 md:grid-cols-3">
              <Field label={t({ en: "Billing mode", "zh-CN": "计费模式", ja: "課金モード", ko: "과금 방식" })}>
                <Select
                  value={form.billing_mode}
                  onValueChange={(value: PriceBookVersion["billing_mode"]) =>
                    setForm((current) => ({
                      ...current,
                      billing_mode: value,
                      components: value === "provider_reported"
                        ? []
                        : current.components.length > 0
                          ? current.components
                          : defaultComponents(current.media_kind, book.purpose, preset),
                    }))
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {BILLING_MODES[book.purpose].map((mode) => (
                      <SelectItem key={mode} value={mode}>{billingModeLabel(mode, t)}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
              <Field label={t({ en: "Execution channel", "zh-CN": "执行渠道", ja: "実行チャネル", ko: "실행 채널" })}>
                <Select
                  value={form.execution_surface}
                  disabled={identityLocked}
                  onValueChange={(value: PriceBookVersion["execution_surface"]) =>
                    setField(setForm, "execution_surface", value)
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="provider_cli">{t({ en: "Provider CLI", "zh-CN": "供应商 CLI", ja: "プロバイダー CLI", ko: "제공업체 CLI" })}</SelectItem>
                    <SelectItem value="provider_api">{t({ en: "Provider API", "zh-CN": "供应商 API", ja: "プロバイダー API", ko: "제공업체 API" })}</SelectItem>
                    <SelectItem value="manual_import">{t({ en: "Manual import", "zh-CN": "人工导入", ja: "手動インポート", ko: "수동 가져오기" })}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label={t({ en: "Charge status", "zh-CN": "收费状态", ja: "課金状態", ko: "과금 상태" })}>
                <Select
                  value={form.is_free ? "free" : "paid"}
                  disabled={providerReported}
                  onValueChange={(value) =>
                    setForm((current) => ({
                      ...current,
                      is_free: value === "free",
                      components: value === "free"
                        ? current.components.map((item) => ({ ...item, unit_price: "0" }))
                        : current.components,
                    }))
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="paid">{t({ en: "Paid", "zh-CN": "付费", ja: "有料", ko: "유료" })}</SelectItem>
                    <SelectItem value="free">{t({ en: "Free", "zh-CN": "免费", ja: "無料", ko: "무료" })}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            </div>

            {providerReported ? (
              <div className="rounded-md border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
                {t({
                  en: "Provider-reported billing uses the native amount returned upstream and does not configure static prices.",
                  "zh-CN": "供应商回执模式直接采用上游返回的原生金额，不配置静态单价。",
                  ja: "プロバイダー報告モードでは上流から返された金額をそのまま使用し、固定単価は設定しません。",
                  ko: "제공업체 보고 방식은 상위 서비스가 반환한 원 금액을 사용하며 정적 단가를 설정하지 않습니다.",
                })}
              </div>
            ) : (
              <div className="overflow-hidden rounded-md border">
                <div className="flex items-center justify-between border-b bg-muted/30 px-4 py-3">
                  <div>
                    <p className="text-sm font-medium">
                      {t({ en: "Metered items", "zh-CN": "计量项", ja: "計量項目", ko: "계량 항목" })}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      {t({
                        en: "Supports official units such as tokens, images, video seconds, requests, and points.",
                        "zh-CN": "支持 token、图片、视频秒数、请求与积分等官方口径。",
                        ja: "トークン、画像、動画秒数、リクエスト、ポイントなどの公式単位に対応します。",
                        ko: "토큰, 이미지, 동영상 초, 요청, 포인트 등의 공식 단위를 지원합니다.",
                      })}
                    </p>
                  </div>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() =>
                      setForm((current) => ({
                        ...current,
                        components: [...current.components, defaultComponent(current.media_kind, book.purpose)],
                      }))
                    }
                  >
                    <Plus aria-hidden="true" />
                    {t({ en: "Add metered item", "zh-CN": "添加计量项", ja: "計量項目を追加", ko: "계량 항목 추가" })}
                  </Button>
                </div>
                <div className="divide-y">
                  {form.components.map((component, index) => (
                    <ComponentEditor
                      key={index}
                      component={component}
                      currency={book.currency}
                      free={form.is_free}
                      purpose={book.purpose}
                      onChange={(next) =>
                        setForm((current) => ({
                          ...current,
                          components: current.components.map((item, itemIndex) =>
                            itemIndex === index ? next : item
                          ),
                        }))
                      }
                      onRemove={() =>
                        setForm((current) => ({
                          ...current,
                          components: current.components.filter((_, itemIndex) => itemIndex !== index),
                        }))
                      }
                    />
                  ))}
                </div>
              </div>
            )}
          </section>

          <section className="space-y-4 border-t pt-6">
            <SectionTitle
              title={t({ en: "Effective date and source", "zh-CN": "生效与来源", ja: "適用日時とソース", ko: "적용 시점 및 출처" })}
              description={t({
                en: "Official and contract prices must include a verifiable source URL and verification time.",
                "zh-CN": "官方或合同价格必须保存可复核的来源地址与核验时间。",
                ja: "公式価格と契約価格には、検証可能なソース URL と検証日時が必要です。",
                ko: "공식 가격 및 계약 가격에는 검증 가능한 출처 URL과 검증 시간이 필요합니다.",
              })}
            />
            <div className="grid gap-4 md:grid-cols-2">
              <Field label={t({ en: "Effective from", "zh-CN": "生效时间", ja: "適用開始", ko: "적용 시작" })}>
                <Input
                  type="datetime-local"
                  value={form.effective_from}
                  onChange={(event) => setField(setForm, "effective_from", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Source type", "zh-CN": "来源类型", ja: "ソース種別", ko: "출처 유형" })}>
                <Select
                  value={form.source_kind}
                  onValueChange={(value: PriceBookVersion["source_kind"]) =>
                    setForm((current) => ({
                      ...current,
                      source_kind: value,
                      source_checked_at:
                        (value === "official_document" || value === "provider_contract")
                          && !current.source_checked_at
                          ? toLocalInput(Date.now())
                          : current.source_checked_at,
                    }))
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="official_document">{t({ en: "Official documentation", "zh-CN": "官方文档", ja: "公式ドキュメント", ko: "공식 문서" })}</SelectItem>
                    <SelectItem value="provider_contract">{t({ en: "Provider contract", "zh-CN": "供应商合同", ja: "プロバイダー契約", ko: "제공업체 계약" })}</SelectItem>
                    <SelectItem value="manual">{t({ en: "Manual platform configuration", "zh-CN": "平台人工配置", ja: "プラットフォーム手動設定", ko: "플랫폼 수동 설정" })}</SelectItem>
                    <SelectItem value="imported">{t({ en: "Bulk import", "zh-CN": "批量导入", ja: "一括インポート", ko: "일괄 가져오기" })}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label={t({ en: "Source URL", "zh-CN": "来源地址", ja: "ソース URL", ko: "출처 URL" })} className="md:col-span-2">
                <Input
                  type="url"
                  value={form.source_url}
                  required={officialSource}
                  placeholder="https://..."
                  onChange={(event) => setField(setForm, "source_url", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Source verified at", "zh-CN": "来源核验时间", ja: "ソース検証日時", ko: "출처 검증 시간" })}>
                <Input
                  type="datetime-local"
                  value={form.source_checked_at}
                  required={officialSource}
                  onChange={(event) => setField(setForm, "source_checked_at", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Provider", "zh-CN": "供应商", ja: "プロバイダー", ko: "제공업체" })}>
                <Input
                  value={form.provider_id}
                  disabled={identityLocked}
                  placeholder={t({
                    en: "e.g. openai, grok, dreamina",
                    "zh-CN": "例如 openai、grok、dreamina",
                    ja: "例: openai、grok、dreamina",
                    ko: "예: openai, grok, dreamina",
                  })}
                  onChange={(event) => setField(setForm, "provider_id", event.target.value)}
                />
              </Field>
              <Field label={t({ en: "Internal notes", "zh-CN": "内部备注", ja: "内部メモ", ko: "내부 메모" })} className="md:col-span-2">
                <Textarea
                  value={form.notes}
                  placeholder={t({
                    en: "Describe the pricing basis, manual conversion method, or applicable restrictions",
                    "zh-CN": "说明价格口径、人工换算方法或适用限制",
                    ja: "価格基準、手動換算方法、適用制限を記載",
                    ko: "가격 기준, 수동 환산 방식 또는 적용 제한을 설명하세요",
                  })}
                  onChange={(event) => setField(setForm, "notes", event.target.value)}
                />
              </Field>
            </div>
          </section>
        </div>

        <DialogFooter className="sticky bottom-0 border-t bg-background px-6 py-4">
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
          </Button>
          <Button type="button" disabled={saving || Boolean(validation)} onClick={save}>
            <Save aria-hidden="true" />
            {saving
              ? t({ en: "Saving", "zh-CN": "保存中", ja: "保存中", ko: "저장 중" })
              : t({ en: "Save draft", "zh-CN": "保存草稿", ja: "下書きを保存", ko: "초안 저장" })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ComponentEditor({
  component,
  currency,
  free,
  purpose,
  onChange,
  onRemove,
}: {
  component: EditableComponent;
  currency: string;
  free: boolean;
  purpose: PriceBook["purpose"];
  onChange: (component: EditableComponent) => void;
  onRemove: () => void;
}) {
  const { t } = useI18n();
  return (
    <div className="space-y-4 p-4">
      <div className="grid gap-3 md:grid-cols-[1fr_1.2fr_0.8fr_1fr_auto] md:items-end">
        <Field label={t({ en: "Key", "zh-CN": "标识", ja: "キー", ko: "키" })}>
          <Input
            value={component.component_key}
            onChange={(event) => onChange({ ...component, component_key: event.target.value })}
          />
        </Field>
        <Field label={t({ en: "Metric", "zh-CN": "计量指标", ja: "計量指標", ko: "계량 지표" })}>
          <Select
            value={component.metric}
            onValueChange={(metric) => onChange({ ...component, metric })}
          >
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              {METRICS.map((metric) => (
                <SelectItem key={metric} value={metric}>{metricLabel(metric, t)}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field label={t({ en: "Per", "zh-CN": "每", ja: "単位", ko: "단위" })}>
          <Input
            inputMode="numeric"
            value={component.unit_size}
            onChange={(event) => onChange({ ...component, unit_size: event.target.value })}
          />
        </Field>
        <Field label={t({
          en: "Unit price ({currency})",
          "zh-CN": "单价 ({currency})",
          ja: "単価（{currency}）",
          ko: "단가 ({currency})",
        }, { currency })}>
          <Input
            inputMode="decimal"
            value={component.unit_price}
            disabled={free}
            onChange={(event) => onChange({ ...component, unit_price: event.target.value })}
          />
        </Field>
        <Button
          type="button"
          size="icon"
          variant="ghost"
          aria-label={t({ en: "Remove metered item", "zh-CN": "删除计量项", ja: "計量項目を削除", ko: "계량 항목 삭제" })}
          onClick={onRemove}
        >
          <Trash2 aria-hidden="true" />
        </Button>
      </div>
      <details className="text-sm">
        <summary className="cursor-pointer text-muted-foreground">
          {t({ en: "Advanced metering rules", "zh-CN": "高级计量规则", ja: "高度な計量ルール", ko: "고급 계량 규칙" })}
        </summary>
        <div className="mt-3 grid gap-3 md:grid-cols-4">
          <Field label={t({ en: "Outcome", "zh-CN": "结果", ja: "結果", ko: "결과" })}>
            <Select value={component.outcome} onValueChange={(value) => onChange({ ...component, outcome: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="succeeded">{t({ en: "Succeeded", "zh-CN": "成功", ja: "成功", ko: "성공" })}</SelectItem>
                <SelectItem value="failed">{t({ en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" })}</SelectItem>
                <SelectItem value="no_effect">{t({ en: "No output", "zh-CN": "无产出", ja: "出力なし", ko: "출력 없음" })}</SelectItem>
                <SelectItem value="any">{t({ en: "Any", "zh-CN": "全部", ja: "すべて", ko: "전체" })}</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label={t({ en: "Quantity source", "zh-CN": "数量来源", ja: "数量ソース", ko: "수량 출처" })}>
            <Select value={component.quantity_source} onValueChange={(value) => onChange({ ...component, quantity_source: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="provider_reported">{t({ en: "Provider reported", "zh-CN": "供应商回执", ja: "プロバイダー報告", ko: "제공업체 보고" })}</SelectItem>
                <SelectItem value="request_derived">{t({ en: "Derived from request", "zh-CN": "请求推导", ja: "リクエストから算出", ko: "요청에서 산출" })}</SelectItem>
                {purpose !== "customer_sale" ? (
                  <SelectItem value="media_inspected">{t({ en: "Media inspection", "zh-CN": "媒体实测", ja: "メディア実測", ko: "미디어 실측" })}</SelectItem>
                ) : null}
                <SelectItem value="official_lookup">{t({ en: "Official lookup", "zh-CN": "官方查表", ja: "公式参照表", ko: "공식 조회표" })}</SelectItem>
                <SelectItem value="operator_adjustment">{t({ en: "Manual adjustment", "zh-CN": "人工调整", ja: "手動調整", ko: "수동 조정" })}</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label={t({ en: "Minimum confidence", "zh-CN": "最低置信度", ja: "最低信頼度", ko: "최소 신뢰도" })}>
            <Select value={component.required_confidence} onValueChange={(value) => onChange({ ...component, required_confidence: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="exact">{t({ en: "Exact", "zh-CN": "精确", ja: "正確", ko: "정확" })}</SelectItem>
                <SelectItem value="bounded">{t({ en: "Bounded estimate", "zh-CN": "有界估算", ja: "範囲付き推定", ko: "범위 추정" })}</SelectItem>
                <SelectItem value="estimated">{t({ en: "Estimated", "zh-CN": "估算", ja: "推定", ko: "추정" })}</SelectItem>
                <SelectItem value="any">{t({ en: "Any", "zh-CN": "任意", ja: "任意", ko: "모두 허용" })}</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label={t({ en: "Rounding", "zh-CN": "舍入", ja: "丸め", ko: "반올림" })}>
            <Select value={component.rounding_mode} onValueChange={(value) => onChange({ ...component, rounding_mode: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="ceil">{t({ en: "Round up", "zh-CN": "向上", ja: "切り上げ", ko: "올림" })}</SelectItem>
                <SelectItem value="floor">{t({ en: "Round down", "zh-CN": "向下", ja: "切り捨て", ko: "내림" })}</SelectItem>
                <SelectItem value="half_up">{t({ en: "Round half up", "zh-CN": "四舍五入", ja: "四捨五入", ko: "사사오입" })}</SelectItem>
                <SelectItem value="exact">{t({ en: "Must divide evenly", "zh-CN": "必须整除", ja: "割り切れる場合のみ", ko: "정확히 나누어져야 함" })}</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label={t({ en: "Pricing dimensions JSON", "zh-CN": "计价维度 JSON", ja: "価格ディメンション JSON", ko: "가격 차원 JSON" })} className="md:col-span-4">
            <Textarea
              className="min-h-20 font-mono text-xs"
              value={component.dimensions}
              onChange={(event) => onChange({ ...component, dimensions: event.target.value })}
            />
          </Field>
        </div>
      </details>
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

function SectionTitle({ title, description }: { title: string; description: string }) {
  return (
    <div>
      <h3 className="text-sm font-semibold">{title}</h3>
      <p className="mt-1 text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

function emptyForm(book: PriceBook | null, preset: PricingCoverageRow | null): VersionForm {
  const mediaKind = preset?.media_kind ?? "image";
  const purpose = book?.purpose ?? "customer_sale";
  return {
    api_profile: preset?.api_profile ?? "openai.images.v1",
    operation: preset?.pricing_operation
      ?? (mediaKind === "video" ? "video_generation" : "generation"),
    provider_id: preset?.provider_id ?? book?.provider_id ?? "",
    provider_model_id: preset?.provider_model_id ?? "",
    public_model_id: preset?.public_model_id ?? "",
    media_kind: mediaKind,
    service_tier: "standard",
    execution_surface: preset || book?.provider_id ? "provider_cli" : "provider_api",
    billing_mode: BILLING_MODES[purpose][0],
    is_free: false,
    effective_from: toLocalInput(Date.now()),
    source_kind: "manual",
    source_url: "",
    source_checked_at: "",
    notes: "",
    components: defaultComponents(mediaKind, purpose, preset),
  };
}

function formFromVersion(version: PriceBookVersion): VersionForm {
  return {
    api_profile: version.api_profile,
    operation: version.operation,
    provider_id: version.provider_id ?? "",
    provider_model_id: version.provider_model_id ?? "",
    public_model_id: version.public_model_id,
    media_kind: version.media_kind,
    service_tier: normalizePriceServiceTier(version.service_tier),
    execution_surface: version.execution_surface,
    billing_mode: version.billing_mode,
    is_free: version.is_free,
    effective_from: toLocalInput(version.effective_from_ms),
    source_kind: version.source_kind,
    source_url: version.source_url ?? "",
    source_checked_at: version.source_checked_at_ms
      ? toLocalInput(version.source_checked_at_ms)
      : "",
    notes: version.notes ?? "",
    components: version.components.map((component) => ({
      component_key: component.component_key,
      metric: component.metric,
      unit_size: component.unit_size,
      unit_price: microsToDecimal(component.unit_price_micros),
      outcome: component.outcome,
      quantity_source: component.quantity_source,
      required_confidence: component.required_confidence,
      rounding_mode: component.rounding_mode,
      dimensions: JSON.stringify(component.dimensions, null, 2),
    })),
  };
}

function defaultComponent(
  mediaKind: "image" | "video",
  purpose: PriceBook["purpose"],
  outcome = "succeeded",
): EditableComponent {
  const providerActual = purpose === "provider_actual";
  const customerSale = purpose === "customer_sale";
  const componentKey = mediaKind === "video"
    ? "output_second"
    : "output_image";
  return {
    component_key: `${componentKey}_${outcome}`,
    metric: mediaKind === "video"
      ? customerSale
        ? "video_requested_second"
        : "video_output_second"
      : "image_output",
    unit_size: "1",
    unit_price: "0",
    outcome,
    quantity_source: providerActual
      ? "provider_reported"
      : mediaKind === "video" && !customerSale
        ? "media_inspected"
        : "request_derived",
    required_confidence: "exact",
    rounding_mode: "exact",
    dimensions: "{}",
  };
}

function customerComponent(
  basis: PricingCoverageRow["customer_metering_bases"][number],
  outcome: string,
): EditableComponent {
  return {
    component_key: `${basis.metric}_${outcome}`,
    metric: basis.metric,
    unit_size: "1",
    unit_price: "0",
    outcome,
    quantity_source: basis.quantity_source,
    required_confidence: basis.confidence,
    rounding_mode: "exact",
    dimensions: "{}",
  };
}

function defaultComponents(
  mediaKind: "image" | "video",
  purpose: PriceBook["purpose"],
  preset: PricingCoverageRow | null,
) {
  if (purpose !== "customer_sale") {
    return [defaultComponent(mediaKind, purpose)];
  }
  if (preset?.customer_metering_bases.length) {
    return preset.customer_metering_bases.flatMap((basis) =>
      ["succeeded", "failed", "no_effect"].map((outcome) =>
        customerComponent(basis, outcome)
      )
    );
  }
  return ["succeeded", "failed", "no_effect"].map((outcome) =>
    defaultComponent(mediaKind, purpose, outcome)
  );
}

function buildDraft(form: VersionForm): PriceBookVersionDraft {
  return {
    api_profile: form.api_profile.trim(),
    operation: form.operation.trim(),
    provider_id: emptyToNull(form.provider_id),
    provider_model_id: emptyToNull(form.provider_model_id),
    public_model_id: form.public_model_id.trim(),
    media_kind: form.media_kind,
    service_tier: form.service_tier.trim(),
    execution_surface: form.execution_surface,
    billing_mode: form.billing_mode,
    is_free: form.is_free,
    effective_from_ms: new Date(form.effective_from).getTime(),
    source_kind: form.source_kind,
    source_url: emptyToNull(form.source_url),
    source_checked_at_ms: form.source_checked_at
      ? new Date(form.source_checked_at).getTime()
      : null,
    notes: emptyToNull(form.notes),
    components: form.billing_mode === "provider_reported"
      ? []
      : form.components.map(toComponentDraft),
  };
}

function toComponentDraft(component: EditableComponent): PriceComponentDraft {
  return {
    component_key: component.component_key.trim(),
    metric: component.metric,
    unit: metricUnit(component.metric),
    unit_size: component.unit_size.trim(),
    unit_price_micros: decimalToMicros(component.unit_price),
    outcome: component.outcome,
    quantity_source: component.quantity_source,
    required_confidence: component.required_confidence,
    rounding_mode: component.rounding_mode,
    dimensions: JSON.parse(component.dimensions || "{}") as Record<string, unknown>,
  };
}

function validateForm(form: VersionForm, book: PriceBook | null, t: Translate): string | null {
  if (!book) {
    return t({ en: "Select a price book", "zh-CN": "请选择价格簿", ja: "価格表を選択してください", ko: "가격표를 선택하세요" });
  }
  if (!form.public_model_id.trim()) {
    return t({ en: "Enter a public model ID", "zh-CN": "请填写外部模型 ID", ja: "公開モデル ID を入力してください", ko: "외부 모델 ID를 입력하세요" });
  }
  if (!form.api_profile.trim() || !form.operation.trim()) {
    return t({ en: "Enter the API protocol and operation", "zh-CN": "请填写 API 协议和操作", ja: "API プロトコルと操作を入力してください", ko: "API 프로토콜과 작업을 입력하세요" });
  }
  if (!form.service_tier.trim()) {
    return t({ en: "Enter a service tier", "zh-CN": "请填写服务层级", ja: "サービス階層を入力してください", ko: "서비스 등급을 입력하세요" });
  }
  if (!form.effective_from || Number.isNaN(new Date(form.effective_from).getTime())) {
    return t({ en: "Select a valid effective time", "zh-CN": "请选择有效的生效时间", ja: "有効な適用日時を選択してください", ko: "유효한 적용 시간을 선택하세요" });
  }
  const official =
    form.source_kind === "official_document" || form.source_kind === "provider_contract";
  if (official && (!form.source_url.trim() || !form.source_checked_at)) {
    return t({
      en: "Official documentation and contract prices require a source URL and verification time",
      "zh-CN": "官方文档或合同价格必须填写来源地址和核验时间",
      ja: "公式ドキュメントまたは契約価格にはソース URL と検証日時が必要です",
      ko: "공식 문서 또는 계약 가격에는 출처 URL과 검증 시간이 필요합니다",
    });
  }
  if (form.billing_mode !== "provider_reported" && form.components.length === 0) {
    return t({ en: "Add at least one metered item", "zh-CN": "请至少添加一个计量项", ja: "計量項目を1つ以上追加してください", ko: "계량 항목을 하나 이상 추가하세요" });
  }
  for (const component of form.components) {
    if (!/^[A-Za-z0-9_.:-]+$/.test(component.component_key)) {
      return t({
        en: "Metered item keys may contain only letters, numbers, periods, colons, underscores, and hyphens",
        "zh-CN": "计量项标识只能包含字母、数字、点、冒号、下划线和连字符",
        ja: "計量項目キーには英数字、ピリオド、コロン、アンダースコア、ハイフンのみ使用できます",
        ko: "계량 항목 키에는 영문자, 숫자, 마침표, 콜론, 밑줄 및 하이픈만 사용할 수 있습니다",
      });
    }
    if (!/^[1-9]\d*$/.test(component.unit_size)) {
      return t({
        en: "The metering unit quantity must be a positive integer",
        "zh-CN": "计量单位数量必须是正整数",
        ja: "計量単位数は正の整数である必要があります",
        ko: "계량 단위 수량은 양의 정수여야 합니다",
      });
    }
    try {
      decimalToMicros(component.unit_price);
      const dimensions = JSON.parse(component.dimensions || "{}");
      if (!dimensions || Array.isArray(dimensions) || typeof dimensions !== "object") {
        return t({
          en: "Pricing dimensions must be a JSON object",
          "zh-CN": "计价维度必须是 JSON 对象",
          ja: "価格ディメンションは JSON オブジェクトである必要があります",
          ko: "가격 차원은 JSON 객체여야 합니다",
        });
      }
    } catch {
      return t({
        en: "Unit prices support up to 6 decimal places, and pricing dimensions must be a valid JSON object",
        "zh-CN": "单价最多支持 6 位小数，计价维度必须是有效 JSON 对象",
        ja: "単価は小数点以下6桁まで対応し、価格ディメンションは有効な JSON オブジェクトである必要があります",
        ko: "단가는 소수점 이하 6자리까지 지원하며 가격 차원은 유효한 JSON 객체여야 합니다",
      });
    }
  }
  return null;
}

function normalizePriceServiceTier(
  value: string,
): VersionForm["service_tier"] {
  if (value === "flex" || value === "priority") return value;
  return "standard";
}

function metricUnit(metric: string) {
  if (metric === "request") return "request";
  if (metric === "image_input" || metric === "image_output") return "image";
  if (
    metric === "video_input_second"
    || metric === "video_requested_second"
    || metric === "video_output_second"
  ) return "second";
  if (metric === "membership_point") return "point";
  return "token";
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

function decimalToMicros(value: string) {
  const match = value.trim().match(/^(\d+)(?:\.(\d{0,6}))?$/);
  if (!match) throw new Error("invalid decimal");
  const whole = BigInt(match[1]);
  const fraction = BigInt((match[2] ?? "").padEnd(6, "0") || "0");
  return (whole * 1_000_000n + fraction).toString();
}

function microsToDecimal(value: string) {
  const parsed = BigInt(value);
  const whole = parsed / 1_000_000n;
  const fraction = (parsed % 1_000_000n).toString().padStart(6, "0").replace(/0+$/, "");
  return fraction ? `${whole}.${fraction}` : whole.toString();
}

function toLocalInput(value: number) {
  const date = new Date(value);
  const offset = date.getTimezoneOffset() * 60_000;
  return new Date(value - offset).toISOString().slice(0, 16);
}

function emptyToNull(value: string) {
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function setField<K extends keyof VersionForm>(
  setForm: React.Dispatch<React.SetStateAction<VersionForm>>,
  key: K,
  value: VersionForm[K],
) {
  setForm((current) => ({ ...current, [key]: value }));
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
