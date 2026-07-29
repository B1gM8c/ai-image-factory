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
import { consoleFetch } from "@/lib/auth/client";
import type {
  PriceBook,
  PriceBookVersion,
  PriceBookVersionDraft,
  PriceComponentDraft,
  PricingCoverageRow,
} from "@/lib/admin/types";

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
  const [form, setForm] = useState<VersionForm>(() => emptyForm(book, preset));
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) setForm(version ? formFromVersion(version) : emptyForm(book, preset));
  }, [book, open, preset, version]);

  const providerReported = form.billing_mode === "provider_reported";
  const officialSource =
    form.source_kind === "official_document" || form.source_kind === "provider_contract";
  const title = version ? "编辑价格草稿" : "添加模型价格";
  const identityLocked = Boolean(preset && !version);

  const validation = useMemo(() => validateForm(form, book), [book, form]);

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
      if (!response.ok) throw new Error(await responseMessage(response));
      const saved = (await response.json()) as PriceBookVersion;
      toast.success(version ? "价格草稿已保存" : "模型价格草稿已创建");
      onSaved(saved);
      onOpenChange(false);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存价格草稿失败");
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
            {book.display_name} · {book.currency}。保存后仍是草稿，发布前可以继续修改。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 px-6 py-1">
          <section className="space-y-4">
            <SectionTitle
              title="模型与接口"
              description={identityLocked
                ? "模型身份来自已启用的平台路由，内部契约字段已由系统锁定。"
                : "外部模型 ID 面向 API 用户，原生模型 ID 用于供应商执行与成本归因。"}
            />
            <div className="grid gap-4 md:grid-cols-2">
              <Field label="外部模型 ID">
                <Input
                  value={form.public_model_id}
                  disabled={identityLocked}
                  placeholder="例如 gpt-image-2"
                  onChange={(event) => setField(setForm, "public_model_id", event.target.value)}
                />
              </Field>
              <Field label="供应商原生模型 ID">
                <Input
                  value={form.provider_model_id}
                  disabled={identityLocked}
                  placeholder="没有独立原生 ID 时可留空"
                  onChange={(event) => setField(setForm, "provider_model_id", event.target.value)}
                />
              </Field>
              <Field label="API 协议">
                <Input
                  value={form.api_profile}
                  disabled={identityLocked}
                  placeholder="例如 openai.images.v1"
                  onChange={(event) => setField(setForm, "api_profile", event.target.value)}
                />
              </Field>
              <Field label="操作">
                <Input
                  value={form.operation}
                  disabled={identityLocked}
                  placeholder="例如 generation"
                  onChange={(event) => setField(setForm, "operation", event.target.value)}
                />
              </Field>
              <Field label="媒体类型">
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
                    <SelectItem value="image">图片</SelectItem>
                    <SelectItem value="video">视频</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="服务层级">
                <Select
                  value={form.service_tier}
                  disabled={identityLocked}
                  onValueChange={(value: VersionForm["service_tier"]) =>
                    setField(setForm, "service_tier", value)
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="standard">Default</SelectItem>
                    <SelectItem value="flex">Flex</SelectItem>
                    <SelectItem value="priority">Priority</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            </div>
          </section>

          <section className="space-y-4 border-t pt-6">
            <SectionTitle title="计价方式" description="每个计量项使用整数微单位存储，界面按当前币种输入十进制单价。" />
            <div className="grid gap-4 md:grid-cols-3">
              <Field label="计费模式">
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
                      <SelectItem key={mode} value={mode}>{billingModeLabel(mode)}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
              <Field label="执行渠道">
                <Select
                  value={form.execution_surface}
                  disabled={identityLocked}
                  onValueChange={(value: PriceBookVersion["execution_surface"]) =>
                    setField(setForm, "execution_surface", value)
                  }
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="provider_cli">供应商 CLI</SelectItem>
                    <SelectItem value="provider_api">供应商 API</SelectItem>
                    <SelectItem value="manual_import">人工导入</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="收费状态">
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
                    <SelectItem value="paid">付费</SelectItem>
                    <SelectItem value="free">免费</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            </div>

            {providerReported ? (
              <div className="rounded-md border bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
                供应商回执模式直接采用上游返回的原生金额，不配置静态单价。
              </div>
            ) : (
              <div className="overflow-hidden rounded-md border">
                <div className="flex items-center justify-between border-b bg-muted/30 px-4 py-3">
                  <div>
                    <p className="text-sm font-medium">计量项</p>
                    <p className="text-xs text-muted-foreground">支持 token、图片、视频秒数、请求与积分等官方口径。</p>
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
                    添加计量项
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
            <SectionTitle title="生效与来源" description="官方或合同价格必须保存可复核的来源地址与核验时间。" />
            <div className="grid gap-4 md:grid-cols-2">
              <Field label="生效时间">
                <Input
                  type="datetime-local"
                  value={form.effective_from}
                  onChange={(event) => setField(setForm, "effective_from", event.target.value)}
                />
              </Field>
              <Field label="来源类型">
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
                    <SelectItem value="official_document">官方文档</SelectItem>
                    <SelectItem value="provider_contract">供应商合同</SelectItem>
                    <SelectItem value="manual">平台人工配置</SelectItem>
                    <SelectItem value="imported">批量导入</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="来源地址" className="md:col-span-2">
                <Input
                  type="url"
                  value={form.source_url}
                  required={officialSource}
                  placeholder="https://..."
                  onChange={(event) => setField(setForm, "source_url", event.target.value)}
                />
              </Field>
              <Field label="来源核验时间">
                <Input
                  type="datetime-local"
                  value={form.source_checked_at}
                  required={officialSource}
                  onChange={(event) => setField(setForm, "source_checked_at", event.target.value)}
                />
              </Field>
              <Field label="供应商">
                <Input
                  value={form.provider_id}
                  disabled={identityLocked}
                  placeholder="例如 openai、grok、dreamina"
                  onChange={(event) => setField(setForm, "provider_id", event.target.value)}
                />
              </Field>
              <Field label="内部备注" className="md:col-span-2">
                <Textarea
                  value={form.notes}
                  placeholder="说明价格口径、人工换算方法或适用限制"
                  onChange={(event) => setField(setForm, "notes", event.target.value)}
                />
              </Field>
            </div>
          </section>
        </div>

        <DialogFooter className="sticky bottom-0 border-t bg-background px-6 py-4">
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>取消</Button>
          <Button type="button" disabled={saving || Boolean(validation)} onClick={save}>
            <Save aria-hidden="true" />
            {saving ? "保存中" : "保存草稿"}
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
  return (
    <div className="space-y-4 p-4">
      <div className="grid gap-3 md:grid-cols-[1fr_1.2fr_0.8fr_1fr_auto] md:items-end">
        <Field label="标识">
          <Input
            value={component.component_key}
            onChange={(event) => onChange({ ...component, component_key: event.target.value })}
          />
        </Field>
        <Field label="计量指标">
          <Select
            value={component.metric}
            onValueChange={(metric) => onChange({ ...component, metric })}
          >
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              {METRICS.map((metric) => (
                <SelectItem key={metric} value={metric}>{metricLabel(metric)}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field label="每">
          <Input
            inputMode="numeric"
            value={component.unit_size}
            onChange={(event) => onChange({ ...component, unit_size: event.target.value })}
          />
        </Field>
        <Field label={`单价 (${currency})`}>
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
          aria-label="删除计量项"
          onClick={onRemove}
        >
          <Trash2 aria-hidden="true" />
        </Button>
      </div>
      <details className="text-sm">
        <summary className="cursor-pointer text-muted-foreground">高级计量规则</summary>
        <div className="mt-3 grid gap-3 md:grid-cols-4">
          <Field label="结果">
            <Select value={component.outcome} onValueChange={(value) => onChange({ ...component, outcome: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="succeeded">成功</SelectItem>
                <SelectItem value="failed">失败</SelectItem>
                <SelectItem value="no_effect">无产出</SelectItem>
                <SelectItem value="any">全部</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="数量来源">
            <Select value={component.quantity_source} onValueChange={(value) => onChange({ ...component, quantity_source: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="provider_reported">供应商回执</SelectItem>
                <SelectItem value="request_derived">请求推导</SelectItem>
                {purpose !== "customer_sale" ? (
                  <SelectItem value="media_inspected">媒体实测</SelectItem>
                ) : null}
                <SelectItem value="official_lookup">官方查表</SelectItem>
                <SelectItem value="operator_adjustment">人工调整</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="最低置信度">
            <Select value={component.required_confidence} onValueChange={(value) => onChange({ ...component, required_confidence: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="exact">精确</SelectItem>
                <SelectItem value="bounded">有界估算</SelectItem>
                <SelectItem value="estimated">估算</SelectItem>
                <SelectItem value="any">任意</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="舍入">
            <Select value={component.rounding_mode} onValueChange={(value) => onChange({ ...component, rounding_mode: value })}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="ceil">向上</SelectItem>
                <SelectItem value="floor">向下</SelectItem>
                <SelectItem value="half_up">四舍五入</SelectItem>
                <SelectItem value="exact">必须整除</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="计价维度 JSON" className="md:col-span-4">
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

function validateForm(form: VersionForm, book: PriceBook | null): string | null {
  if (!book) return "请选择价格簿";
  if (!form.public_model_id.trim()) return "请填写外部模型 ID";
  if (!form.api_profile.trim() || !form.operation.trim()) return "请填写 API 协议和操作";
  if (!form.service_tier.trim()) return "请填写服务层级";
  if (!form.effective_from || Number.isNaN(new Date(form.effective_from).getTime())) {
    return "请选择有效的生效时间";
  }
  const official =
    form.source_kind === "official_document" || form.source_kind === "provider_contract";
  if (official && (!form.source_url.trim() || !form.source_checked_at)) {
    return "官方文档或合同价格必须填写来源地址和核验时间";
  }
  if (form.billing_mode !== "provider_reported" && form.components.length === 0) {
    return "请至少添加一个计量项";
  }
  for (const component of form.components) {
    if (!/^[A-Za-z0-9_.:-]+$/.test(component.component_key)) {
      return "计量项标识只能包含字母、数字、点、冒号、下划线和连字符";
    }
    if (!/^[1-9]\d*$/.test(component.unit_size)) return "计量单位数量必须是正整数";
    try {
      decimalToMicros(component.unit_price);
      const dimensions = JSON.parse(component.dimensions || "{}");
      if (!dimensions || Array.isArray(dimensions) || typeof dimensions !== "object") {
        return "计价维度必须是 JSON 对象";
      }
    } catch {
      return "单价最多支持 6 位小数，计价维度必须是有效 JSON 对象";
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

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as { error?: string | { message?: string } };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Fall through to a stable UI message.
  }
  return `保存失败 (${response.status})`;
}
