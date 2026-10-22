"use client";

import { useEffect, useState } from "react";
import { LoaderCircle } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { useI18n } from "@/i18n/locale-provider";
import type { ProviderRoute } from "@/lib/admin/types";
import {
  consoleFetch,
  consoleRequestFailure,
  consoleResponseFailure,
} from "@/lib/auth/client";

type PlatformBinding = {
  route_id: string;
  route_revision: number;
  state: string;
};

export function PlatformRouteBindingPanel({
  route,
  onPendingChange,
  onConflict,
}: {
  route: ProviderRoute;
  onPendingChange: (pending: boolean) => void;
  onConflict: () => void;
}) {
  const { t } = useI18n();
  const [binding, setBinding] = useState<PlatformBinding | null>();
  const [error, setError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  const [confirming, setConfirming] = useState(false);
  const [pending, setPending] = useState(false);
  const [conflict, setConflict] = useState(false);
  const path = `/api/gateway/admin/v1/provider-routes/${route.route_id}/platform-binding`;

  useEffect(() => {
    const controller = new AbortController();
    setBinding(undefined);
    setError(null);
    setConfirming(false);
    const failure = t({
      en: "Unable to load the platform route",
      "zh-CN": "无法读取平台路由",
      ja: "プラットフォームルートを読み込めません",
      ko: "플랫폼 라우트를 불러올 수 없습니다",
    });
    async function load() {
      try {
        const response = await consoleFetch(path, { signal: controller.signal });
        if (!response.ok) {
          throw new Error(await consoleResponseFailure(response, failure, t));
        }
        const result = (await response.json()) as { binding: PlatformBinding | null };
        if (!controller.signal.aborted) setBinding(result.binding);
      } catch (reason) {
        if (!controller.signal.aborted) {
          setError(consoleRequestFailure(reason, failure, t));
        }
      }
    }
    void load();
    return () => controller.abort();
  }, [path, route.revision, reload, t]);

  const current = binding?.route_id === route.route_id;
  const upToDate = current && binding.route_revision === route.revision && binding.state === "enabled";
  const action = current
    ? t({
        en: "Update platform route to this version",
        "zh-CN": "更新平台路由到当前版本",
        ja: "プラットフォームルートをこのバージョンに更新",
        ko: "플랫폼 라우트를 현재 버전으로 업데이트",
      })
    : t({
        en: "Set as platform route",
        "zh-CN": "设为平台路由",
        ja: "プラットフォームルートに設定",
        ko: "플랫폼 라우트로 설정",
      });

  async function publish() {
    if (binding === undefined || pending || conflict) return;
    setPending(true);
    onPendingChange(true);
    setError(null);
    const failure = t({
      en: "Unable to update the platform route",
      "zh-CN": "无法更新平台路由",
      ja: "プラットフォームルートを更新できません",
      ko: "플랫폼 라우트를 업데이트할 수 없습니다",
    });
    try {
      const response = await consoleFetch(path, {
        method: "PUT",
        body: JSON.stringify({
          expected_route_revision: route.revision,
          expected_binding: binding,
        }),
      });
      if (response.status === 409) {
        setConflict(true);
        onConflict();
        return;
      }
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      const result = (await response.json()) as { binding: PlatformBinding | null };
      setBinding(result.binding);
      toast.success(t({
        en: "Platform route updated",
        "zh-CN": "平台路由已更新",
        ja: "プラットフォームルートを更新しました",
        ko: "플랫폼 라우트가 업데이트되었습니다",
      }));
    } catch (reason) {
      setBinding(undefined);
      setError(consoleRequestFailure(reason, failure, t));
    } finally {
      setConfirming(false);
      setPending(false);
      onPendingChange(false);
    }
  }

  return (
    <section className="space-y-5 text-sm">
      <div className="space-y-2">
        <h3 className="font-medium">
          {t({
            en: "Platform route for {provider} / {operation}",
            "zh-CN": "{provider} / {operation} 的平台路由",
            ja: "{provider} / {operation} のプラットフォームルート",
            ko: "{provider} / {operation}의 플랫폼 라우트",
          }, { provider: route.provider_id, operation: route.operation_id })}
        </h3>
        <p className="text-muted-foreground">
          {t({
            en: "This selects the saved route version used by platform default requests and pricing coverage for this provider and capability. It does not change existing API key or project route bindings. Models still need an active price before they can be called.",
            "zh-CN": "此设置决定该供应商与能力的平台默认调用和定价覆盖所使用的路由版本。已有 API key 或项目的路由绑定保持不变；模型仍需配置有效价格才能调用。",
            ja: "このプロバイダーと機能のプラットフォーム既定リクエストおよび価格設定対象に使用する保存済みルートを選択します。既存の API キーやプロジェクトのルート設定は変更されません。呼び出しにはモデルの有効な価格設定も必要です。",
            ko: "이 공급자와 기능의 플랫폼 기본 요청 및 가격 설정 범위에 사용할 저장된 라우트 버전을 선택합니다. 기존 API 키나 프로젝트의 라우트 연결은 변경되지 않습니다. 모델을 호출하려면 유효한 가격도 설정해야 합니다.",
          })}
        </p>
      </div>

      <div className="space-y-2 rounded-lg border p-4">
        <p className="font-medium">
          {t({
            en: "Target: {name}, saved version {version}",
            "zh-CN": "目标：{name}，已保存版本 {version}",
            ja: "対象：{name}、保存済みバージョン {version}",
            ko: "대상: {name}, 저장된 버전 {version}",
          }, { name: route.display_name, version: route.revision })}
        </p>
        <p className="text-muted-foreground">
          {t({
            en: "Unsaved edits in this sheet are not included. To use edited mappings or members, publish the new version first, then reopen this sheet.",
            "zh-CN": "本面板中尚未保存的编辑不会生效。若要使用修改后的模型映射或成员，请先发布新版本，再重新打开此面板。",
            ja: "この画面の未保存の編集は含まれません。変更したモデルマッピングやメンバーを使うには、新しいバージョンを公開してから画面を開き直してください。",
            ko: "이 화면에서 저장하지 않은 편집은 적용되지 않습니다. 변경한 모델 매핑이나 구성원을 사용하려면 새 버전을 게시한 뒤 이 화면을 다시 여세요.",
          })}
        </p>
        <p className="break-all" aria-live="polite">
          {binding === undefined
            ? !error && t({ en: "Loading current binding…", "zh-CN": "正在读取当前绑定…", ja: "現在の設定を読み込み中…", ko: "현재 연결을 불러오는 중…" })
            : binding === null
              ? t({ en: "No platform route is configured yet.", "zh-CN": "尚未配置平台路由。", ja: "プラットフォームルートは未設定です。", ko: "플랫폼 라우트가 아직 설정되지 않았습니다." })
              : t({
                  en: "Current binding: {route}, version {version} ({state})",
                  "zh-CN": "当前绑定：{route}，版本 {version}（{state}）",
                  ja: "現在の設定：{route}、バージョン {version}（{state}）",
                  ko: "현재 연결: {route}, 버전 {version} ({state})",
                }, { route: current ? route.display_name : binding.route_id, version: binding.route_revision, state: binding.state })}
        </p>
      </div>

      {error ? <p role="alert" className="text-destructive">{error}</p> : null}
      {conflict ? (
        <p role="alert" className="text-destructive">
          {t({
            en: "The route or platform binding changed. Close and reopen this sheet to review the latest version before trying again.",
            "zh-CN": "路由或平台绑定已发生变化。请关闭并重新打开此面板，核对最新版本后再操作。",
            ja: "ルートまたはプラットフォーム設定が変更されました。画面を閉じて開き直し、最新バージョンを確認してから再実行してください。",
            ko: "라우트 또는 플랫폼 연결이 변경되었습니다. 이 화면을 닫았다가 다시 열어 최신 버전을 확인한 후 다시 시도하세요.",
          })}
        </p>
      ) : route.state !== "enabled" ? (
        <p className="text-muted-foreground">
          {t({ en: "Only enabled routes can be selected.", "zh-CN": "只有已启用的路由可以设为平台路由。", ja: "有効なルートのみ選択できます。", ko: "활성화된 라우트만 선택할 수 있습니다." })}
        </p>
      ) : upToDate ? (
        <p className="text-muted-foreground">
          {t({ en: "This saved version is already the active platform route.", "zh-CN": "此已保存版本已是当前生效的平台路由。", ja: "この保存済みバージョンは既に有効なプラットフォームルートです。", ko: "이 저장된 버전이 이미 활성 플랫폼 라우트입니다." })}
        </p>
      ) : confirming ? (
        <div className="space-y-3 rounded-lg border p-4">
          <p>
            {t({
              en: "Confirm switching the platform route to {name}, version {version}? This replaces the current binding shown above.",
              "zh-CN": "确认将平台路由切换为 {name} 的版本 {version}？此操作会替换上方展示的当前绑定。",
              ja: "プラットフォームルートを {name} のバージョン {version} に切り替えますか？上記の現在の設定を置き換えます。",
              ko: "플랫폼 라우트를 {name}의 버전 {version}(으)로 전환할까요? 위에 표시된 현재 연결을 대체합니다.",
            }, { name: route.display_name, version: route.revision })}
          </p>
          <div className="flex flex-wrap gap-2">
            <Button onClick={() => void publish()} disabled={pending}>
              {pending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}
              {t({ en: "Confirm", "zh-CN": "确认", ja: "確認", ko: "확인" })}
            </Button>
            <Button variant="outline" onClick={() => setConfirming(false)} disabled={pending}>
              {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
            </Button>
          </div>
        </div>
      ) : binding === undefined && error ? (
        <Button variant="outline" onClick={() => setReload((value) => value + 1)}>
          {t({ en: "Reload current binding", "zh-CN": "重新读取当前绑定", ja: "現在の設定を再読み込み", ko: "현재 연결 다시 불러오기" })}
        </Button>
      ) : (
        <Button onClick={() => setConfirming(true)} disabled={binding === undefined || pending}>
          {action}
        </Button>
      )}
    </section>
  );
}
