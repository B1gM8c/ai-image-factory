"use client";

import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { useI18n } from "@/i18n/locale-provider";
import type { ProviderProfileReadiness } from "@/lib/gateway/server";

export function ReadinessChart({ profiles }: { profiles: ProviderProfileReadiness }) {
  const { t } = useI18n();
  const chartConfig = {
    count: {
      label: t({
        en: "Profiles",
        "zh-CN": "Profile 数",
        ja: "プロファイル数",
        ko: "프로필 수",
      }),
      color: "var(--chart-2)",
    },
  } satisfies ChartConfig;
  const data = [
    {
      state: t({
        en: "Configured",
        "zh-CN": "已配置",
        ja: "設定済み",
        ko: "구성됨",
      }),
      count: profiles.configured,
    },
    {
      state: t({
        en: "Active",
        "zh-CN": "活跃",
        ja: "稼働中",
        ko: "활성",
      }),
      count: profiles.active,
    },
    {
      state: t({
        en: "Draining",
        "zh-CN": "排空中",
        ja: "ドレイン中",
        ko: "드레이닝",
      }),
      count: profiles.draining,
    },
    {
      state: t({
        en: "Blocked",
        "zh-CN": "已阻止",
        ja: "ブロック中",
        ko: "차단됨",
      }),
      count: profiles.blocked,
    },
  ];

  return (
    <ChartContainer config={chartConfig} className="h-56 w-full aspect-auto">
      <BarChart data={data} margin={{ left: 0, right: 8, top: 8, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="state" tickLine={false} axisLine={false} tickMargin={8} />
        <YAxis allowDecimals={false} tickLine={false} axisLine={false} width={24} />
        <ChartTooltip cursor={false} content={<ChartTooltipContent hideLabel />} />
        <Bar dataKey="count" fill="var(--color-count)" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ChartContainer>
  );
}
