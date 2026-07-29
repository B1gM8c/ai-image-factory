"use client";

import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import type { ProviderProfileReadiness } from "@/lib/gateway/server";

const chartConfig = {
  count: { label: "Profile 数", color: "var(--chart-2)" },
} satisfies ChartConfig;

export function ReadinessChart({ profiles }: { profiles: ProviderProfileReadiness }) {
  const data = [
    { state: "Configured", count: profiles.configured },
    { state: "Active", count: profiles.active },
    { state: "Draining", count: profiles.draining },
    { state: "Blocked", count: profiles.blocked },
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
