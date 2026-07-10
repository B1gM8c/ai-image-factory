import { cn } from "@/lib/utils";

type BadgeTone = "green" | "blue" | "amber" | "red" | "neutral";

const tones: Record<BadgeTone, string> = {
  green: "border-[#9ec8bc] bg-[#e3f4ef] text-[#14594d]",
  blue: "border-[#a8c2dd] bg-[#e8f1fb] text-[#244f7a]",
  amber: "border-[#e2c17b] bg-[#fff2cf] text-[#805000]",
  red: "border-[#dfa6a6] bg-[#fae8e8] text-[#8b2f2f]",
  neutral: "border-[var(--line)] bg-[#f4f4f1] text-[#4d5560]",
};

export function Badge({
  children,
  tone = "neutral",
  className,
}: {
  children: React.ReactNode;
  tone?: BadgeTone;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex h-6 items-center rounded-md border px-2 text-xs font-medium",
        tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}
