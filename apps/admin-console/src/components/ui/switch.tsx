"use client";

import * as React from "react";
import { cn } from "@/lib/utils";

const Switch = React.forwardRef<
  HTMLButtonElement,
  Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, "onChange"> & {
    checked?: boolean;
    onCheckedChange?: (checked: boolean) => void;
  }
>(({ checked = false, className, disabled, onCheckedChange, onClick, ...props }, ref) => (
  <button
    ref={ref}
    type="button"
    role="switch"
    aria-checked={checked}
    data-state={checked ? "checked" : "unchecked"}
    disabled={disabled}
    className={cn(
      "peer inline-flex h-5 w-9 shrink-0 cursor-pointer items-center rounded-full border border-transparent bg-input shadow-xs outline-none transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-primary",
      className,
    )}
    onClick={(event) => {
      onClick?.(event);
      if (!event.defaultPrevented) onCheckedChange?.(!checked);
    }}
    {...props}
  >
    <span
      data-state={checked ? "checked" : "unchecked"}
      className="pointer-events-none block size-4 rounded-full bg-background shadow-sm transition-transform data-[state=checked]:translate-x-4 data-[state=unchecked]:translate-x-0"
    />
  </button>
));
Switch.displayName = "Switch";

export { Switch };
