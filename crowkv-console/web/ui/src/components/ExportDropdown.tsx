import { useState, useRef, useEffect } from 'react';
import { Download, ChevronDown } from 'lucide-react';
import { cn } from '../utils/cn';

export interface ExportOption {
  /** Stable id for the option. */
  id: string;
  /** Label shown in the menu, e.g. "Export as PNG". */
  label: string;
  /** Optional sublabel / hint. */
  hint?: string;
  /** Invoked when the user clicks the option. */
  onSelect: () => void | Promise<void>;
}

export interface ExportDropdownProps {
  options: ExportOption[];
  /** Override button label; default "Export". */
  buttonLabel?: string;
  /** Optional className applied to the trigger. */
  className?: string;
}

/**
 * Reusable export dropdown used by the topology canvas, inspector tabs, and
 * activity log. Renders a list of format options; the caller wires each
 * format to the appropriate helper in `utils/exportUtils.ts`.
 */
export function ExportDropdown({
  options,
  buttonLabel = 'Export',
  className,
}: ExportDropdownProps) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Close on outside click.
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as globalThis.Node)) {
        setOpen(false);
      }
    };
    window.addEventListener('mousedown', handler);
    return () => window.removeEventListener('mousedown', handler);
  }, [open]);

  return (
    <div ref={containerRef} className={cn('tw-relative', className)}>
      <button
        onClick={() => setOpen((v) => !v)}
        className="tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-border tw-border-border tw-bg-bg tw-text-text hover:tw-bg-panel tw-text-sm tw-transition-colors"
        aria-haspopup="menu"
        aria-expanded={open}
      >
        <Download className="tw-h-3.5 tw-w-3.5" />
        <span>{buttonLabel}</span>
        <ChevronDown className="tw-h-3 tw-w-3" />
      </button>
      {open && (
        <div
          role="menu"
          className="tw-absolute tw-top-full tw-right-0 tw-mt-1 tw-w-56 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-z-50 tw-py-1 tw-animate-fade-in"
        >
          {options.map((opt) => (
            <button
              key={opt.id}
              role="menuitem"
              onClick={async () => {
                setOpen(false);
                await opt.onSelect();
              }}
              className="tw-w-full tw-px-3 tw-py-1.5 tw-text-left tw-text-sm tw-text-text hover:tw-bg-bg tw-transition-colors"
            >
              <div>{opt.label}</div>
              {opt.hint && <div className="tw-text-[10px] tw-text-muted">{opt.hint}</div>}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
