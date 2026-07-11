import { useEffect, useMemo, useState, useCallback } from 'react';
import {
  Filter,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Bookmark,
  Plus,
  Trash2,
  X,
} from 'lucide-react';
import { cn } from '../utils/cn';
import { localStorage } from '../utils/localStorage';

export interface FilterDimension {
  /** Stable id used as the key in selectedFilters. */
  id: string;
  /** Human-readable label, e.g. "Status". */
  label: string;
  /** Available option values for this dimension. */
  options: { value: string; label: string }[];
}

export interface SortOption {
  /** Stable id e.g. 'id', 'name', 'health'. */
  id: string;
  label: string;
}

export interface SortState {
  id: string;
  direction: 'asc' | 'desc';
}

export interface FilterPreset {
  id: string;
  name: string;
  filters: Record<string, string[]>;
  sort: SortState;
}

export interface FilterControlsProps {
  /** Optional id used to namespace persisted presets in localStorage. */
  presetNamespace?: string;
  filterDimensions: FilterDimension[];
  sortOptions: SortOption[];
  selectedFilters: Record<string, string[]>;
  selectedSort: SortState;
  onFiltersChange: (filters: Record<string, string[]>) => void;
  onSortChange: (sort: SortState) => void;
  /** Hide the preset bar when not desired. */
  showPresets?: boolean;
}

/**
 * Reusable filter + sort + preset toolbar. Persists presets per namespace.
 *
 * Owns only the popover UI state; the selected filters/sort live in the parent
 * so they can drive the underlying list.
 */
export function FilterControls({
  presetNamespace,
  filterDimensions,
  sortOptions,
  selectedFilters,
  selectedSort,
  onFiltersChange,
  onSortChange,
  showPresets = true,
}: FilterControlsProps) {
  const [openMenu, setOpenMenu] = useState<'filter' | 'sort' | 'presets' | null>(null);

  // Persisted presets, namespaced under filterPresets[<namespace>].
  const [presets, setPresets] = useState<FilterPreset[]>(() => {
    if (!presetNamespace) return [];
    const all = localStorage.get<Record<string, FilterPreset[]>>('filterPresets', {});
    return all[presetNamespace] || [];
  });

  useEffect(() => {
    if (!presetNamespace) return;
    const all = localStorage.get<Record<string, FilterPreset[]>>('filterPresets', {});
    all[presetNamespace] = presets;
    localStorage.set('filterPresets', all);
  }, [presets, presetNamespace]);

  const activeFilterCount = useMemo(
    () => Object.values(selectedFilters).reduce((acc, vals) => acc + vals.length, 0),
    [selectedFilters],
  );

  const toggleFilterValue = useCallback(
    (dimensionId: string, value: string) => {
      const current = selectedFilters[dimensionId] || [];
      const next = current.includes(value)
        ? current.filter((v) => v !== value)
        : [...current, value];
      onFiltersChange({ ...selectedFilters, [dimensionId]: next });
    },
    [selectedFilters, onFiltersChange],
  );

  const clearFilters = useCallback(() => {
    onFiltersChange({});
  }, [onFiltersChange]);

  const savePreset = useCallback(() => {
    const name = window.prompt('Preset name:');
    if (!name) return;
    const preset: FilterPreset = {
      id: `preset-${Date.now()}`,
      name,
      filters: { ...selectedFilters },
      sort: { ...selectedSort },
    };
    setPresets((prev) => [...prev, preset]);
  }, [selectedFilters, selectedSort]);

  const applyPreset = useCallback(
    (preset: FilterPreset) => {
      onFiltersChange(preset.filters);
      onSortChange(preset.sort);
      setOpenMenu(null);
    },
    [onFiltersChange, onSortChange],
  );

  const deletePreset = useCallback((presetId: string) => {
    setPresets((prev) => prev.filter((p) => p.id !== presetId));
  }, []);

  const currentSort = sortOptions.find((s) => s.id === selectedSort.id) || sortOptions[0];

  return (
    <div className="tw-flex tw-items-center tw-gap-2 tw-text-sm">
      {/* Filter dropdown */}
      <div className="tw-relative">
        <button
          onClick={() => setOpenMenu(openMenu === 'filter' ? null : 'filter')}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-border tw-border-border tw-transition-colors',
            activeFilterCount > 0
              ? 'tw-bg-accent/10 tw-text-accent tw-border-accent/30'
              : 'tw-bg-bg tw-text-text hover:tw-bg-panel',
          )}
          aria-label="Filter"
          aria-expanded={openMenu === 'filter'}
        >
          <Filter className="tw-h-3.5 tw-w-3.5" />
          <span>Filter</span>
          {activeFilterCount > 0 && (
            <span className="tw-text-xs tw-px-1.5 tw-py-0.5 tw-rounded tw-bg-accent tw-text-white">
              {activeFilterCount}
            </span>
          )}
        </button>
        {openMenu === 'filter' && (
          <div className="tw-absolute tw-top-full tw-left-0 tw-mt-1 tw-w-64 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-z-50 tw-p-3 tw-animate-fade-in">
            {filterDimensions.length === 0 ? (
              <div className="tw-text-muted tw-text-xs">No filters available.</div>
            ) : (
              <div className="tw-space-y-3">
                {filterDimensions.map((dim) => (
                  <div key={dim.id}>
                    <div className="tw-text-xs tw-uppercase tw-tracking-wider tw-font-semibold tw-text-muted tw-mb-1">
                      {dim.label}
                    </div>
                    <div className="tw-flex tw-flex-wrap tw-gap-1">
                      {dim.options.map((opt) => {
                        const checked = (selectedFilters[dim.id] || []).includes(opt.value);
                        return (
                          <label
                            key={opt.value}
                            className={cn(
                              'tw-flex tw-items-center tw-gap-1.5 tw-px-2 tw-py-1 tw-rounded tw-text-xs tw-cursor-pointer tw-border tw-transition-colors',
                              checked
                                ? 'tw-bg-accent/10 tw-text-accent tw-border-accent/30'
                                : 'tw-bg-bg tw-border-border tw-text-text hover:tw-bg-panel',
                            )}
                          >
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={() => toggleFilterValue(dim.id, opt.value)}
                              className="tw-sr-only"
                            />
                            <span>{opt.label}</span>
                          </label>
                        );
                      })}
                    </div>
                  </div>
                ))}
                {activeFilterCount > 0 && (
                  <button
                    onClick={clearFilters}
                    className="tw-text-xs tw-text-muted hover:tw-text-text tw-flex tw-items-center tw-gap-1"
                  >
                    <X className="tw-h-3 tw-w-3" /> Clear all
                  </button>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Sort dropdown */}
      <div className="tw-relative">
        <button
          onClick={() => setOpenMenu(openMenu === 'sort' ? null : 'sort')}
          className="tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-border tw-border-border tw-bg-bg tw-text-text hover:tw-bg-panel tw-transition-colors"
          aria-label="Sort"
          aria-expanded={openMenu === 'sort'}
        >
          <ArrowUpDown className="tw-h-3.5 tw-w-3.5" />
          <span>{currentSort?.label || 'Sort'}</span>
          {selectedSort.direction === 'asc' ? (
            <ArrowUp className="tw-h-3 tw-w-3" />
          ) : (
            <ArrowDown className="tw-h-3 tw-w-3" />
          )}
        </button>
        {openMenu === 'sort' && (
          <div className="tw-absolute tw-top-full tw-left-0 tw-mt-1 tw-w-48 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-z-50 tw-py-1 tw-animate-fade-in">
            {sortOptions.map((opt) => {
              const isActive = opt.id === selectedSort.id;
              return (
                <button
                  key={opt.id}
                  onClick={() => {
                    if (isActive) {
                      onSortChange({
                        id: opt.id,
                        direction: selectedSort.direction === 'asc' ? 'desc' : 'asc',
                      });
                    } else {
                      onSortChange({ id: opt.id, direction: 'asc' });
                    }
                  }}
                  className={cn(
                    'tw-w-full tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-1.5 tw-text-left tw-text-sm tw-transition-colors',
                    isActive ? 'tw-bg-accent/10 tw-text-accent' : 'tw-text-text hover:tw-bg-bg',
                  )}
                >
                  <span>{opt.label}</span>
                  {isActive &&
                    (selectedSort.direction === 'asc' ? (
                      <ArrowUp className="tw-h-3 tw-w-3" />
                    ) : (
                      <ArrowDown className="tw-h-3 tw-w-3" />
                    ))}
                </button>
              );
            })}
          </div>
        )}
      </div>

      {/* Presets */}
      {showPresets && presetNamespace && (
        <div className="tw-relative">
          <button
            onClick={() => setOpenMenu(openMenu === 'presets' ? null : 'presets')}
            className="tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-border tw-border-border tw-bg-bg tw-text-text hover:tw-bg-panel tw-transition-colors"
            aria-label="Presets"
            aria-expanded={openMenu === 'presets'}
          >
            <Bookmark className="tw-h-3.5 tw-w-3.5" />
            <span>Presets</span>
            {presets.length > 0 && (
              <span className="tw-text-xs tw-text-muted">({presets.length})</span>
            )}
          </button>
          {openMenu === 'presets' && (
            <div className="tw-absolute tw-top-full tw-left-0 tw-mt-1 tw-w-56 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-z-50 tw-py-1 tw-animate-fade-in">
              <button
                onClick={savePreset}
                className="tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-text-left tw-text-sm tw-text-text hover:tw-bg-bg tw-transition-colors"
              >
                <Plus className="tw-h-3.5 tw-w-3.5" />
                Save current as preset
              </button>
              {presets.length > 0 && (
                <>
                  <div className="tw-border-t tw-border-border tw-my-1" />
                  {presets.map((preset) => (
                    <div
                      key={preset.id}
                      className="tw-flex tw-items-center tw-justify-between tw-group hover:tw-bg-bg"
                    >
                      <button
                        onClick={() => applyPreset(preset)}
                        className="tw-flex-1 tw-px-3 tw-py-1.5 tw-text-left tw-text-sm tw-text-text tw-truncate"
                      >
                        {preset.name}
                      </button>
                      <button
                        onClick={() => deletePreset(preset.id)}
                        className="tw-opacity-0 group-hover:tw-opacity-100 tw-px-2 tw-text-muted hover:tw-text-failed tw-transition-opacity"
                        aria-label={`Delete preset ${preset.name}`}
                      >
                        <Trash2 className="tw-h-3 tw-w-3" />
                      </button>
                    </div>
                  ))}
                </>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
