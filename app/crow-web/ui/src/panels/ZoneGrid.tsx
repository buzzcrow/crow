// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useRef, useEffect, useState, useCallback } from 'react';
import type { ZoneUsageDto } from '../types';

interface ZoneGridProps {
  zones: ZoneUsageDto[];
  onZoneClick?: (zone: ZoneUsageDto) => void;
}

function busyColor(pct: number): string {
  if (pct < 30) return '#22c55e';
  if (pct < 60) return '#eab308';
  if (pct < 85) return '#f97316';
  return '#ef4444';
}

export function ZoneGrid({ zones, onZoneClick }: ZoneGridProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [hoverZone, setHoverZone] = useState<{ index: number; pct: number; x: number; y: number } | null>(null);
  const [selectedZone, setSelectedZone] = useState<number | null>(null);

  const gridSize = Math.max(1, Math.ceil(Math.sqrt(zones.length)));
  const cellSize = 10;
  const gap = 1;
  const canvasSize = gridSize * (cellSize + gap) + gap;

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    for (let i = 0; i < zones.length; i++) {
      const row = Math.floor(i / gridSize);
      const col = i % gridSize;
      const x = gap + col * (cellSize + gap);
      const y = gap + row * (cellSize + gap);
      const pct = zones[i].capacity_bytes > 0
        ? Math.round((zones[i].busy_bytes / zones[i].capacity_bytes) * 100)
        : 0;
      ctx.fillStyle = busyColor(pct);
      ctx.fillRect(x, y, cellSize, cellSize);
      if (selectedZone === i) {
        ctx.strokeStyle = '#3b82f6';
        ctx.lineWidth = 2;
        ctx.strokeRect(x - 0.5, y - 0.5, cellSize + 1, cellSize + 1);
      }
    }
  }, [zones, gridSize, selectedZone]);

  useEffect(() => { draw(); }, [draw]);

  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    const col = Math.floor((x - gap) / (cellSize + gap));
    const row = Math.floor((y - gap) / (cellSize + gap));
    const idx = row * gridSize + col;
    if (idx >= 0 && idx < zones.length) {
      const pct = zones[idx].capacity_bytes > 0
        ? Math.round((zones[idx].busy_bytes / zones[idx].capacity_bytes) * 100)
        : 0;
      setHoverZone({ index: idx, pct, x, y });
    } else {
      setHoverZone(null);
    }
  };

  const handleMouseLeave = () => setHoverZone(null);

  const handleClick = () => {
    if (hoverZone) {
      setSelectedZone(hoverZone.index);
      onZoneClick?.(zones[hoverZone.index]);
    }
  };

  return (
    <div className="tw-relative tw-inline-block">
      <canvas
        ref={canvasRef}
        width={canvasSize}
        height={canvasSize}
        className="tw-border tw-border-border tw-rounded tw-cursor-pointer"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
      />
      {hoverZone && (
        <div
          className="tw-absolute tw-pointer-events-none tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text tw-z-10"
          style={{ left: hoverZone.x + 14, top: hoverZone.y + 14 }}
        >
          Zone {hoverZone.index} · {hoverZone.pct}% busy
        </div>
      )}
    </div>
  );
}
