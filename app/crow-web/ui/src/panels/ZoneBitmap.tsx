// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useRef, useEffect, useState, useCallback } from 'react';

interface ZoneBitmapProps {
  /** Hex-encoded bitmap string from the API. Each bit = 1 unit. */
  usageBitmap?: string;
  /** Total number of units in the zone. */
  totalUnits: number;
}

/**
 * Renders a zone's unit-level bitmap as a canvas grid. Busy = red,
 * free = green. Uses an offscreen canvas for double-buffering to
 * avoid flicker on re-draw.
 */
export function ZoneBitmap({ usageBitmap, totalUnits }: ZoneBitmapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const offscreenRef = useRef<HTMLCanvasElement | null>(null);
  const [hover, setHover] = useState<{ offset: number; busy: boolean; x: number; y: number } | null>(null);

  const gridSize = Math.max(1, Math.ceil(Math.sqrt(Math.max(totalUnits, 1))));
  const cellSize = 4;
  const gap = 0;
  const canvasSize = gridSize * (cellSize + gap);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Draw to offscreen canvas first.
    if (!offscreenRef.current) {
      offscreenRef.current = document.createElement('canvas');
    }
    const off = offscreenRef.current;
    off.width = canvas.width;
    off.height = canvas.height;
    const offCtx = off.getContext('2d');
    if (!offCtx) return;
    offCtx.fillStyle = '#1a1a2e';
    offCtx.fillRect(0, 0, off.width, off.height);

    // Parse hex bitmap: each hex char = 4 bits.
    const bits: boolean[] = [];
    if (usageBitmap) {
      for (const ch of usageBitmap) {
        const val = parseInt(ch, 16);
        if (isNaN(val)) continue;
        for (let b = 3; b >= 0; b--) {
          bits.push(((val >> b) & 1) === 1);
        }
      }
    }

    for (let i = 0; i < totalUnits; i++) {
      const row = Math.floor(i / gridSize);
      const col = i % gridSize;
      const x = col * (cellSize + gap);
      const y = row * (cellSize + gap);
      offCtx.fillStyle = bits[i] ? '#ef4444' : '#22c55e';
      offCtx.fillRect(x, y, cellSize, cellSize);
    }

    // Blit to visible canvas.
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(off, 0, 0);
  }, [usageBitmap, totalUnits, gridSize]);

  useEffect(() => { draw(); }, [draw]);

  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    const col = Math.floor(x / (cellSize + gap));
    const row = Math.floor(y / (cellSize + gap));
    const offset = row * gridSize + col;
    if (offset >= 0 && offset < totalUnits) {
      // Parse bit at offset.
      let busy = false;
      if (usageBitmap) {
        const charIdx = Math.floor(offset / 4);
        const bitIdx = 3 - (offset % 4);
        if (charIdx < usageBitmap.length) {
          const val = parseInt(usageBitmap[charIdx], 16);
          if (!isNaN(val)) busy = ((val >> bitIdx) & 1) === 1;
        }
      }
      setHover({ offset, busy, x, y });
    } else {
      setHover(null);
    }
  };

  const handleMouseLeave = () => setHover(null);

  return (
    <div className="tw-relative tw-inline-block">
      <canvas
        ref={canvasRef}
        width={canvasSize}
        height={canvasSize}
        className="tw-border tw-border-border tw-rounded"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      />
      {hover && (
        <div
          className="tw-absolute tw-pointer-events-none tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text tw-z-10"
          style={{ left: hover.x + 8, top: hover.y + 8 }}
        >
          Unit {hover.offset} · {hover.busy ? 'busy' : 'free'}
        </div>
      )}
    </div>
  );
}
