import { Node, Edge } from 'reactflow';

/**
 * Export React Flow canvas as SVG
 */
export async function exportAsSVG(
  nodes: Node[],
  edges: Edge[],
  width: number,
  height: number,
  filename: string = 'topology.svg'
): Promise<void> {
  // Create SVG element
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('width', width.toString());
  svg.setAttribute('height', height.toString());
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.setAttribute('xmlns', 'http://www.w3.org/2000/svg');

  // Add background
  const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
  rect.setAttribute('width', '100%');
  rect.setAttribute('height', '100%');
  rect.setAttribute('fill', '#0b0d10');
  svg.appendChild(rect);

  // Add edges
  edges.forEach(edge => {
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', edge.data?.path || '');
    path.setAttribute('stroke', edge.data?.stroke || '#4c566a');
    path.setAttribute('stroke-width', '2');
    path.setAttribute('fill', 'none');
    svg.appendChild(path);
  });

  // Add nodes
  nodes.forEach(node => {
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', node.position.x.toString());
    rect.setAttribute('y', node.position.y.toString());
    rect.setAttribute('width', node.width?.toString() || '150');
    rect.setAttribute('height', node.height?.toString() || '50');
    rect.setAttribute('fill', node.data?.color || '#161a1f');
    rect.setAttribute('stroke', node.data?.stroke || '#2e3440');
    rect.setAttribute('rx', '8');
    svg.appendChild(rect);

    // Add node label
    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    text.setAttribute('x', (node.position.x + 10).toString());
    text.setAttribute('y', (node.position.y + 30).toString());
    text.setAttribute('fill', '#d8dee9');
    text.setAttribute('font-size', '14');
    text.textContent = node.data?.label || '';
    svg.appendChild(text);
  });

  // Convert to blob and download
  const svgData = new XMLSerializer().serializeToString(svg);
  const blob = new Blob([svgData], { type: 'image/svg+xml' });
  const url = URL.createObjectURL(blob);
  downloadFile(url, filename);
}

/**
 * Export React Flow canvas as PNG
 */
export async function exportAsPNG(
  nodes: Node[],
  edges: Edge[],
  width: number,
  height: number,
  filename: string = 'topology.png'
): Promise<void> {
  // Create canvas
  const canvas = document.createElement('canvas');
  const scale = 2; // High resolution
  canvas.width = width * scale;
  canvas.height = height * scale;
  const ctx = canvas.getContext('2d');

  if (!ctx) throw new Error('Could not get canvas context');

  ctx.scale(scale, scale);
  ctx.fillStyle = '#0b0d10';
  ctx.fillRect(0, 0, width, height);

  // Draw edges
  edges.forEach(edge => {
    ctx.strokeStyle = edge.data?.stroke || '#4c566a';
    ctx.lineWidth = 2;
    // Simple path rendering
    if (edge.data?.path) {
      const path = new Path2D(edge.data.path);
      ctx.stroke(path);
    }
  });

  // Draw nodes
  nodes.forEach(node => {
    ctx.fillStyle = node.data?.color || '#161a1f';
    ctx.strokeStyle = node.data?.stroke || '#2e3440';
    ctx.lineWidth = 2;
    roundRect(ctx, node.position.x, node.position.y, node.width || 150, node.height || 50, 8);
    ctx.fill();
    ctx.stroke();

    // Draw label
    ctx.fillStyle = '#d8dee9';
    ctx.font = '14px sans-serif';
    ctx.fillText(node.data?.label || '', node.position.x + 10, node.position.y + 30);
  });

  // Convert to blob and download
  const blob = await new Promise<Blob>((resolve) => canvas.toBlob(resolve as any, 'image/png', 1.0));
  const url = URL.createObjectURL(blob);
  downloadFile(url, filename);
}

/**
 * Export data as CSV
 */
export function exportAsCSV<T>(
  data: T[],
  headers: { key: keyof T; label: string }[],
  filename: string = 'export.csv'
): void {
  const csvRows: string[] = [];

  // Add headers
  csvRows.push(headers.map(h => `"${h.label}"`).join(','));

  // Add rows
  data.forEach(row => {
    const values = headers.map(h => {
      const value = row[h.key];
      // Handle strings with quotes
      return `"${String(value).replace(/"/g, '""')}"`;
    });
    csvRows.push(values.join(','));
  });

  const csvString = csvRows.join('\n');
  const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  downloadFile(url, filename);
}

/**
 * Export data as JSON
 */
export function exportAsJSON<T>(data: T, filename: string = 'export.json'): void {
  const jsonString = JSON.stringify(data, null, 2);
  const blob = new Blob([jsonString], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  downloadFile(url, filename);
}

/**
 * Generate and export cluster health report as PDF
 */
export async function generateHealthReport(
  clusterName: string = 'CrowKV Cluster',
  healthStatus: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown',
  entityStats: { type: string; count: number; healthy: number }[],
  metricsData: any[],
  filename: string = 'cluster-health-report.pdf'
): Promise<void> {
  // Lazy-load jsPDF + autotable so the ~150 KB chunk is excluded from the
  // initial bundle and only fetched when the user actually exports a PDF.
  const [{ jsPDF }, autoTableMod] = await Promise.all([
    import('jspdf'),
    import('jspdf-autotable'),
  ]);
  const autoTable = autoTableMod.default;
  const doc = new jsPDF();

  // Add title
  doc.setFontSize(20);
  doc.text(`${clusterName} Health Report`, 14, 20);

  // Add date
  doc.setFontSize(12);
  doc.text(`Generated on: ${new Date().toLocaleString()}`, 14, 30);

  // Add health status
  doc.setFontSize(16);
  doc.text('Overall Health Status:', 14, 45);

  const statusColor: [number, number, number] = ({
    Healthy: [16, 185, 129],
    Degraded: [245, 158, 11],
    Failed: [239, 68, 68],
    Unknown: [107, 114, 128]
  } as Record<string, [number, number, number]>)[healthStatus] || [107, 114, 128];

  doc.setFillColor(statusColor[0], statusColor[1], statusColor[2]);
  doc.roundedRect(14, 50, 30, 10, 3, 3, 'F');
  doc.setTextColor(255, 255, 255);
  doc.text(healthStatus, 29, 57, { align: 'center' });
  doc.setTextColor(0, 0, 0);

  // Add entity stats table
  doc.setFontSize(16);
  doc.text('Entity Health Summary', 14, 75);

  autoTable(doc, {
    startY: 80,
    head: [['Entity Type', 'Total Count', 'Healthy Count', 'Health Percentage']],
    body: entityStats.map(stat => [
      stat.type,
      stat.count.toString(),
      stat.healthy.toString(),
      `${Math.round((stat.healthy / stat.count) * 100)}%`
    ]),
    theme: 'striped',
  });

  // Add metrics section
  if (metricsData.length > 0) {
    const finalY = (doc as any).lastAutoTable.finalY || 120;
    doc.setFontSize(16);
    doc.text('Recent Metrics', 14, finalY + 15);

    // Simple metrics table
    autoTable(doc, {
      startY: finalY + 20,
      head: [['Metric', 'Value', 'Timestamp']],
      body: metricsData.slice(0, 10).map((m: any) => [m.name, m.value, new Date(m.timestamp).toLocaleString()]),
      theme: 'striped',
    });
  }

  // Save the PDF
  doc.save(filename);
}

/**
 * Helper function to download a file
 */
function downloadFile(url: string, filename: string): void {
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

/**
 * Helper function to draw rounded rectangles on canvas
 */
function roundRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
): void {
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.lineTo(x + width - radius, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
  ctx.lineTo(x + width, y + height - radius);
  ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
  ctx.lineTo(x + radius, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
  ctx.lineTo(x, y + radius);
  ctx.quadraticCurveTo(x, y, x + radius, y);
  ctx.closePath();
}
