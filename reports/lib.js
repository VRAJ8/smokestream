const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun,
  WidthType, ShadingType, AlignmentType, BorderStyle, LevelFormat, Footer, PageNumber,
} = require('docx');

const FONT = 'Calibri';
const BODY = 20;      // 10pt
const SMALL = 17;     // 8.5pt
const ACCENT = '1F3864';
const MARGIN_LR = 1008; // 0.7"
const MARGIN_TB = 936;  // 0.65"
const CONTENT_W = 12240 - 2 * MARGIN_LR;

// Parse **bold** and *italic* markers into TextRuns.
function runs(text, size = BODY, extra = {}) {
  const out = [];
  const re = /(\*\*[^*]+\*\*|\*[^*]+\*)/g;
  let last = 0, m;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), size, font: FONT, ...extra }));
    const t = m[0];
    if (t.startsWith('**')) out.push(new TextRun({ text: t.slice(2, -2), bold: true, size, font: FONT, ...extra }));
    else out.push(new TextRun({ text: t.slice(1, -1), italics: true, size, font: FONT, ...extra }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), size, font: FONT, ...extra }));
  return out;
}

const P = (text, opts = {}) => new Paragraph({
  children: runs(text, opts.size || BODY),
  spacing: { after: opts.after ?? 50, line: 235 },
  alignment: opts.align || AlignmentType.JUSTIFIED,
});

const H = (text) => new Paragraph({
  children: [new TextRun({ text, bold: true, size: 22, font: FONT, color: ACCENT })],
  spacing: { before: 90, after: 40 },
  keepNext: true,
});

const BUL = (text) => new Paragraph({
  children: runs(text),
  numbering: { reference: 'bul', level: 0 },
  spacing: { after: 25, line: 235 },
  alignment: AlignmentType.LEFT,
});

const border = { style: BorderStyle.SINGLE, size: 4, color: 'B4BCCB' };
const borders = { top: border, bottom: border, left: border, right: border };

function TABLE(rows, widthsFrac, caption) {
  const widths = widthsFrac.map((f) => Math.round(f * CONTENT_W));
  widths[widths.length - 1] = CONTENT_W - widths.slice(0, -1).reduce((a, b) => a + b, 0);
  const trs = rows.map((r, i) => new TableRow({
    tableHeader: i === 0,
    cantSplit: true,
    children: r.map((cell, j) => new TableCell({
      width: { size: widths[j], type: WidthType.DXA },
      borders,
      shading: i === 0 ? { fill: 'DCE3EE', type: ShadingType.CLEAR, color: 'auto' } : undefined,
      margins: { top: 30, bottom: 30, left: 70, right: 70 },
      children: [new Paragraph({
        children: i === 0 ? [new TextRun({ text: cell, bold: true, size: SMALL, font: FONT })] : runs(cell, SMALL),
        spacing: { after: 0, line: 228 },
        alignment: j === 0 ? AlignmentType.LEFT : AlignmentType.CENTER,
      })],
    })),
  }));
  const out = [];
  if (caption) out.push(new Paragraph({
    children: runs(caption, SMALL), spacing: { before: 40, after: 30 }, keepNext: true,
  }));
  out.push(new Table({ width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: widths, rows: trs }));
  out.push(new Paragraph({ children: [], spacing: { after: 30 } }));
  return out;
}

// Image from a PNG, scaled to widthIn inches, keeping its aspect ratio.
function FIG(path, widthIn, caption) {
  const buf = fs.readFileSync(path);
  const w = buf.readUInt32BE(16), h = buf.readUInt32BE(20); // PNG IHDR
  const px = widthIn * 96;
  return [
    new Paragraph({
      children: [new ImageRun({ type: 'png', data: buf, transformation: { width: px, height: px * h / w },
                                altText: { title: caption, description: caption, name: 'figure' } })],
      alignment: AlignmentType.CENTER, spacing: { before: 40, after: 20 },
    }),
    new Paragraph({ children: runs(caption, SMALL), spacing: { after: 60 }, alignment: AlignmentType.JUSTIFIED }),
  ];
}

function header(title, subtitle, team) {
  return [
    new Paragraph({
      children: [new TextRun({ text: title, bold: true, size: 28, font: FONT, color: ACCENT })],
      alignment: AlignmentType.CENTER, spacing: { after: 30 },
    }),
    new Paragraph({
      children: [new TextRun({ text: subtitle, size: 18, font: FONT })],
      alignment: AlignmentType.CENTER, spacing: { after: 20 },
    }),
    new Paragraph({
      children: [new TextRun({ text: team, bold: true, size: 18, font: FONT })],
      alignment: AlignmentType.CENTER, spacing: { after: 80 },
      border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: ACCENT, space: 4 } },
    }),
  ];
}

async function build(file, title, children) {
  const doc = new Document({
    creator: 'Vraj Patel, Brandon Moriguchi',
    lastModifiedBy: 'Vraj Patel',
    title,
    description: 'DATA 226 Project',
    styles: { default: { document: { run: { font: FONT, size: BODY } } } },
    numbering: { config: [{ reference: 'bul', levels: [{ level: 0, format: LevelFormat.BULLET, text: '•', alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 300, hanging: 200 } } } }] }] },
    sections: [{
      properties: { page: { size: { width: 12240, height: 15840 }, margin: { top: MARGIN_TB, bottom: MARGIN_TB, left: MARGIN_LR, right: MARGIN_LR } } },
      footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ children: [PageNumber.CURRENT], size: 16, font: FONT })] })] }) },
      children,
    }],
  });
  fs.writeFileSync(file, await Packer.toBuffer(doc));
  console.log('wrote', file);
}

module.exports = { P, H, BUL, TABLE, FIG, header, build, CONTENT_W };
