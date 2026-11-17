// SmokeStream presentation. Usage: node build_slides.js <output.pptx>
const path = require("path");
const fs = require("fs");
const pptxgen = require("pptxgenjs");
const React = require("react");
const ReactDOMServer = require("react-dom/server");
const sharp = require("sharp");
const JSZip = require("jszip");
const fa = require("react-icons/fa");

const FIG = path.join(__dirname, "..", "figures");
const OUT = process.argv[2] || path.join(__dirname, "SmokeStream_Slides.pptx");

const THEME = {
  name: "SmokeStream",
  headFontFace: "Cambria",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "22252E", lt1: "FFFFFF", dk2: "3A3F4B", lt2: "EEF0F3",
    accent1: "E4572E", accent2: "2A9D8F", accent3: "F3A712", accent4: "6C7A89",
    accent5: "8E5572", accent6: "4F6D7A", hlink: "2A9D8F", folHlink: "8E5572",
  },
};
const HEX = THEME.colors;

async function icon(Comp, color, size = 256) {
  const svg = ReactDOMServer.renderToStaticMarkup(React.createElement(Comp, { color: "#" + color, size: String(size) }));
  const buf = await sharp(Buffer.from(svg)).png().toBuffer();
  return "image/png;base64," + buf.toString("base64");
}

async function main() {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_16x9"; // 10 x 5.625 in
  pres.title = "SmokeStream";
  pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
  const C = pres.SchemeColor;

  pres.defineSlideMaster({
    title: "TITLE_DARK",
    background: { color: C.text1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.6, y: 1.35, w: 8.8, h: 1.7, fontSize: 36, bold: true,
          color: C.background1, valign: "bottom", fontFace: THEME.headFontFace }, text: "" } },
      { placeholder: { options: { name: "body", type: "body", x: 0.6, y: 3.2, w: 8.8, h: 1.6, fontSize: 16,
          color: C.background2, valign: "top" }, text: "" } },
    ],
  });
  pres.defineSlideMaster({
    title: "CONTENT",
    background: { color: C.background1 },
    margin: [0.5, 0.5, 0.5, 0.5],
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.5, y: 0.25, w: 9.0, h: 0.75, fontSize: 28, bold: true,
          color: C.text1, valign: "middle", fontFace: THEME.headFontFace }, text: "" } },
      { text: { text: "SmokeStream  |  DATA 226, San José State University", options: { x: 0.5, y: 5.25, w: 6, h: 0.25,
          fontSize: 9, color: C.accent4 } } },
    ],
    slideNumber: { x: 9.0, y: 5.25, w: 0.5, h: 0.25, fontSize: 9, color: C.accent4, align: "right" },
  });

  const ICON = {};
  for (const [k, comp, col] of [
    ["clock", fa.FaClock, HEX.accent1], ["plug", fa.FaPlug, HEX.accent1], ["sat", fa.FaSatellite, HEX.accent1],
    ["q1", fa.FaBalanceScale, HEX.lt1], ["q2", fa.FaSlidersH, HEX.lt1], ["q3", fa.FaFire, HEX.lt1], ["q4", fa.FaTachometerAlt, HEX.lt1],
    ["filter", fa.FaFilter, HEX.lt1], ["hash", fa.FaHashtag, HEX.lt1], ["window", fa.FaStream, HEX.lt1], ["dice", fa.FaDiceFive, HEX.lt1],
    ["check", fa.FaCheckCircle, HEX.accent2], ["warn", fa.FaExclamationTriangle, HEX.accent1],
    ["l1", fa.FaVial, HEX.lt1], ["l2", fa.FaLayerGroup, HEX.lt1], ["l3", fa.FaRulerCombined, HEX.lt1],
    ["l4", fa.FaSyncAlt, HEX.lt1], ["l5", fa.FaDatabase, HEX.lt1], ["l6", fa.FaCoins, HEX.lt1],
    ["play", fa.FaPlay, HEX.lt1], ["github", fa.FaGithub, HEX.lt1],
  ]) ICON[k] = await icon(comp, col);

  let n = 0;
  const content = (title, notes) => {
    const s = pres.addSlide({ masterName: "CONTENT" });
    s.addText(title, { placeholder: "title" });
    if (notes) s.addNotes(notes);
    n += 1;
    return s;
  };
  const circleIcon = (s, data, x, y, d, fill, name) => {
    s.addShape(pres.shapes.OVAL, { x, y, w: d, h: d, fill: { color: fill }, line: { color: fill }, objectName: name + " circle" });
    s.addImage({ data, x: x + d * 0.25, y: y + d * 0.25, w: d * 0.5, h: d * 0.5, objectName: name + " icon" });
  };
  const card = (s, x, y, w, h, name) => s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.08,
    fill: { color: C.background2 }, line: { color: C.background2 }, objectName: name });
  const body = (s, text, opts) => s.addText(text, { isTextBox: true, fontSize: 14, color: C.text2, valign: "top", margin: 0, ...opts });
  const stat = (s, big, small, x, y, w, color, name, size = 40) => {
    s.addText(big, { isTextBox: true, x, y, w, h: 0.75, fontSize: size, bold: true, color, fontFace: THEME.headFontFace, margin: 0, objectName: name });
    s.addText(small, { isTextBox: true, x, y: y + 0.75, w, h: 0.6, fontSize: 12, color: C.text2, margin: 0, valign: "top" });
  };

  // 1. Title
  let s = pres.addSlide({ masterName: "TITLE_DARK" });
  s.addText("How Fast Can a Smoke Alert Be?", { placeholder: "title" });
  s.addText([
    { text: "The latency–accuracy trade-off of real-time wildfire smoke detection from crowdsourced sensor streams", options: { breakLine: true } },
    { text: " ", options: { breakLine: true, fontSize: 8 } },
    { text: "Vraj Patel  ·  Brandon Moriguchi  ·  DATA 226, Fall 2026", options: { fontSize: 13, color: C.accent3 } },
  ], { placeholder: "body" });
  s.addNotes("Title. One-line framing: smoke maps are real-time systems, and nobody has measured what deciding in real time costs.");

  // 2. Problem
  s = content("Smoke maps make hidden real-time choices",
    "September 9, 2020: orange sky. People checked PurpleAir maps, not the 20 regulatory monitors. Every real-time map makes choices users never see. " +
    "Corrections were validated on complete data; we measure the cost of deciding while data is late, missing and stale.");
  stat(s, "826 µg/m³", "hourly PM2.5 beside the CZU fire, Sept 2020: 15× the Unhealthy threshold", 0.5, 1.3, 3.6, C.accent1, "Peak stat");
  body(s, "Correction and quality-check methods for PurpleAir were validated on complete, after-the-fact data. A live system has to decide while:", { x: 0.5, y: 2.85, w: 3.6, h: 1.6 });
  [["clock", "Readings arrive late", "median 80 s, measured from 200 live sensors"],
   ["plug", "Sensors drop out", "98 outages in 207 polls"],
   ["sat", "Fire and plume data are stale", "satellite products lag by hours"]].forEach(([ic, h, t], i) => {
    const y = 1.3 + i * 1.25;
    card(s, 4.6, y, 4.9, 1.05, "Challenge card " + (i + 1));
    s.addImage({ data: ICON[ic], x: 4.8, y: y + 0.27, w: 0.5, h: 0.5, objectName: "Challenge icon " + (i + 1) });
    s.addText(h, { isTextBox: true, x: 5.5, y: y + 0.12, w: 3.8, h: 0.4, fontSize: 16, bold: true, color: C.text1, margin: 0 });
    s.addText(t, { isTextBox: true, x: 5.5, y: y + 0.52, w: 3.8, h: 0.4, fontSize: 13, color: C.text2, margin: 0 });
  });

  // 3. Research questions
  s = content("Four research questions",
    "RQ1 the cost; RQ2 which knob; RQ3 can context separate smoke from fireworks; RQ4 can a laptop keep up.");
  [["q1", "RQ1  Cost of real time", "How much accuracy does streaming lose against full hindsight on the same data?"],
   ["q2", "RQ2  The frontier", "How do window, watermark, QC history and context freshness trade latency for accuracy?"],
   ["q3", "RQ3  Smoke or local?", "Can fire, plume and wind context separate wildfire smoke from fireworks?"],
   ["q4", "RQ4  Performance", "Can Kafka + Spark sustain accelerated replay within a latency target on one laptop?"]].forEach(([ic, h, t], i) => {
    const x = 0.5 + (i % 2) * 4.6, y = 1.25 + Math.floor(i / 2) * 1.95;
    card(s, x, y, 4.4, 1.75, "RQ card " + (i + 1));
    circleIcon(s, ICON[ic], x + 0.25, y + 0.3, 0.75, i === 2 ? C.accent1 : C.accent2, "RQ " + (i + 1));
    s.addText(h, { isTextBox: true, x: x + 1.2, y: y + 0.2, w: 3.0, h: 0.45, fontSize: 17, bold: true, color: C.text1, margin: 0 });
    s.addText(t, { isTextBox: true, x: x + 1.2, y: y + 0.7, w: 3.0, h: 0.9, fontSize: 13, color: C.text2, margin: 0, valign: "top" });
  });

  // 4. Flow schematic
  s = content("System flow: one codebase, two modes",
    "Replay re-sends history with measured delays into Kafka. Spark windows by event time with a watermark, checks neighbours, joins context from Airflow, labels. " +
    "MongoDB Atlas serves live near-me queries. The batch job runs the same code on complete data; Snowflake holds both for comparison. Monitors are held out.");
  const box = (t, x, y, w, h, fill, txt, name, bold) => s.addText(t, { shape: pres.shapes.ROUNDED_RECTANGLE, rectRadius: 0.06, x, y, w, h,
    fill: { color: fill }, line: { color: fill }, fontSize: 11, color: txt, align: "center", valign: "middle", bold: !!bold, margin: 2, objectName: name });
  const arrow = (x1, y1, x2, y2, name, dash) => s.addShape(pres.shapes.LINE, { x: Math.min(x1, x2), y: Math.min(y1, y2), w: Math.abs(x2 - x1), h: Math.abs(y2 - y1),
    flipV: y2 < y1, line: { color: HEX.accent4, width: 1.5, endArrowType: "triangle", dashType: dash ? "dash" : "solid" }, objectName: name });
  box("PurpleAir\nhistory + live poll", 0.5, 1.45, 1.45, 0.75, C.accent4, C.background1, "Flow PurpleAir");
  box("Replay\nmeasured delays", 2.25, 1.45, 1.4, 0.75, C.accent3, C.text1, "Flow Replay");
  box("Kafka", 3.95, 1.45, 1.05, 0.75, C.accent2, C.background1, "Flow Kafka", true);
  box("Spark Structured Streaming\nvalidate · correct · H3 · window +\nwatermark · neighbour QC · label", 5.3, 1.3, 2.5, 1.05, C.accent1, C.background1, "Flow Spark", true);
  box("MongoDB Atlas\nlive near-me", 8.1, 1.45, 1.4, 0.75, C.accent2, C.background1, "Flow Mongo");
  box("FIRMS · HMS ·\nASOS · AirNow", 0.5, 2.85, 1.45, 0.75, C.accent4, C.background1, "Flow sources");
  box("Airflow DAGs", 2.25, 2.85, 1.4, 0.75, C.accent3, C.text1, "Flow Airflow");
  box("Context\nstore", 3.95, 2.85, 1.05, 0.75, C.accent2, C.background1, "Flow context");
  box("Hindsight batch job\nsame code, all data", 5.3, 2.85, 2.5, 0.75, C.accent5, C.background1, "Flow batch", true);
  box("Snowflake\nstar schema, SCD2", 8.1, 2.85, 1.4, 0.75, C.accent2, C.background1, "Flow Snowflake");
  box("Stream summaries: Bloom · FM · DGIM · reservoir", 2.25, 4.15, 2.75, 0.6, C.background2, C.text1, "Flow summaries");
  box("Evaluation vs held-out monitors · LSH · privacy · poisoning", 5.3, 4.15, 4.2, 0.6, C.background2, C.text1, "Flow evaluation");
  arrow(1.95, 1.83, 2.25, 1.83, "Arrow 1"); arrow(3.65, 1.83, 3.95, 1.83, "Arrow 2"); arrow(5.0, 1.83, 5.3, 1.83, "Arrow 3");
  arrow(7.8, 1.83, 8.1, 1.83, "Arrow 4"); arrow(1.95, 3.23, 2.25, 3.23, "Arrow 5"); arrow(3.65, 3.23, 3.95, 3.23, "Arrow 6");
  arrow(5.0, 3.0, 5.3, 2.3, "Arrow 7"); arrow(5.0, 3.23, 5.3, 3.23, "Arrow 8"); arrow(7.8, 3.23, 8.1, 3.23, "Arrow 9");
  arrow(4.47, 2.2, 4.47, 2.85, "Arrow 10", true); arrow(6.55, 3.6, 6.55, 4.15, "Arrow 11"); arrow(6.55, 2.35, 6.55, 2.85, "Arrow 12", true);

  // 5. Data
  s = content("Real data: three fire seasons, measured delays",
    "Ground truth is held out. PurpleAir costs points per row, so we spent them where it matters: the 2020 peak and three 4-day event windows. " +
    "Sensors sampled around monitors because a random sample covered only 5 of 20 monitors.");
  s.addTable([
    [{ text: "Season", options: { bold: true, color: C.background1, fill: { color: C.text1 } } },
     { text: "Unhealthy monitor-hours", options: { bold: true, color: C.background1, fill: { color: C.text1 } } },
     { text: "Peak µg/m³", options: { bold: true, color: C.background1, fill: { color: C.text1 } } },
     { text: "Fire detections", options: { bold: true, color: C.background1, fill: { color: C.text1 } } }],
    ["2020", "3,327", "826", "85,660"], ["2021", "155", "182", "25,014"], ["2024", "12", "222", "2,721"],
  ], { x: 0.5, y: 1.3, w: 5.0, colW: [1.0, 1.6, 1.1, 1.3], fontSize: 13, color: C.text2, border: { type: "solid", color: HEX.lt2, pt: 1 },
       align: "center", valign: "middle", rowH: 0.42, objectName: "Season table" });
  body(s, "Ground truth: EPA AQS monitors (held out)\nContext: NASA FIRMS fires, NOAA HMS plumes, airport wind", { x: 0.5, y: 3.2, w: 5.0, h: 0.8, fontSize: 13 });
  stat(s, "143,748", "PurpleAir readings, 40 sensors, Aug 16 – Sep 15, 2020", 6.0, 1.25, 3.5, C.accent1, "Readings stat");
  stat(s, "3 windows", "July 4 2021 & 2024 (fireworks), Aug 2021 smoke: 26–29 sensors each", 6.0, 2.6, 3.5, C.accent2, "Windows stat");
  stat(s, "80 s", "median arrival delay, measured from 200 live sensors", 6.0, 3.95, 3.5, C.accent3, "Delay stat");

  // 6. Simulator
  s = content("A validated simulator made the sweep possible",
    "Real engine: 0.27 s per 1-minute micro-batch, 6.5 hours per setting. Simulator: seconds, identical output window for window. " +
    "Our equivalence test caught that Spark drops late rows using the previous batch's watermark, not the current one.");
  stat(s, "6.5 h → seconds", "per configuration, identical output window for window", 0.5, 1.3, 4.3, C.accent2, "Simulator stat", 36);
  card(s, 0.5, 2.75, 4.3, 2.15, "Pitfall 1 card");
  s.addImage({ data: ICON.warn, x: 0.7, y: 2.95, w: 0.45, h: 0.45, objectName: "Pitfall 1 icon" });
  s.addText("Pitfall 1: which watermark?", { isTextBox: true, x: 1.3, y: 2.95, w: 3.4, h: 0.45, fontSize: 16, bold: true, color: C.text1, margin: 0 });
  body(s, "Spark (3.4+) drops late rows using the previous batch's watermark and evicts state with the current one. Our automated test against the engine caught it.", { x: 0.7, y: 3.5, w: 3.9, h: 1.3, fontSize: 13 });
  card(s, 5.2, 2.75, 4.3, 2.15, "Pitfall 2 card");
  s.addImage({ data: ICON.warn, x: 5.4, y: 2.95, w: 0.45, h: 0.45, objectName: "Pitfall 2 icon" });
  s.addText("Pitfall 2: batch size", { isTextBox: true, x: 6.0, y: 2.95, w: 3.4, h: 0.45, fontSize: 16, bold: true, color: C.text1, margin: 0 });
  body(s, "Lateness is judged once per micro-batch. Batches spanning hours make every watermark look identical, so we slice replays by simulated minute.", { x: 5.4, y: 3.5, w: 3.9, h: 1.3, fontSize: 13 });
  body(s, "Without these, every RQ2 number would be silently wrong.", { x: 5.2, y: 1.45, w: 4.3, h: 1.0, fontSize: 16, italic: true, color: C.accent1 });

  // 7. RQ1
  s = content("RQ1: late data is almost free",
    "Stream vs hindsight differs by 0.12 µg/m³ at 10-minute windows; sensors vs monitors differ by 17.4. The watermark moves latency by 10 minutes per step without changing accuracy.");
  s.addImage({ path: path.join(FIG, "frontier.png"), x: 0.5, y: 1.15, w: 5.6, h: 3.8, objectName: "Frontier figure",
    altText: "Accuracy against decision latency for each setting and run" });
  stat(s, "0.12", "µg/m³ stream vs hindsight (10-min windows, 2020)", 6.5, 1.25, 3.0, C.accent2, "Stream error stat");
  stat(s, "17.4", "µg/m³ sensors vs regulatory monitors", 6.5, 2.55, 3.0, C.accent4, "Sensor error stat");
  stat(s, "99 min", "median lead of smoke alerts over the official hourly value (88% precise)", 6.5, 3.85, 3.0, C.accent1, "Lead stat");

  // 8. RQ2 context
  s = content("RQ2: stale context costs the most",
    "Refreshing fire and plume context every 6 hours instead of 15 minutes: 2020 label agreement 92% to 84%; July 2021 window 100% to 55%. " +
    "So the expensive knob is orchestration (Airflow schedule), not the watermark.");
  s.addImage({ path: path.join(FIG, "context_refresh.png"), x: 0.5, y: 1.15, w: 5.6, h: 3.46, objectName: "Context figure",
    altText: "Label agreement falls as context refresh interval grows" });
  stat(s, "92% → 84%", "label agreement, 2020, context every 15 min vs 6 h", 6.5, 1.3, 3.0, C.accent1, "Context 2020 stat", 30);
  stat(s, "100% → 55%", "label agreement, July 2021 window", 6.5, 2.7, 3.0, C.accent1, "Context July stat", 30);
  body(s, "The watermark buys nothing past a few minutes; the Airflow schedule is the knob that matters.", { x: 6.5, y: 4.05, w: 3.0, h: 0.9, fontSize: 13, italic: true });

  // 9. RQ3
  s = content("RQ3: fireworks vs smoke (preliminary)",
    "Pipeline labels, not yet accuracy: hand labels by two labelers with Cohen's kappa are in progress. 2021: sparse sensors give no extent signal, so uncertain. " +
    "2024: fires and plumes elsewhere made fireworks look like smoke.");
  [["July 4, 2021", "15 of 16", "events labeled uncertain: too few sensors to judge extent", C.accent3],
   ["July 4, 2024", "13 of 14", "events labeled wildfire: fires and plumes elsewhere made fireworks look like smoke", C.accent1],
   ["Aug 2021 smoke", "88% / 49%", "of Unhealthy monitor-hours caught / alert precision, median 169 min early", C.accent2]].forEach(([h, big, t, col], i) => {
    const x = 0.5 + i * 3.07;
    card(s, x, 1.3, 2.86, 3.0, "RQ3 card " + (i + 1));
    s.addText(h, { isTextBox: true, x: x + 0.2, y: 1.45, w: 2.5, h: 0.4, fontSize: 16, bold: true, color: C.text1, margin: 0 });
    s.addText(big, { isTextBox: true, x: x + 0.2, y: 1.95, w: 2.5, h: 0.8, fontSize: 34, bold: true, color: col, margin: 0, fontFace: THEME.headFontFace });
    s.addText(t, { isTextBox: true, x: x + 0.2, y: 2.85, w: 2.5, h: 1.3, fontSize: 13, color: C.text2, margin: 0, valign: "top" });
  });
  body(s, "Next: 100 events hand-labeled blind by two people; agreement measured with Cohen's kappa.", { x: 0.5, y: 4.5, w: 9.0, h: 0.5, fontSize: 13, italic: true });

  // 10. RQ4
  s = content("RQ4: one laptop keeps up",
    "Synthetic: 641 messages/s at 300x real time, p95 11.5 s. Real data into Atlas: median 16.4 s, of which 9.5 s is designed waiting for the watermark and about 7 s is the system.");
  s.addChart(pres.charts.BAR, [
    { name: "Designed wait (watermark)", labels: ["Median", "95th pct."], values: [9.47, 9.67] },
    { name: "System overhead", labels: ["Median", "95th pct."], values: [6.97, 7.51] },
  ], { x: 0.5, y: 1.2, w: 5.6, h: 3.7, barDir: "bar", barGrouping: "stacked", chartColors: [HEX.accent3, HEX.accent1],
       showValue: true, dataLabelPosition: "ctr", dataLabelColor: "FFFFFF", dataLabelFontSize: 12, dataLabelFormatCode: "0.0",
       showLegend: true, legendPos: "b", legendFontSize: 12, legendFontFace: "+mn-lt",
       showTitle: true, title: "Kafka → Spark → Atlas latency on real data (s)", titleFontSize: 13, titleColor: HEX.dk2, titleFontFace: "+mn-lt",
       catAxisLabelColor: HEX.accent4, valAxisLabelColor: HEX.accent4, catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt",
       catAxisLabelFontSize: 12, valAxisLabelFontSize: 11, valGridLine: { color: HEX.lt2, size: 0.5 }, catGridLine: { style: "none" },
       objectName: "Latency split chart" });
  stat(s, "641 msg/s", "sustained at 300× real time, p95 latency 11.5 s (synthetic load)", 6.5, 1.3, 3.0, C.accent2, "Throughput stat");
  stat(s, "2,411", "windows into MongoDB Atlas from 14 h of Sept 2020 smoke at 120×", 6.5, 2.7, 3.0, C.accent1, "Windows stat");
  body(s, "Most latency is designed waiting, not the system.", { x: 6.5, y: 4.15, w: 3.0, h: 0.8, fontSize: 13, italic: true });

  // 11. Streaming algorithms
  s = content("Streaming algorithms on the live stream",
    "All four run over the replayed Kafka stream in arrival order, checked against exact answers. Bloom catches every duplicate delivery at 14 bits per key. " +
    "Memory stays fixed as the network grows.");
  [["filter", "Bloom filter", "Drops Kafka's duplicate deliveries", "2,843 of 2,843 caught · 7 false drops in 145k · 1.4 KB"],
   ["hash", "Flajolet–Martin", "Distinct sensors reporting per hour", "3.4% mean error · 256 bytes"],
   ["window", "DGIM", "Elevated readings among the last N", "≤ 50% bound held · 52 B vs 1,250 B at N = 10,000"],
   ["dice", "Reservoir sampling", "Unbiased sample of events to label", "each tenth of the stream kept 9.8–10.3% of the time"]].forEach(([ic, h, t, r], i) => {
    const x = 0.5 + (i % 2) * 4.6, y = 1.25 + Math.floor(i / 2) * 1.95;
    card(s, x, y, 4.4, 1.75, "Algorithm card " + (i + 1));
    circleIcon(s, ICON[ic], x + 0.25, y + 0.3, 0.75, C.accent2, "Algorithm " + (i + 1));
    s.addText(h, { isTextBox: true, x: x + 1.2, y: y + 0.18, w: 3.0, h: 0.4, fontSize: 17, bold: true, color: C.text1, margin: 0 });
    s.addText(t, { isTextBox: true, x: x + 1.2, y: y + 0.58, w: 3.0, h: 0.4, fontSize: 13, color: C.text2, margin: 0 });
    s.addText(r, { isTextBox: true, x: x + 1.2, y: y + 1.0, w: 3.0, h: 0.6, fontSize: 12, bold: true, color: C.accent1, margin: 0, valign: "top" });
  });

  // 12. LSH
  s = content("LSH: which sensors move together?",
    "SimHash on 144-slot daily curves finds 98% of correlated pairs while checking a quarter of all pairs. " +
    "Coherence is twice as high on smoke days as clean days, but July 4 2021 looks like smoke too: fireworks are everywhere at once.");
  s.addChart(pres.charts.BAR, [{ name: "Mean coherence", labels: ["Clean", "Moderate", "Smoke", "July 4, 2021", "July 4, 2024"],
    values: [0.133, 0.172, 0.282, 0.286, 0.099] }], {
    x: 0.5, y: 1.2, w: 5.6, h: 3.7, barDir: "col", chartColors: [HEX.accent4, HEX.accent4, HEX.accent1, HEX.accent3, HEX.accent3],
    invertedColors: [HEX.accent4], showValue: true, dataLabelPosition: "outEnd", dataLabelFormatCode: "0.00", dataLabelFontSize: 12,
    dataLabelColor: HEX.dk2, showLegend: false, showTitle: true, title: "Share of sensors moving together (correlation ≥ 0.8)",
    titleFontSize: 13, titleColor: HEX.dk2, titleFontFace: "+mn-lt", catAxisLabelColor: HEX.accent4, valAxisLabelColor: HEX.accent4,
    catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt", catAxisLabelFontSize: 11, valAxisLabelFontSize: 11,
    valAxisMaxVal: 0.35, valGridLine: { color: HEX.lt2, size: 0.5 }, catGridLine: { style: "none" }, objectName: "Coherence chart" });
  stat(s, "98%", "of correlated sensor-day pairs found (16 bands × 8 hyperplanes)", 6.5, 1.3, 3.0, C.accent2, "LSH recall stat");
  stat(s, "4× fewer", "comparisons than all pairs (1,025 sensor-days pooled)", 6.5, 2.7, 3.0, C.accent2, "LSH savings stat");
  body(s, "A useful extra signal for RQ3, not a classifier on its own.", { x: 6.5, y: 4.15, w: 3.0, h: 0.8, fontSize: 13, italic: true });

  // 13. Privacy
  s = content("Privacy: what safe publishing costs",
    "A hexagon with one sensor publishes one household's air. k-anonymity forces coarser cells; differential privacy adds noise. " +
    "Clipping each value at 100 µg/m³, all an AQI-100 alert needs, makes DP workable.");
  s.addImage({ path: path.join(FIG, "privacy.png"), x: 0.5, y: 1.15, w: 5.6, h: 3.62, objectName: "Privacy figure",
    altText: "Alerts kept against privacy budget for k = 1, 3, 5" });
  stat(s, "3.7%", "of hexagon-hours have 2+ sensors: sparse networks must generalize", 6.5, 1.3, 3.0, C.accent1, "Sparsity stat");
  stat(s, "87%", "of alerts kept with k = 3, ε = 2 per hour (74% precise)", 6.5, 2.7, 3.0, C.accent2, "DP stat");
  body(s, "Clip to the decision threshold: the cheapest privacy lever.", { x: 6.5, y: 4.15, w: 3.0, h: 0.8, fontSize: 13, italic: true });

  // 14. Poisoning
  s = content("Poisoning: can fake sensors raise alerts?",
    "Fakes report a plausible 120 µg/m³ with both channels agreeing, so only the neighbour check can catch them. " +
    "Lone fakes are caught where two or more honest sensors are nearby; colluding fakes vouch for each other (a Sybil attack).");
  [["Lone fakes (4)", "438 → 146", "false Unhealthy windows in 24 h, check off → on. 3 of 4 excluded within 20 min; the 4th had only 1 honest neighbour.", ICON.check, C.accent2],
   ["Colluding fakes (3)", "144 → 144", "false windows, check off → on. They are each other's neighbours, so the median is their own value.", ICON.warn, C.accent1]].forEach(([h, big, t, ic, col], i) => {
    const x = 0.5 + i * 4.6;
    card(s, x, 1.25, 4.4, 2.6, "Poison card " + (i + 1));
    s.addImage({ data: ic, x: x + 0.25, y: 1.45, w: 0.45, h: 0.45, objectName: "Poison icon " + (i + 1) });
    s.addText(h, { isTextBox: true, x: x + 0.85, y: 1.45, w: 3.4, h: 0.45, fontSize: 17, bold: true, color: C.text1, margin: 0 });
    s.addText(big, { isTextBox: true, x: x + 0.25, y: 2.05, w: 3.9, h: 0.7, fontSize: 34, bold: true, color: col, margin: 0, fontFace: THEME.headFontFace });
    s.addText(t, { isTextBox: true, x: x + 0.25, y: 2.8, w: 3.9, h: 0.95, fontSize: 13, color: C.text2, margin: 0, valign: "top" });
  });
  body(s, "Defenses: weigh sensor age and history, cap one owner's influence, cross-check satellite plumes and monitors.", { x: 0.5, y: 4.1, w: 9.0, h: 0.8, fontSize: 14 });

  // 15. Demo
  s = content("Live demo",
    "1. bash mongo/demo_live.sh: Kafka -> Spark -> Atlas replay of Sept 11, 2020, with near-SJSU queries while it streams. " +
    "2. stream_summaries.py --kafka on the same topic. 3. Airflow UI: DAG runs. 4. Snowflake warehouse queries.");
  [["Kafka → Spark → Atlas", "Replay Sept 10–11, 2020 smoke at 120×; \"AQI near SJSU\" $geoNear queries while it streams", "bash mongo/demo_live.sh"],
   ["Stream summaries", "Bloom, Flajolet–Martin and DGIM printing live from the same Kafka topic", "analysis/stream_summaries.py --kafka purpleair_demo"],
   ["Airflow", "Live FIRMS, HMS, wind and AirNow DAG runs; backfill", "airflow/run_airflow.sh"],
   ["Warehouse", "Star schema queries: smokiest days near SJSU (H3 + geospatial)", "warehouse/load_snowflake.py query daily_peak_near_sjsu"]].forEach(([h, t, cmd], i) => {
    const y = 1.2 + i * 0.98;
    circleIcon(s, ICON.play, 0.5, y + 0.08, 0.6, C.accent1, "Demo step " + (i + 1));
    s.addText(h, { isTextBox: true, x: 1.3, y, w: 2.6, h: 0.75, fontSize: 16, bold: true, color: C.text1, margin: 0, valign: "middle" });
    s.addText([{ text: t, options: { breakLine: true } }, { text: cmd, options: { fontFace: "Courier New", fontSize: 11, color: C.accent4 } }],
      { isTextBox: true, x: 4.0, y, w: 5.5, h: 0.8, fontSize: 13, color: C.text2, margin: 0, valign: "middle" });
  });

  // 16. Lessons
  s = content("Lessons learned",
    "Test the engine; control batch size; measure inputs; freshness beats waiting; self-checking loads; budget paid APIs.");
  [["l1", "Test the engine, not the docs", "An equivalence test exposed the previous-batch watermark rule"],
   ["l2", "Slice replays in simulated time", "Big micro-batches hide every lateness effect"],
   ["l3", "Measure what shapes the result", "Live delays; correction re-validated (the extended form inflates smoke ~50%)"],
   ["l4", "Freshness beats waiting", "The Airflow schedule mattered more than the watermark"],
   ["l5", "Loads that check themselves", "Idempotent slices; SQL must match the Python results"],
   ["l6", "Budget paid APIs like compute", "Estimate, cap, measure: 4 points per row + 2 per request"]].forEach(([ic, h, t], i) => {
    const x = 0.5 + (i % 2) * 4.6, y = 1.2 + Math.floor(i / 2) * 1.3;
    circleIcon(s, ICON[ic], x, y + 0.1, 0.65, i % 2 ? C.accent2 : C.accent1, "Lesson " + (i + 1));
    s.addText(h, { isTextBox: true, x: x + 0.85, y: y + 0.02, w: 3.5, h: 0.4, fontSize: 15, bold: true, color: C.text1, margin: 0 });
    s.addText(t, { isTextBox: true, x: x + 0.85, y: y + 0.45, w: 3.5, h: 0.7, fontSize: 13, color: C.text2, margin: 0, valign: "top" });
  });

  // 17. CRediT
  s = content("Contributions (CRediT)",
    "Contributor Roles Taxonomy (credit.niso.org). Shares to be filled in by the team.");
  const roles = ["Conceptualization", "Methodology", "Software", "Validation", "Formal analysis", "Investigation",
    "Data curation", "Writing: original draft", "Writing: review & editing", "Visualization", "Project administration"];
  const head = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: C.text1 } } });
  s.addTable([[head("CRediT role"), head("Vraj Patel"), head("Brandon Moriguchi")],
    ...roles.map((r) => [r, "[ %]", "[ %]"])], { x: 1.5, y: 1.1, w: 7.0, colW: [3.4, 1.8, 1.8], fontSize: 11, color: C.text2,
    border: { type: "solid", color: HEX.lt2, pt: 1 }, rowH: 0.33, valign: "middle", objectName: "CRediT table" });

  // 18. Conclusion
  s = content("Takeaways and next steps",
    "Real time is cheap where engineers worry (watermarks) and expensive where they look least (context freshness). Next: RQ3 labels, denser samples, Sybil-resistant QC, a workshop paper.");
  [["Late data is cheap", "≤ 0.61 µg/m³ from hindsight vs 17.4 sensor error", C.accent2],
   ["Stale context is expensive", "up to 45 points of label agreement lost", C.accent1],
   ["Alerts can be early", "88% precise, ~1.5 h before official values", C.accent3]].forEach(([h, t, col], i) => {
    const x = 0.5 + i * 3.07;
    card(s, x, 1.25, 2.86, 1.6, "Takeaway " + (i + 1));
    s.addText(h, { isTextBox: true, x: x + 0.2, y: 1.4, w: 2.5, h: 0.5, fontSize: 17, bold: true, color: col, margin: 0 });
    s.addText(t, { isTextBox: true, x: x + 0.2, y: 1.95, w: 2.5, h: 0.8, fontSize: 13, color: C.text2, margin: 0, valign: "top" });
  });
  s.addText([
    { text: "Score RQ3 with two blind labelers (Cohen's kappa)", options: { bullet: true, breakLine: true } },
    { text: "Denser sensor samples; delay-profile sensitivity", options: { bullet: true, breakLine: true } },
    { text: "Sybil-resistant quality check", options: { bullet: true, breakLine: true } },
    { text: "Target: workshop paper at ACM DEBS or ACM SIGSPATIAL", options: { bullet: true } },
  ], { isTextBox: true, x: 0.5, y: 3.1, w: 9.0, h: 1.8, fontSize: 15, color: C.text2, paraSpaceAfter: 6, valign: "top" });

  // 19. Thank you
  s = pres.addSlide({ masterName: "TITLE_DARK" });
  s.addText("Thank you. Questions?", { placeholder: "title" });
  s.addText([
    { text: "github.com/VRAJ8/smokestream", options: { breakLine: true, color: C.accent3 } },
    { text: "Kafka · Spark Structured Streaming · MongoDB Atlas · Snowflake · Airflow · H3", options: { fontSize: 13 } },
  ], { placeholder: "body" });
  s.addNotes("Q&A. Likely questions: why not Flink; why the simulator is trustworthy; why 10-minute data; how the labels are made blind; privacy composition over a day.");

  await pres.writeFile({ fileName: OUT });
  const { applyTheme } = require(process.env.PPTX_SKILL + "/scripts/apply_theme.js");
  await applyTheme(OUT, THEME);

  // Fade transition on every slide (creative technique; plain OOXML, opens in PowerPoint and Keynote)
  const zip = await JSZip.loadAsync(fs.readFileSync(OUT));
  for (const name of Object.keys(zip.files).filter((f) => /^ppt\/slides\/slide\d+\.xml$/.test(f))) {
    let xml = await zip.file(name).async("string");
    if (!xml.includes("<p:transition")) {
      xml = xml.replace("</p:clrMapOvr>", '</p:clrMapOvr><p:transition spd="med"><p:fade/></p:transition>');
      zip.file(name, xml);
    }
  }
  const clean = new JSZip();  // files only: folder entries trip up cloud co-editing
  for (const [name, f] of Object.entries(zip.files)) if (!f.dir) clean.file(name, await f.async("nodebuffer"), { createFolders: false });
  fs.writeFileSync(OUT, await clean.generateAsync({ type: "nodebuffer", compression: "DEFLATE" }));
  console.log("Wrote " + OUT);
}

main().catch((e) => { console.error(e); process.exit(1); });
