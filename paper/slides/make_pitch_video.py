#!/usr/bin/env python3
"""Narrated pitch video from the pitch deck, on macOS, with no uploads anywhere.

Each slide is rendered with Quick Look, given a slow zoom, and crossfaded into the
next; its speaker notes are read by a built-in macOS voice and placed on cue.
Quick Look does not draw native charts, so the chart slide gets the matching figure.

Usage: python make_pitch_video.py <pitch.pptx> <out.mp4> [--voice Daniel] [--rate 165]
Needs: imageio-ffmpeg and Pillow (pip), macOS `say`, `afinfo` and `qlmanage`.
"""

import argparse
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path

import imageio_ffmpeg
from PIL import Image

FIG = Path(__file__).resolve().parent.parent / "figures"
W, H, FPS = 1920, 1080, 30
LEAD, TAIL, XFADE = 0.6, 0.9, 0.7   # seconds of silence before/after each narration, crossfade length
SAY_AS = {"DGIM": "D G I M", "SmokeStream": "Smoke Stream", "H3": "H 3", "µg/m³": "micrograms per cubic meter"}


def slide_notes(pptx):
    z = zipfile.ZipFile(pptx)
    pres = z.read("ppt/presentation.xml").decode()
    rels = z.read("ppt/_rels/presentation.xml.rels").decode()
    targets = {}
    for rel in re.findall(r"<Relationship [^>]*/>", rels):
        targets[re.search(r'Id="([^"]+)"', rel).group(1)] = re.search(r'Target="([^"]+)"', rel).group(1)
    order = ["ppt/" + targets[r].lstrip("/").replace("ppt/", "") for r in re.findall(r'<p:sldId [^>]*r:id="(rId\d+)"', pres)]
    notes = []
    for slide in order:
        srels = z.read(slide.replace("slides/", "slides/_rels/") + ".rels").decode()
        m = re.search(r'Target="\.\./notesSlides/(notesSlide\d+\.xml)"', srels)
        text = ""
        if m:
            xml = z.read("ppt/notesSlides/" + m.group(1)).decode()
            text = " ".join(re.findall(r"<a:t>([^<]*)</a:t>", xml)).strip()
        notes.append(text)
    return order, notes


def render_slides(pptx, order, work):
    """One PNG per slide: split the deck into single-slide files and let Quick Look draw each."""
    z = zipfile.ZipFile(pptx)
    pres = z.read("ppt/presentation.xml").decode()
    ids = re.findall(r"<p:sldId [^>]*/>", pres)
    images = []
    for i, sid in enumerate(ids):
        single = work / f"slide_{i + 1:02d}.pptx"
        new = re.sub(r"<p:sldIdLst>.*?</p:sldIdLst>", f"<p:sldIdLst>{sid}</p:sldIdLst>", pres, flags=re.S)
        with zipfile.ZipFile(single, "w", zipfile.ZIP_DEFLATED) as out:
            for item in z.infolist():
                out.writestr(item.filename, new.encode() if item.filename == "ppt/presentation.xml" else z.read(item.filename))
        subprocess.run(["qlmanage", "-t", "-s", str(W), "-o", str(work), str(single)], check=True, capture_output=True)
        img = Image.open(f"{single}.png").convert("RGB").resize((W, H), Image.LANCZOS)
        if b"<c:chart " in z.read(order[i]):
            img = add_chart_figure(img)
        path = work / f"frame_{i + 1:02d}.png"
        img.save(path)
        images.append(path)
    return images


def add_chart_figure(img):
    """Quick Look leaves native charts blank; paste the matching dark figure into the chart's box."""
    fig = Image.open(FIG / "latency_split_dark.png").convert("RGBA")
    px = W / 10.0                                   # slide is 10 inches wide
    box_x, box_y, box_w, box_h = 0.5 * px, 1.2 * px, 5.6 * px, 3.7 * px
    scale = min(box_w / fig.width, box_h / fig.height)
    fig = fig.resize((int(fig.width * scale), int(fig.height * scale)), Image.LANCZOS)
    img.paste(fig, (int(box_x + (box_w - fig.width) / 2), int(box_y + (box_h - fig.height) / 2)), fig)
    return img


def narrate(text, voice, rate, path):
    for word, spoken in SAY_AS.items():
        text = text.replace(word, spoken)
    subprocess.run(["say", "-v", voice, "-r", str(rate), "-o", str(path), text], check=True)
    info = subprocess.run(["afinfo", str(path)], check=True, capture_output=True, text=True).stdout
    return float(re.search(r"estimated duration: ([\d.]+)", info).group(1))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pptx")
    parser.add_argument("out")
    parser.add_argument("--voice", default="Daniel")
    parser.add_argument("--rate", type=int, default=165, help="words per minute")
    args = parser.parse_args()
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()

    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        order, notes = slide_notes(args.pptx)
        images = render_slides(args.pptx, order, work)
        clips, audio, starts, t = [], [], [], 0.0
        for i, (img, text) in enumerate(zip(images, notes)):
            wav = work / f"voice_{i + 1:02d}.aiff"
            dur = LEAD + narrate(text, args.voice, args.rate, wav) + TAIL
            frames = int(dur * FPS)
            clip = work / f"clip_{i + 1:02d}.mp4"
            zoom = f"zoompan=z='1+0.05*on/{frames}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={frames}:s={W}x{H}:fps={FPS}"
            subprocess.run([ffmpeg, "-y", "-loglevel", "error", "-loop", "1", "-i", str(img), "-vf",
                            f"scale={2 * W}:{2 * H},{zoom},format=yuv420p", "-frames:v", str(frames),
                            "-c:v", "libx264", "-preset", "medium", "-crf", "18", str(clip)], check=True)
            clips.append((clip, dur))
            audio.append(wav)
            starts.append(t + LEAD)
            t += dur - XFADE

        # Crossfade the clips into one video, fading in from and out to black.
        inputs, chain, prev, offset = [], [], "0:v", 0.0
        for clip, _ in clips:
            inputs += ["-i", str(clip)]
        for i in range(1, len(clips)):
            offset += clips[i - 1][1] - XFADE
            chain.append(f"[{prev}][{i}:v]xfade=transition=fade:duration={XFADE}:offset={offset:.3f}[v{i}]")
            prev = f"v{i}"
        total = offset + clips[-1][1]
        chain.append(f"[{prev}]fade=t=in:st=0:d=0.8,fade=t=out:st={total - 1.0:.3f}:d=1.0[vout]")
        # Each narration starts on cue; mix them into one track.
        n = len(clips)
        for k, (wav, start) in enumerate(zip(audio, starts)):
            inputs += ["-i", str(wav)]
            ms = int(start * 1000)
            chain.append(f"[{n + k}:a]aresample=48000,adelay={ms}|{ms}[a{k}]")
        chain.append("".join(f"[a{k}]" for k in range(n)) + f"amix=inputs={n}:normalize=0,apad[aout]")
        subprocess.run([ffmpeg, "-y", "-loglevel", "error", *inputs, "-filter_complex", ";".join(chain),
                        "-map", "[vout]", "-map", "[aout]", "-t", f"{total:.3f}", "-c:v", "libx264", "-preset", "medium",
                        "-crf", "20", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart",
                        args.out], check=True)
    print(f"Wrote {args.out}: {len(clips)} slides, {total:.0f} s, voice {args.voice}")


if __name__ == "__main__":
    main()
