#!/usr/bin/env python3
"""Slide backgrounds: drifting smoke over near-black with an ember glow, generated
from layered blurred noise (no stock images). Writes paper/slides/assets/bg_*.jpg."""

from pathlib import Path

import numpy as np
from PIL import Image, ImageFilter

W, H = 1600, 900
OUT = Path(__file__).resolve().parent / "assets"


def smoke(seed, scales=(24, 50, 100, 200), weights=(0.12, 0.23, 0.32, 0.33)):
    """Fractal smoke in [0, 1]: noise at several scales, stretched sideways so it drifts,
    each upsampled and blurred; the large scales dominate, so it billows instead of grains."""
    rng = np.random.default_rng(seed)
    total = np.zeros((H, W))
    for s, w in zip(scales, weights):
        small = Image.fromarray((rng.random((H // s + 2, W // (2 * s) + 2)) * 255).astype(np.uint8))
        big = small.resize((W + 2 * s, H + 2 * s), Image.BICUBIC).filter(ImageFilter.GaussianBlur(s * 0.6))
        total += w * np.asarray(big, dtype=float)[s:s + H, s:s + W] / 255
    total = (total - total.min()) / (total.max() - total.min())
    return total


def glow(cx, cy, radius):
    y, x = np.mgrid[0:H, 0:W]
    d = np.hypot((x - cx * W) / W, (y - cy * H) / H)
    return np.clip(1 - d / radius, 0, 1) ** 2.2


def render(name, seed, smoke_amount, glow_at, glow_radius, glow_amount, vignette=0.55):
    s = smoke(seed) ** 1.6
    g = glow(*glow_at, glow_radius)
    base = np.array([11, 13, 18], dtype=float)          # near-black, slightly blue
    ash = np.array([92, 98, 110], dtype=float)           # smoke
    ember = np.array([228, 87, 46], dtype=float)         # the deck's accent
    img = base + smoke_amount * s[..., None] * (ash - base)
    img += glow_amount * g[..., None] * (0.6 + 0.8 * s[..., None]) * ember
    y, x = np.mgrid[0:H, 0:W]
    v = 1 - vignette * np.clip(np.hypot((x - W / 2) / (W / 2), (y - H / 2) / (H / 2)) - 0.35, 0, 1)
    img *= v[..., None]
    OUT.mkdir(exist_ok=True)
    Image.fromarray(np.clip(img, 0, 255).astype(np.uint8)).save(OUT / f"bg_{name}.jpg", quality=82, optimize=True)


def ember_glow(size=800):
    """A soft ember light with a transparent edge; Morph slides it across the deck."""
    y, x = np.mgrid[0:size, 0:size]
    d = np.hypot(x - size / 2, y - size / 2) / (size / 2)
    alpha = np.clip(1 - d, 0, 1) ** 2.4 * 150
    rgba = np.zeros((size, size, 4), dtype=np.uint8)
    rgba[..., :3] = (240, 106, 59)
    rgba[..., 3] = alpha.astype(np.uint8)
    Image.fromarray(rgba, "RGBA").save(OUT / "glow.png", optimize=True)


if __name__ == "__main__":
    ember_glow()
    render("title", seed=7, smoke_amount=0.55, glow_at=(0.85, 0.95), glow_radius=0.75, glow_amount=0.55)
    render("divider", seed=11, smoke_amount=0.45, glow_at=(0.1, 0.9), glow_radius=0.7, glow_amount=0.45)
    render("content", seed=3, smoke_amount=0.16, glow_at=(1.0, 1.0), glow_radius=0.6, glow_amount=0.12, vignette=0.35)
    print(f"Backgrounds in {OUT}")
