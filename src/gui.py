from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from .config import load_env, set_env_var, CONFIG_DIR
from .gui_modes import ManualModeView, AutoModeView
from .core import send_media_job
from .discord_client import DiscordClient


@dataclass
class JobRowState:
    input_var: tk.StringVar
    url_var: tk.StringVar
    input_entry: tk.Entry
    url_entry: tk.Entry
    input_label: ttk.Label
    browse_button: ttk.Button
    url_label: ttk.Label
    remove_button: ttk.Button


class DynamicJobsList(ttk.Frame):
    def __init__(self, master: tk.Misc, title: str):
        super().__init__(master)
        self.columnconfigure(1, weight=1)
        lbl = ttk.Label(self, text=title, font=(None, 10, "bold"))
        lbl.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 6))
        self.rows: List[JobRowState] = []
        self._rows_container = ttk.Frame(self)
        self._rows_container.grid(row=1, column=0, columnspan=3, sticky="nsew")
        self._add_row_button = ttk.Button(self, text="+ Add row", command=self.add_row)
        self._add_row_button.grid(row=2, column=0, sticky="w", pady=(8, 0))
        self._on_change: Optional[callable] = None
        self.add_row()

    def _make_row(self, row_index: int) -> JobRowState:
        input_var = tk.StringVar()
        url_var = tk.StringVar()
        def _notify(*_args):
            try:
                if self._on_change is not None:
                    self._on_change()
            except Exception:
                pass
        input_var.trace_add("write", _notify)
        url_var.trace_add("write", _notify)
        r = JobRowState(
            input_var=input_var,
            url_var=url_var,
            input_entry=ttk.Entry(self._rows_container, textvariable=input_var, width=36),
            url_entry=ttk.Entry(self._rows_container, textvariable=url_var, width=40),
            input_label=ttk.Label(self._rows_container, text="Input dir:"),
            browse_button=ttk.Button(self._rows_container, text="Browse", command=lambda v=input_var: self._browse_dir(v)),
            url_label=ttk.Label(self._rows_container, text="Discord URL:"),
            remove_button=ttk.Button(self._rows_container, text="Remove", command=lambda idx=row_index: self.remove_row(idx)),
        )
        r.input_label.grid(row=row_index, column=0, sticky="w", padx=(0, 6), pady=2)
        r.input_entry.grid(row=row_index, column=1, sticky="we", pady=2)
        r.browse_button.grid(row=row_index, column=2, padx=(6, 0), pady=2)

        r.url_label.grid(row=row_index, column=3, sticky="w", padx=(16, 6), pady=2)
        r.url_entry.grid(row=row_index, column=4, sticky="we", pady=2)
        r.remove_button.grid(row=row_index, column=5, sticky="w", padx=(6, 0), pady=2)
        self._rows_container.columnconfigure(1, weight=1)
        self._rows_container.columnconfigure(4, weight=1)
        try:
            self.after(0, _notify)
        except Exception:
            pass
        return r

    def add_row(self) -> None:
        r = self._make_row(len(self.rows))
        self.rows.append(r)
        if self._on_change is not None:
            try:
                self._on_change()
            except Exception:
                pass

    def remove_row(self, index: int) -> None:
        if len(self.rows) <= 1:
            # Keep at least one row
            return
        # Destroy widgets for the row to be removed
        row = self.rows.pop(index)
        for w in [
            row.input_label,
            row.input_entry,
            row.browse_button,
            row.url_label,
            row.url_entry,
            row.remove_button,
        ]:
            try:
                w.destroy()
            except Exception:
                pass
        # Rebuild remaining rows to have contiguous indices and working commands
        existing = [(r.input_var.get(), r.url_var.get()) for r in self.rows]
        self.set_jobs(existing)
        if self._on_change is not None:
            try:
                self._on_change()
            except Exception:
                pass

    def get_jobs(self) -> List[Tuple[Path, str]]:
        jobs: List[Tuple[Path, str]] = []
        for r in self.rows:
            input_val = r.input_var.get().strip()
            url_val = r.url_var.get().strip()
            if not input_val and not url_val:
                continue
            if input_val and url_val:
                p = Path(input_val)
                jobs.append((p, url_val))
        return jobs

    def set_jobs(self, jobs: List[Tuple[str, str]]) -> None:
        # Clear current rows UI
        for child in list(self._rows_container.winfo_children()):
            child.destroy()
        self.rows.clear()
        if not jobs:
            self.add_row()
            return
        for idx, (inp, url) in enumerate(jobs):
            r = self._make_row(idx)
            r.input_var.set(inp)
            r.url_var.set(url)
            self.rows.append(r)

    def _browse_dir(self, var: tk.StringVar) -> None:
        d = filedialog.askdirectory(title="Select input directory")
        if d:
            var.set(d)

    def set_on_change(self, callback: callable) -> None:
        self._on_change = callback


class AdvancedOptions(ttk.LabelFrame):
    def __init__(self, master: tk.Misc):
        super().__init__(master, text="Advanced options")
        self.columnconfigure(1, weight=1)

        # Token and token type
        ttk.Label(self, text="Token:").grid(row=0, column=0, sticky="w")
        self.token_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.token_var, show="*", width=48).grid(row=0, column=1, sticky="we", pady=2)
        self.save_token_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(self, text="Save token to .env", variable=self.save_token_var).grid(row=0, column=2, padx=(8, 0))

        ttk.Label(self, text="Token type:").grid(row=1, column=0, sticky="w")
        self.token_type_var = tk.StringVar(value="auto")
        ttk.Combobox(self, textvariable=self.token_type_var, values=["auto", "bot", "user"], width=8, state="readonly").grid(row=1, column=1, sticky="w")

        # Booleans
        self.ignore_dedupe_var = tk.BooleanVar(value=False)
        self.dry_run_var = tk.BooleanVar(value=False)
        self.skip_oversize_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(self, text="Ignore dedupe", variable=self.ignore_dedupe_var).grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Checkbutton(self, text="Dry run", variable=self.dry_run_var).grid(row=2, column=1, sticky="w", pady=(6, 0))
        ttk.Checkbutton(self, text="Skip oversize", variable=self.skip_oversize_var).grid(row=2, column=2, sticky="w", pady=(6, 0))
        # Ignore segmentation
        self.ignore_segmentation_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(self, text="Ignore segmentation", variable=self.ignore_segmentation_var).grid(row=2, column=3, sticky="w", pady=(6, 0))

        # Numeric/text options
        def add_num(label: str, row: int, var: tk.StringVar, default: str, width: int = 8):
            ttk.Label(self, text=label).grid(row=row, column=0, sticky="w")
            var.set(default)
            ttk.Entry(self, textvariable=var, width=width).grid(row=row, column=1, sticky="w")

        self.history_limit_var = tk.StringVar()
        self.request_timeout_var = tk.StringVar()
        self.upload_timeout_var = tk.StringVar()
        self.delay_seconds_var = tk.StringVar()
        self.max_file_mb_var = tk.StringVar()
        add_num("History limit:", 3, self.history_limit_var, "1000")
        add_num("Request timeout (s):", 4, self.request_timeout_var, "30.0")
        add_num("Upload timeout (s):", 5, self.upload_timeout_var, "120.0")
        add_num("Delay (s):", 6, self.delay_seconds_var, "1.0")
        add_num("Max file MB:", 7, self.max_file_mb_var, "10.0")
        # Concurrency (messages in-flight per job)
        self.concurrency_var = tk.StringVar()
        add_num("Concurrency:", 12, self.concurrency_var, "1")

        # Forum/media options
        ttk.Label(self, text="Post title:").grid(row=8, column=0, sticky="w")
        self.post_title_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.post_title_var, width=32).grid(row=8, column=1, sticky="we")
        ttk.Label(self, text="Post tag:").grid(row=9, column=0, sticky="w")
        self.post_tag_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.post_tag_var, width=24).grid(row=9, column=1, sticky="we")

        # Relay options
        ttk.Label(self, text="Relay from URL:").grid(row=10, column=0, sticky="w")
        self.relay_from_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.relay_from_var, width=40).grid(row=10, column=1, sticky="we")
        ttk.Label(self, text="Relay download dir:").grid(row=11, column=0, sticky="w")
        self.relay_dir_var = tk.StringVar(value=".adms_cache")
        relay_entry = ttk.Entry(self, textvariable=self.relay_dir_var, width=36)
        relay_entry.grid(row=11, column=1, sticky="we")
        ttk.Button(self, text="Browse", command=self._browse_relay_dir).grid(row=11, column=2, sticky="w")

        # Prepend option for auto mode thread names
        self.prepend_enabled_var = tk.BooleanVar(value=False)
        self.prepend_text_var = tk.StringVar(value="")
        ttk.Label(self, text="Auto mode prepend:").grid(row=12, column=0, sticky="w")
        prepend_frame = ttk.Frame(self)
        prepend_frame.grid(row=12, column=1, columnspan=2, sticky="we", pady=2)
        prepend_frame.columnconfigure(1, weight=1)
        ttk.Checkbutton(prepend_frame, text="Enable", variable=self.prepend_enabled_var).grid(row=0, column=0, sticky="w")
        ttk.Entry(prepend_frame, textvariable=self.prepend_text_var, width=24).grid(row=0, column=1, sticky="we", padx=(8, 0))

        # Media type selection
        ttk.Label(self, text="Media types:").grid(row=13, column=0, sticky="w", pady=(6, 0))
        self.media_all_var = tk.BooleanVar(value=True)
        self.media_videos_var = tk.BooleanVar(value=False)
        self.media_gifs_var = tk.BooleanVar(value=False)
        self.media_images_var = tk.BooleanVar(value=False)

        def _on_all_changed():
            if self.media_all_var.get():
                self.media_videos_var.set(False)
                self.media_gifs_var.set(False)
                self.media_images_var.set(False)

        def _on_single_changed():
            # If any non-All is selected, All should be off
            if self.media_videos_var.get() or self.media_gifs_var.get() or self.media_images_var.get():
                self.media_all_var.set(False)
            # If none are selected, default back to All
            if not (self.media_videos_var.get() or self.media_gifs_var.get() or self.media_images_var.get()):
                self.media_all_var.set(True)

        media_frame = ttk.Frame(self)
        media_frame.grid(row=13, column=1, columnspan=2, sticky="w", pady=(6, 0))
        ttk.Checkbutton(media_frame, text="All", variable=self.media_all_var, command=_on_all_changed).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(media_frame, text="Videos", variable=self.media_videos_var, command=_on_single_changed).grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Checkbutton(media_frame, text="Gifs", variable=self.media_gifs_var, command=_on_single_changed).grid(row=0, column=2, sticky="w", padx=(8, 0))
        ttk.Checkbutton(media_frame, text="Images", variable=self.media_images_var, command=_on_single_changed).grid(row=0, column=3, sticky="w", padx=(8, 0))

        # Per-job post overrides section (hidden until needed)
        self._per_job_frame = ttk.LabelFrame(self, text="Per-job post fields")
        self._per_job_frame.grid(row=14, column=0, columnspan=3, sticky="we", pady=(8, 0))
        ttk.Label(self._per_job_frame, text="#").grid(row=0, column=0, sticky="w", padx=(4, 8))
        ttk.Label(self._per_job_frame, text="Title").grid(row=0, column=1, sticky="w")
        ttk.Label(self._per_job_frame, text="Tag").grid(row=0, column=2, sticky="w", padx=(8, 0))
        self._per_job_rows: list[tuple[ttk.Label, tk.StringVar, ttk.Entry, tk.StringVar, ttk.Entry]] = []
        self._per_job_frame.grid_remove()

    def _browse_relay_dir(self) -> None:
        d = filedialog.askdirectory(title="Select relay download directory")
        if d:
            self.relay_dir_var.set(d)

    def set_per_job_indices(self, indices: List[int]) -> None:
        # Show only if 2+ jobs require new posts
        if len(indices) >= 2:
            self._per_job_frame.grid()
        else:
            self._per_job_frame.grid_remove()
            self._clear_per_job_rows()
            return
        current = len(self._per_job_rows)
        needed = len(indices)
        while current < needed:
            row_idx = current + 1
            ln_lbl = ttk.Label(self._per_job_frame, text=str(indices[current]))
            title_var = tk.StringVar()
            title_entry = ttk.Entry(self._per_job_frame, textvariable=title_var, width=32)
            tag_var = tk.StringVar()
            tag_entry = ttk.Entry(self._per_job_frame, textvariable=tag_var, width=16)
            ln_lbl.grid(row=row_idx, column=0, sticky="w", padx=(4, 8), pady=2)
            title_entry.grid(row=row_idx, column=1, sticky="we", pady=2)
            tag_entry.grid(row=row_idx, column=2, sticky="we", pady=2, padx=(8, 0))
            self._per_job_rows.append((ln_lbl, title_var, title_entry, tag_var, tag_entry))
            current += 1
        while current > needed:
            ln_lbl, _tvar, t_entry, _gvar, g_entry = self._per_job_rows.pop()
            try:
                ln_lbl.destroy(); t_entry.destroy(); g_entry.destroy()
            except Exception:
                pass
            current -= 1
        for idx, (ln_lbl, _tvar, _te, _gvar, _ge) in enumerate(self._per_job_rows):
            ln_lbl.configure(text=str(indices[idx]))

    def get_per_job_overrides(self) -> List[Tuple[str, str]]:
        vals: List[Tuple[str, str]] = []
        for _ln, tvar, _te, gvar, _ge in self._per_job_rows:
            vals.append((tvar.get().strip(), gvar.get().strip()))
        return vals

    def _clear_per_job_rows(self) -> None:
        while self._per_job_rows:
            ln_lbl, _tvar, t_entry, _gvar, g_entry = self._per_job_rows.pop()
            try:
                ln_lbl.destroy(); t_entry.destroy(); g_entry.destroy()
            except Exception:
                pass


class RunPane(ttk.Frame):
    def __init__(self, master: tk.Misc):
        super().__init__(master)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self._main_thread = threading.main_thread()

        # Scrollable container of job logs
        self._canvas = tk.Canvas(self, borderwidth=0, highlightthickness=0)
        self._scrollbar = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._container = ttk.Frame(self._canvas)
        self._container.bind(
            "<Configure>", lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all"))
        )
        self._container_window = self._canvas.create_window((0, 0), window=self._container, anchor="nw")
        self._canvas.configure(yscrollcommand=self._scrollbar.set)
        self._canvas.grid(row=0, column=0, sticky="nsew")
        self._scrollbar.grid(row=0, column=1, sticky="ns")

        self._job_items: list[dict] = []
        self._text_bg = None
        self._text_fg = None
        self._base_bg = None
        self._num_cols = 1
        self._min_panel_width = 420  # px threshold for adding another column
        self._global_row = 1000  # rolling row index for global messages

        # Recalculate layout when the canvas width changes
        self._canvas.bind("<Configure>", self._on_canvas_resize)
        # Keep container width in sync with canvas so columns compute correctly
        self._canvas.bind(
            "<Configure>",
            lambda e: self._canvas.itemconfigure(self._container_window, width=e.width)
        )

    def add_job_panel(self, title: str, on_stop: Optional[callable] = None) -> dict:
        # Determine grid placement based on current number of columns
        idx = len(self._job_items)
        row = idx // self._num_cols
        col = idx % self._num_cols
        frame = ttk.LabelFrame(self._container, text=title)
        pad_left = 0 if col == 0 else 8
        frame.grid(row=row, column=col, sticky="nsew", padx=(pad_left, 0), pady=(0, 8))
        text = tk.Text(frame, height=10, wrap="word", borderwidth=0, highlightthickness=0)
        sb = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=sb.set)
        text.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")
        stop_btn = ttk.Button(frame, text="Stop", command=on_stop) if on_stop else ttk.Button(frame, text="Stop")
        stop_btn.grid(row=1, column=0, sticky="e", pady=(6, 0))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        item = {"frame": frame, "text": text, "title": title, "stop": stop_btn}
        self._job_items.append(item)
        # Apply current theme to new text
        if self._text_bg is not None and self._text_fg is not None:
            text.configure(bg=self._text_bg, fg=self._text_fg, insertbackground=self._text_fg)
        # Ensure container columns have weight
        self._apply_column_weights()
        return item

    def log_global(self, line: str) -> None:
        # Append a global message at the end in a lightweight label
        if threading.current_thread() is self._main_thread:
            self._append_global(line)
        else:
            self.after(0, self._append_global, line)

    # Back-compat convenience wrapper
    def log(self, line: str) -> None:
        self.log_global(line)

    def _append_global(self, line: str) -> None:
        lbl = ttk.Label(self._container, text=line)
        # Place global messages at bottom spanning all columns
        lbl.grid(row=self._global_row, column=0, columnspan=self._num_cols, sticky="w")
        self._global_row += 1

    def log_to(self, item: dict, line: str) -> None:
        if threading.current_thread() is self._main_thread:
            self._append_log(item, line)
        else:
            self.after(0, self._append_log, item, line)

    def _append_log(self, item: dict, line: str) -> None:
        txt: tk.Text = item["text"]
        txt.insert("end", line + "\n")
        txt.see("end")

    def set_text_colors(self, bg: str, fg: str) -> None:
        self._text_bg = bg
        self._text_fg = fg
        for item in self._job_items:
            txt: tk.Text = item["text"]
            txt.configure(bg=bg, fg=fg, insertbackground=fg)

    def set_base_colors(self, bg: str) -> None:
        # Canvas is a Tk widget and does not pick up ttk styles automatically
        self._base_bg = bg
        self._canvas.configure(bg=bg)

    def clear(self) -> None:
        # Remove all job frames and reset state
        for item in self._job_items:
            try:
                item["frame"].destroy()
            except Exception:
                pass
        self._job_items.clear()
        # Also remove any trailing global labels
        for child in list(self._container.winfo_children()):
            if isinstance(child, ttk.Label) and child not in [i.get("frame") for i in self._job_items]:
                try:
                    child.destroy()
                except Exception:
                    pass
        # Reset global row counter
        self._global_row = 1000

    def _apply_column_weights(self) -> None:
        # Give weight to active columns so frames expand evenly
        for c in range(self._num_cols):
            self._container.columnconfigure(c, weight=1)
        # Reset a couple of extra columns to zero weight
        for c in range(self._num_cols, self._num_cols + 3):
            self._container.columnconfigure(c, weight=0)

    def _regrid_items(self) -> None:
        # Reposition frames according to the current number of columns
        for idx, item in enumerate(self._job_items):
            row = idx // self._num_cols
            col = idx % self._num_cols
            pad_left = 0 if col == 0 else 8
            item["frame"].grid_configure(row=row, column=col, padx=(pad_left, 0))
            item["frame"].grid(sticky="nsew")
        self._apply_column_weights()

    def _on_canvas_resize(self, event) -> None:
        try:
            width = max(1, int(event.width))
        except Exception:
            return
        # Account for scrollbar width (~16px) and padding when computing columns
        effective_width = max(1, width - 20)
        desired_cols = max(1, min(3, effective_width // self._min_panel_width))
        if desired_cols != self._num_cols:
            self._num_cols = desired_cols
            self._regrid_items()


def _load_token_from_env() -> Optional[str]:
    try:
        return os.environ.get("DISCORD_TOKEN")
    except Exception:
        return None


def _to_float(s: str, default: float) -> float:
    try:
        return float(s)
    except Exception:
        return default


def _to_int(s: str, default: int) -> int:
    try:
        return int(float(s))
    except Exception:
        return default


CONFIG_PATH = CONFIG_DIR / "adms_gui.json"


def _load_config() -> dict:
    try:
        if CONFIG_PATH.exists():
            with CONFIG_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_config(data: dict) -> None:
    try:
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def _apply_theme(root: tk.Tk, run_pane: RunPane, mode: str) -> None:
    style = ttk.Style(root)
    bg_dark = "#1e1e1e"
    fg_dark = "#f0f0f0"
    entry_dark = "#2b2b2b"
    select_dark = "#3a3a3a"
    bg_light = "#ffffff"
    fg_light = "#000000"

    if mode.lower() == "dark":
        # Use clam for better styling control
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("TFrame", background=bg_dark)
        style.configure("TLabelframe", background=bg_dark)
        style.configure("TLabelframe.Label", background=bg_dark, foreground=fg_dark)
        style.configure("TLabel", background=bg_dark, foreground=fg_dark)
        style.configure("TButton", background=bg_dark, foreground=fg_dark)
        style.configure("TCheckbutton", background=bg_dark, foreground=fg_dark)
        style.configure("TEntry", fieldbackground=entry_dark, foreground=fg_dark)
        style.configure("TCombobox", fieldbackground=entry_dark, foreground=fg_dark, background=entry_dark)
        # Ensure hover/active visuals keep dark backgrounds
        try:
            style.map("TCheckbutton", background=[("active", bg_dark)])
            style.map("TButton", background=[("active", entry_dark)])
            style.map("TCombobox", fieldbackground=[("readonly", entry_dark), ("!disabled", entry_dark)])
            style.map("TCombobox", background=[("active", entry_dark), ("readonly", entry_dark)])
            style.map("TCombobox", foreground=[("readonly", fg_dark)])
        except Exception:
            pass
        # Style the Combobox dropdown list (not covered by ttk styles)
        try:
            root.option_add('*TCombobox*Listbox*Background', entry_dark)
            root.option_add('*TCombobox*Listbox*Foreground', fg_dark)
            root.option_add('*TCombobox*Listbox*selectBackground', select_dark)
            root.option_add('*TCombobox*Listbox*selectForeground', fg_dark)
        except Exception:
            pass
        root.configure(bg=bg_dark)
        try:
            run_pane._canvas.configure(bg=bg_dark)
        except Exception:
            pass
        run_pane.set_text_colors(entry_dark, fg_dark)
    else:
        try:
            style.theme_use("vista")
        except Exception:
            try:
                style.theme_use("default")
            except Exception:
                pass
        style.configure("TFrame", background=bg_light)
        style.configure("TLabelframe", background=bg_light)
        style.configure("TLabelframe.Label", background=bg_light, foreground=fg_light)
        style.configure("TLabel", background=bg_light, foreground=fg_light)
        style.configure("TButton", background=bg_light, foreground=fg_light)
        style.configure("TCheckbutton", background=bg_light, foreground=fg_light)
        style.configure("TEntry", fieldbackground=bg_light, foreground=fg_light)
        style.configure("TCombobox", fieldbackground=bg_light, foreground=fg_light, background=bg_light)
        try:
            style.map("TCheckbutton", background=[("active", bg_light)])
            style.map("TButton", background=[("active", "#e6e6e6")])
            style.map("TCombobox", fieldbackground=[("readonly", bg_light), ("!disabled", bg_light)])
            style.map("TCombobox", background=[("active", bg_light), ("readonly", bg_light)])
            style.map("TCombobox", foreground=[("readonly", fg_light)])
        except Exception:
            pass
        # Dropdown list colors for light mode
        try:
            root.option_add('*TCombobox*Listbox*Background', bg_light)
            root.option_add('*TCombobox*Listbox*Foreground', fg_light)
            root.option_add('*TCombobox*Listbox*selectBackground', "#cce8ff")
            root.option_add('*TCombobox*Listbox*selectForeground', fg_light)
        except Exception:
            pass
        root.configure(bg=bg_light)
        try:
            run_pane._canvas.configure(bg=bg_light)
        except Exception:
            pass
        run_pane.set_text_colors(bg_light, fg_light)


def launch_gui() -> None:
    load_env()
    # Setup logging to ./logs like the CLI does
    try:
        import logging as _logging
        from datetime import datetime as _dt
        logs_dir = Path("logs")
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        log_path = logs_dir / f"run_{_dt.now().strftime('%Y%m%d_%H%M%S')}.log"
        root = _logging.getLogger()
        root.setLevel(_logging.INFO)
        for h in list(root.handlers):
            root.removeHandler(h)
        fh = _logging.FileHandler(str(log_path), encoding="utf-8")
        fmt = _logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        fh.setFormatter(fmt)
        root.addHandler(fh)
        # Keep last 5 logs
        try:
            log_files = list(logs_dir.glob("run_*.log"))
            log_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for old in log_files[5:]:
                try:
                    old.unlink()
                except Exception:
                    pass
        except Exception:
            pass
    except Exception:
        pass
    root = tk.Tk()
    root.title("AutoDisMediaSend")
    root.minsize(900, 600)

    # Top bar with theme
    top_bar = ttk.Frame(root)
    top_bar.grid(row=0, column=0, sticky="we", padx=12, pady=(10, 0))
    ttk.Label(top_bar, text="Theme:").grid(row=0, column=0, sticky="e")
    theme_var = tk.StringVar(value="Dark")
    def _toggle_theme():
        theme_var.set("Light" if theme_var.get().lower() == "dark" else "Dark")
        _apply_theme(root, run_pane, theme_var.get())
    theme_btn = ttk.Button(top_bar, textvariable=theme_var, width=8, command=_toggle_theme)
    theme_btn.grid(row=0, column=1, sticky="w", padx=(6, 0))

    # Mode toggle + views
    lists_frame = ttk.Frame(root)
    lists_frame.grid(row=1, column=0, sticky="nsew", padx=12, pady=10)
    root.columnconfigure(0, weight=1)
    root.rowconfigure(2, weight=1)

    # Auto mode toggle
    auto_mode_var = tk.BooleanVar(value=False)
    ttk.Checkbutton(top_bar, text="Auto mode", variable=auto_mode_var).grid(row=0, column=2, sticky="w", padx=(16, 0))

    manual_view = ManualModeView(lists_frame)
    manual_view.grid(row=0, column=0, sticky="nsew")
    auto_view = AutoModeView(lists_frame)
    auto_view.grid(row=0, column=0, sticky="nsew")
    auto_view.grid_remove()
    lists_frame.columnconfigure(0, weight=1)

    # Advanced options
    adv = AdvancedOptions(root)
    adv.grid(row=2, column=0, sticky="we", padx=12)

    # Run controls and output
    run_pane = RunPane(root)
    run_pane.grid(row=3, column=0, sticky="nsew", padx=12, pady=(8, 12))
    root.rowconfigure(3, weight=1)

    # Auto-manage per-job field grid visibility (manual mode only)
    def _refresh_per_job_fields():
        jobs = manual_view.get_jobs()
        indices: List[int] = []
        for i, (_p, u) in enumerate(jobs, start=1):
            _g, _c, t = DiscordClient.parse_ids_from_url(u)
            if _c is not None and t is None:
                indices.append(i)
        adv.set_per_job_indices(indices)

    manual_view.set_on_change(_refresh_per_job_fields)

    def _toggle_mode_ui():
        if auto_mode_var.get():
            manual_view.grid_remove()
            adv.set_per_job_indices([])  # hide per-job overrides in auto mode
            auto_view.grid()
        else:
            auto_view.grid_remove()
            manual_view.grid()
            _refresh_per_job_fields()

    auto_mode_var.trace_add("write", lambda *_: _toggle_mode_ui())

    # Preload token from env if present
    env_token = _load_token_from_env()
    if env_token:
        adv.token_var.set(env_token)

    # Per-run cancellation management
    current_cancel_events: list[threading.Event] = []

    def run_all_jobs() -> None:
        # Clear previous run panels
        run_pane.clear()
        # Cancel any prior events just in case
        for ev in current_cancel_events:
            try:
                ev.set()
            except Exception:
                pass
        current_cancel_events.clear()
        token = adv.token_var.get().strip() or _load_token_from_env() or ""
        if not token:
            messagebox.showerror("Missing token", "Please enter your Discord token or set DISCORD_TOKEN in .env")
            return
        if adv.save_token_var.get():
            try:
                # Persist to .env and update current process env for immediate use
                set_env_var("DISCORD_TOKEN", token)
                try:
                    os.environ["DISCORD_TOKEN"] = token
                except Exception:
                    pass
                run_pane.log("Saved token to .env")
            except Exception as e:
                run_pane.log(f"Failed to save token: {e}")

        # Determine jobs based on mode
        if auto_mode_var.get():
            # Auto mode collects values later (flow below)
            all_jobs = []
        else:
            all_jobs = manual_view.get_jobs()
            if not all_jobs:
                messagebox.showwarning("No jobs", "Please add at least one (input dir, Discord URL) pair")
                return

        # Parse options
        params = dict(
            token=token,
            token_type=adv.token_type_var.get(),
            post_title=adv.post_title_var.get().strip() or None,
            post_tag=adv.post_tag_var.get().strip() or None,
            relay_from=adv.relay_from_var.get().strip() or None,
            relay_download_dir=Path(adv.relay_dir_var.get().strip() or ".adms_cache"),
            ignore_dedupe=adv.ignore_dedupe_var.get(),
            dry_run=adv.dry_run_var.get(),
            history_limit=_to_int(adv.history_limit_var.get(), 1000),
            request_timeout=_to_float(adv.request_timeout_var.get(), 30.0),
            upload_timeout=_to_float(adv.upload_timeout_var.get(), 120.0),
            delay_seconds=_to_float(adv.delay_seconds_var.get(), 1.0),
            max_file_mb=_to_float(adv.max_file_mb_var.get(), 10.0),
            skip_oversize=adv.skip_oversize_var.get(),
            concurrency=_to_int(adv.concurrency_var.get(), 1),
            prepend_enabled=adv.prepend_enabled_var.get(),
            prepend_text=adv.prepend_text_var.get().strip(),
            ignore_segmentation=adv.ignore_segmentation_var.get(),
        )

        # Media types param
        media_types: list[str] = []
        if adv.media_all_var.get() or (not adv.media_videos_var.get() and not adv.media_gifs_var.get() and not adv.media_images_var.get()):
            media_types = ["all"]
        else:
            if adv.media_videos_var.get():
                media_types.append("videos")
            if adv.media_gifs_var.get():
                media_types.append("gifs")
            if adv.media_images_var.get():
                media_types.append("images")
        if media_types:
            params["media_types"] = media_types

        run_button.config(state="disabled")
        scram_button.config(state="normal")

        def worker():
            futures = []
            per_job_items = []
            # Auto mode flow
            if auto_mode_var.get():
                from .scanner import list_top_level_media_subdirs, has_root_level_media, suggest_thread_title_for_subdir
                root_dir, auto_url = auto_view.get_values()
                if not root_dir or not auto_url:
                    run_pane.log_global("Auto mode: missing root directory or upload URL")
                else:
                    try:
                        client = DiscordClient(token=params["token"], token_type=params["token_type"])  # type: ignore
                        _g, ch_id, th_id = client.parse_ids_from_url(auto_url)
                        if ch_id is None:
                            raise ValueError("Invalid Discord URL")
                        ch = client.get_channel(ch_id, request_timeout=params["request_timeout"])  # type: ignore
                        ch_type = ch.get("type") if ch else None
                        is_forum_like = ch_type in (15, 16) if ch is not None else False
                        send_as_one = auto_view.get_send_as_one()
                        if (is_forum_like and th_id is None) and not send_as_one:
                            # Determine groups: root-only files and subfolders with media
                            subdirs = list_top_level_media_subdirs(root_dir)
                            root_has = has_root_level_media(root_dir)
                            groups: list[tuple[str, Path, bool]] = []  # (title_suggestion, path, only_root_level)
                            if root_has:
                                # Root-only group uses root_dir, flagged to filter at core layer
                                groups.append((root_dir.name, root_dir, True))
                            for p in subdirs:
                                groups.append((suggest_thread_title_for_subdir(p), p, False))

                            if not groups:
                                run_pane.log_global("Auto mode: no media found in root or subfolders")
                            max_workers = max(1, min(6, len(groups)))
                            run_pane.log_global(f"Starting {len(groups)} auto job(s)...")
                            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                                for idx, (title_suggestion, path_to_send, only_root) in enumerate(groups, start=1):
                                    cancel_event = threading.Event()
                                    current_cancel_events.append(cancel_event)
                                    def make_stop(ev: threading.Event):
                                        return lambda: ev.set()
                                    item = run_pane.add_job_panel(f"Auto {idx}: {path_to_send.name} -> {auto_url}", on_stop=make_stop(cancel_event))
                                    run_pane.log_to(item, f"Queued: {path_to_send} -> {auto_url}")
                                    per_job_items.append(item)
                                    job_params = dict(params)

                                    # Determine default job title with prepend if configured
                                    job_title = title_suggestion
                                    if job_params.get("prepend_enabled", False) and job_params.get("prepend_text"):
                                        job_title = f"{job_params['prepend_text']} {job_title}"

                                    # Check for existing thread with this name
                                    final_title = job_title
                                    use_existing_tid: Optional[str] = None
                                    try:
                                        run_pane.log("[gui] checking for existing thread...")
                                        existing_thread_id = client.find_existing_thread_by_name(
                                            ch_id, job_title, request_timeout=params["request_timeout"], guild_id=_g
                                        )
                                    except Exception as ex:
                                        run_pane.log(f"[gui] existing-thread lookup failed: {ex}")
                                        existing_thread_id = None

                                    if existing_thread_id:
                                        use_existing_tid = existing_thread_id
                                        run_pane.log(f"[gui] using existing thread: {job_title}")
                                    else:
                                        # Propose a unique name and prompt the user
                                        try:
                                            from tkinter import simpledialog
                                            import tkinter as tk
                                            base_name = job_title
                                            counter = 2
                                            while True:
                                                test_name = f"{base_name} ({counter})"
                                                try:
                                                    test_thread_id = client.find_existing_thread_by_name(
                                                        ch_id, test_name, request_timeout=params["request_timeout"], guild_id=_g
                                                    )
                                                except Exception as ex:
                                                    run_pane.log(f"[gui] thread name test failed: {ex}")
                                                    test_thread_id = None
                                                if not test_thread_id:
                                                    break
                                                counter += 1
                                            root_win = tk._default_root
                                            new_title = simpledialog.askstring(
                                                "New thread title",
                                                f'Enter new thread title for "{path_to_send.name}" (suggested: "{test_name}"):',
                                                initialvalue=test_name,
                                                parent=root_win,
                                            )
                                            if new_title is None:
                                                # cancelled; skip this job
                                                run_pane.log_to(item, "Thread creation cancelled for this group; skipping.")
                                                continue
                                            final_title = new_title.strip() or test_name
                                            if job_params.get("prepend_enabled", False) and job_params.get("prepend_text"):
                                                prepend_text = job_params["prepend_text"]
                                                if not final_title.startswith(prepend_text):
                                                    final_title = f"{prepend_text} {final_title}"
                                        except Exception as e:
                                            run_pane.log_to(item, f"Thread title prompt failed; using default: {e}")
                                            final_title = job_title

                                    # Set post_title unless we're using an existing thread (then modify URL)
                                    if use_existing_tid:
                                        group_url = f"{auto_url}/threads/{use_existing_tid}"
                                    else:
                                        group_url = auto_url
                                        job_params["post_title"] = final_title

                                    if only_root:
                                        job_params["only_root_level"] = True

                                    def make_logger(itm: dict):
                                        return lambda msg: run_pane.log_to(itm, msg)
                                    futures.append(ex.submit(
                                        send_media_job,
                                        input_dir=path_to_send,
                                        channel_url=group_url,
                                        **job_params,
                                        cancel_event=cancel_event,
                                        on_log=make_logger(item),
                                    ))
                                future_to_item = {fut: itm for fut, itm in zip(futures, per_job_items)}
                                for f in as_completed(futures):
                                    item = future_to_item.get(f)
                                    try:
                                        result = f.result()
                                        if item is not None:
                                            run_pane.log_to(item, f"Done: {result}")
                                    except Exception as e:
                                        if item is not None:
                                            run_pane.log_to(item, f"Failed: {e}")
                            run_pane.log_global("All auto jobs finished.")
                        else:
                            # Single job: either non-forum, existing thread, or forum with send_as_one
                            # If forum-like without thread id and send_as_one, handle thread creation/checking
                            if is_forum_like and th_id is None and send_as_one:
                                # Check for existing threads and prompt user for choice
                                title_holder: list[str] = [""]
                                use_existing_holder: list[Optional[str]] = [None]  # thread_id if using existing
                                done_evt = threading.Event()

                                def _handle_thread_title():
                                    try:
                                        from tkinter import simpledialog, messagebox
                                        import tkinter as tk

                                        title_default = root_dir.name

                                        # Apply prepend text if enabled
                                        if params.get("prepend_enabled", False) and params.get("prepend_text"):
                                            title_default = f"{params['prepend_text']} {title_default}"

                                        # Check if a thread with this name already exists
                                        try:
                                            run_pane.log("[gui] checking for existing thread...")
                                            existing_thread_id = client.find_existing_thread_by_name(
                                                ch_id, title_default, request_timeout=params["request_timeout"], guild_id=_g
                                            )
                                        except Exception as ex:
                                            run_pane.log(f"[gui] existing-thread lookup failed: {ex}")
                                            existing_thread_id = None

                                        # Resolve a root window reference once for parenting dialogs
                                        root_win = tk._default_root
                                        if existing_thread_id:
                                            # Auto-use existing thread without further prompts
                                            title_holder[0] = title_default
                                            use_existing_holder[0] = existing_thread_id
                                            run_pane.log(f"[gui] using existing thread: {title_default}")
                                            return
                                        else:
                                                # Generate unique name
                                                base_name = title_default
                                                counter = 2
                                                while True:
                                                    test_name = f"{base_name} ({counter})"
                                                    try:
                                                        test_thread_id = client.find_existing_thread_by_name(
                                                            ch_id, test_name, request_timeout=params["request_timeout"], guild_id=_g
                                                        )
                                                    except Exception as ex:
                                                        run_pane.log(f"[gui] thread name test failed: {ex}")
                                                        test_thread_id = None
                                                    if not test_thread_id:
                                                        break
                                                    counter += 1

                                                # Ask for new name
                                                new_title = simpledialog.askstring(
                                                    "New thread title",
                                                    f'Enter new thread title for folder "{root_dir.name}" (suggested: "{test_name}"):',
                                                    initialvalue=test_name,
                                                    parent=root_win,
                                                )
                                                if new_title is None:
                                                    title_holder[0] = ""
                                                    use_existing_holder[0] = None
                                                    return
                                                final_title = new_title.strip() or test_name
                                                # Apply prepend text if enabled and user didn't already include it
                                                if params.get("prepend_enabled", False) and params.get("prepend_text"):
                                                    prepend_text = params["prepend_text"]
                                                    if not final_title.startswith(prepend_text):
                                                        final_title = f"{prepend_text} {final_title}"
                                                run_pane.log(f"[gui] creating new thread: {final_title}")
                                                title_holder[0] = final_title
                                                use_existing_holder[0] = None
                                        

                                    except Exception as e:
                                        # Fallback to default on error
                                        fallback_title = root_dir.name
                                        # Apply prepend text if enabled
                                        if params.get("prepend_enabled", False) and params.get("prepend_text"):
                                            fallback_title = f"{params['prepend_text']} {fallback_title}"
                                        title_holder[0] = fallback_title
                                        use_existing_holder[0] = None
                                        run_pane.log(f"Error in thread handling: {e}")
                                    finally:
                                        try:
                                            done_evt.set()
                                        except Exception:
                                            pass

                                try:
                                    run_pane.after(0, _handle_thread_title)
                                    done_evt.wait()
                                except Exception:
                                    pass

                                if title_holder[0]:
                                    params["post_title"] = title_holder[0]
                                    # If using existing thread, we need to modify the URL to include the thread ID
                                    if use_existing_holder[0]:
                                        auto_url = f"{auto_url}/threads/{use_existing_holder[0]}"
                                        run_pane.log(f"Using existing thread: {title_holder[0]}")
                                else:
                                    # Cancelled or failed
                                    run_pane.log("Thread creation cancelled.")
                                    run_button.config(state="normal")
                                    scram_button.config(state="disabled")
                                    return
                            cancel_event = threading.Event()
                            current_cancel_events.append(cancel_event)
                            def make_stop(ev: threading.Event):
                                return lambda: ev.set()
                            item = run_pane.add_job_panel(f"Auto: {root_dir.name} -> {auto_url}", on_stop=make_stop(cancel_event))
                            run_pane.log_to(item, f"Queued: {root_dir} -> {auto_url}")
                            def make_logger(itm: dict):
                                return lambda msg: run_pane.log_to(itm, msg)
                            try:
                                result = send_media_job(
                                    input_dir=root_dir,
                                    channel_url=auto_url,
                                    **params,
                                    cancel_event=cancel_event,
                                    on_log=make_logger(item),
                                )
                                run_pane.log_to(item, f"Done: {result}")
                            except Exception as e:
                                run_pane.log_to(item, f"Failed: {e}")
                    except Exception as e:
                        run_pane.log_global(f"Auto mode failed to initialize: {e}")
                run_button.config(state="normal")
                scram_button.config(state="disabled")
                return

            # Manual mode flow (existing)
            run_pane.log_global(f"Starting {len(all_jobs)} job(s)...")
            max_workers = max(1, min(6, len(all_jobs)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                per_job_items = []
                # Build per-job override map for URLs that are posts (no thread id)
                post_url_indices: List[int] = []
                for i, (_p, u) in enumerate(all_jobs, start=1):
                    _g, _c, t = DiscordClient.parse_ids_from_url(u)
                    if _c is not None and t is None:
                        post_url_indices.append(i)
                override_list = adv.get_per_job_overrides()
                override_map: dict[int, Tuple[str, str]] = {}
                for k, job_idx in enumerate(post_url_indices):
                    if k < len(override_list):
                        override_map[job_idx] = override_list[k]
                for idx, (p, url) in enumerate(all_jobs, start=1):
                    cancel_event = threading.Event()
                    current_cancel_events.append(cancel_event)
                    def make_stop(ev: threading.Event):
                        return lambda: ev.set()
                    item = run_pane.add_job_panel(f"Job {idx}: {p.name} -> {url}", on_stop=make_stop(cancel_event))
                    run_pane.log_to(item, f"Queued: {p} -> {url}")
                    per_job_items.append(item)
                    # Apply per-job overrides if provided for this job
                    job_params = dict(params)
                    title_override, tag_override = override_map.get(idx, ("", ""))
                    if title_override:
                        job_params["post_title"] = title_override
                    if tag_override:
                        job_params["post_tag"] = tag_override
                    def make_logger(itm: dict):
                        return lambda msg: run_pane.log_to(itm, msg)
                    def make_on_thread_created(idx_local: int, item_local: dict, job_name: str):
                        # Update the corresponding row URL StringVar and panel title safely from worker
                        def _cb(new_url: str):
                            try:
                                # Update UI on main thread
                                def _apply():
                                    try:
                                        # Update the jobs list row URL
                                        jobs = manual_view.jobs_list.rows
                                        if 0 <= idx_local - 1 < len(jobs):
                                            jobs[idx_local - 1].url_var.set(new_url)
                                        # Update the panel title to reflect the new URL
                                        frame = item_local.get("frame")
                                        if frame is not None:
                                            try:
                                                frame.configure(text=f"Job {idx_local}: {job_name} -> {new_url}")
                                            except Exception:
                                                pass
                                        run_pane.log_to(item_local, f"Thread created -> {new_url}")
                                    except Exception:
                                        pass
                                run_pane.after(0, _apply)
                            except Exception:
                                pass
                        return _cb

                    futures.append(ex.submit(
                        send_media_job,
                        input_dir=p,
                        channel_url=url,
                        **job_params,
                        cancel_event=cancel_event,
                        on_log=make_logger(item),
                        on_thread_created=make_on_thread_created(idx, item, p.name),
                    ))
                # Map futures to their corresponding UI panels to avoid index mismatches
                future_to_item = {fut: itm for fut, itm in zip(futures, per_job_items)}
                for f in as_completed(futures):
                    item = future_to_item.get(f)
                    try:
                        result = f.result()
                        if item is not None:
                            run_pane.log_to(item, f"Done: {result}")
                    except Exception as e:
                        if item is not None:
                            run_pane.log_to(item, f"Failed: {e}")
            run_pane.log_global("All jobs finished.")
            run_button.config(state="normal")
            scram_button.config(state="disabled")

        t = threading.Thread(target=worker, daemon=True)
        _worker_thread[0] = t
        t.start()

    # Bottom controls
    controls = ttk.Frame(root)
    controls.grid(row=4, column=0, sticky="we", padx=12, pady=(0, 12))
    run_button = ttk.Button(controls, text="Run all", command=run_all_jobs)
    run_button.grid(row=0, column=0, sticky="w")
    def scram_all():
        # Signal all running jobs to stop
        for ev in list(current_cancel_events):
            try:
                ev.set()
            except Exception:
                pass
    scram_button = ttk.Button(controls, text="Scram", command=scram_all)
    scram_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
    scram_button.config(state="disabled")

    # Load saved config and apply
    cfg = _load_config()
    try:
        theme = cfg.get("theme") or theme_var.get()
        theme_var.set(theme)
        _apply_theme(root, run_pane, theme)
        # Restore mode + inputs
        auto_mode = bool(cfg.get("auto_mode", False))
        auto_mode_var.set(auto_mode)
        _toggle_mode_ui()
        if auto_mode:
            auto_root = cfg.get("auto_root") or ""
            auto_url = cfg.get("auto_url") or ""
            auto_view.set_values(auto_root, auto_url)
        else:
            jobs = cfg.get("jobs") or []
            if isinstance(jobs, list):
                norm_jobs: List[Tuple[str, str]] = []
                for item in jobs:
                    if isinstance(item, dict):
                        inp = item.get("input") or ""
                        url = item.get("url") or ""
                        if inp or url:
                            norm_jobs.append((inp, url))
                    elif isinstance(item, (list, tuple)) and len(item) == 2:
                        norm_jobs.append((str(item[0]), str(item[1])))
                manual_view.set_jobs(norm_jobs)
                _refresh_per_job_fields()
        # Restore advanced options
        # Do not persist or restore token from GUI config for security
        adv.save_token_var.set(bool(cfg.get("save_token", True)))
        adv.token_type_var.set(cfg.get("token_type") or adv.token_type_var.get())
        adv.ignore_dedupe_var.set(bool(cfg.get("ignore_dedupe", False)))
        adv.dry_run_var.set(bool(cfg.get("dry_run", False)))
        adv.skip_oversize_var.set(bool(cfg.get("skip_oversize", True)))
        adv.ignore_segmentation_var.set(bool(cfg.get("ignore_segmentation", False)))
        adv.history_limit_var.set(str(cfg.get("history_limit", adv.history_limit_var.get())))
        adv.request_timeout_var.set(str(cfg.get("request_timeout", adv.request_timeout_var.get())))
        adv.upload_timeout_var.set(str(cfg.get("upload_timeout", adv.upload_timeout_var.get())))
        adv.delay_seconds_var.set(str(cfg.get("delay_seconds", adv.delay_seconds_var.get())))
        adv.max_file_mb_var.set(str(cfg.get("max_file_mb", adv.max_file_mb_var.get())))
        adv.concurrency_var.set(str(cfg.get("concurrency", adv.concurrency_var.get() or "1")))
        adv.post_title_var.set(cfg.get("post_title", ""))
        adv.post_tag_var.set(cfg.get("post_tag", ""))
        adv.relay_from_var.set(cfg.get("relay_from", ""))
        adv.relay_dir_var.set(cfg.get("relay_dir", adv.relay_dir_var.get()))
        adv.prepend_enabled_var.set(bool(cfg.get("prepend_enabled", False)))
        adv.prepend_text_var.set(cfg.get("prepend_text", ""))
        # Restore media type selections
        adv.media_all_var.set(bool(cfg.get("media_all", True)))
        adv.media_videos_var.set(bool(cfg.get("media_videos", False)))
        adv.media_gifs_var.set(bool(cfg.get("media_gifs", False)))
        adv.media_images_var.set(bool(cfg.get("media_images", False)))
    except Exception:
        pass

    # Theme is toggled via button; no combobox binding needed

    def capture_config() -> dict:
        base = {
            "theme": theme_var.get(),
            # Manual mode jobs only persisted when manual mode enabled
            "jobs": [{"input": str(p), "url": u} for p, u in manual_view.get_jobs()],
            # Do not persist token in GUI config
            "save_token": bool(adv.save_token_var.get()),
            "token_type": adv.token_type_var.get(),
            "ignore_dedupe": bool(adv.ignore_dedupe_var.get()),
            "dry_run": bool(adv.dry_run_var.get()),
            "skip_oversize": bool(adv.skip_oversize_var.get()),
            "ignore_segmentation": bool(adv.ignore_segmentation_var.get()),
            "history_limit": _to_int(adv.history_limit_var.get(), 1000),
            "request_timeout": _to_float(adv.request_timeout_var.get(), 30.0),
            "upload_timeout": _to_float(adv.upload_timeout_var.get(), 120.0),
            "delay_seconds": _to_float(adv.delay_seconds_var.get(), 1.0),
            "max_file_mb": _to_float(adv.max_file_mb_var.get(), 10.0),
            "concurrency": _to_int(adv.concurrency_var.get(), 1),
            "post_title": adv.post_title_var.get(),
            "post_tag": adv.post_tag_var.get(),
            "relay_from": adv.relay_from_var.get(),
            "relay_dir": adv.relay_dir_var.get(),
            "prepend_enabled": bool(adv.prepend_enabled_var.get()),
            "prepend_text": adv.prepend_text_var.get(),
            # Media type selections
            "media_all": bool(adv.media_all_var.get()),
            "media_videos": bool(adv.media_videos_var.get()),
            "media_gifs": bool(adv.media_gifs_var.get()),
            "media_images": bool(adv.media_images_var.get()),
        }
        base["auto_mode"] = bool(auto_mode_var.get())
        if auto_mode_var.get():
            root_dir, auto_url = auto_view.get_values()
            base["auto_root"] = str(root_dir or "")
            base["auto_url"] = auto_url
        return base

    # Track worker thread for graceful shutdown
    _worker_thread: list[Optional[threading.Thread]] = [None]

    def on_close():
        # Signal all jobs to stop
        try:
            for ev in list(current_cancel_events):
                ev.set()
        except Exception:
            pass
        # Wait briefly for worker thread to finish
        try:
            t = _worker_thread[0]
            if t is not None and t.is_alive():
                t.join(timeout=3.0)
        except Exception:
            pass
        # Save config and close
        _save_config(capture_config())
        try:
            root.destroy()
        except Exception:
            pass

    root.protocol("WM_DELETE_WINDOW", on_close)

    # Install SIGINT handler to close gracefully on Ctrl+C (when run from terminal)
    try:
        import signal
        def _sigint_handler(signum, frame):
            on_close()
        signal.signal(signal.SIGINT, _sigint_handler)
    except Exception:
        pass

    try:
        _refresh_per_job_fields()
        root.mainloop()
    except KeyboardInterrupt:
        on_close()



