import tkinter as tk
from tkinter import ttk, filedialog
from pathlib import Path
from typing import Tuple, List


class ManualModeView(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.jobs_list = None
        # Import and create the DynamicJobsList here to avoid circular imports
        from .gui import DynamicJobsList
        self.jobs_list = DynamicJobsList(self, "Manual mode jobs")
        self.jobs_list.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

    def get_jobs(self) -> List[Tuple[Path, str]]:
        if self.jobs_list:
            return self.jobs_list.get_jobs()
        return []

    def set_jobs(self, jobs: List[Tuple[str, str]]) -> None:
        if self.jobs_list:
            self.jobs_list.set_jobs(jobs)

    def set_on_change(self, callback) -> None:
        if self.jobs_list:
            self.jobs_list.set_on_change(callback)


class AutoModeView(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.columnconfigure(1, weight=1)

        # Root directory selection
        ttk.Label(self, text="Root dir:").grid(row=0, column=0, sticky="w", pady=2)
        self.root_var = tk.StringVar()
        self.root_entry = ttk.Entry(self, textvariable=self.root_var, width=40)
        self.root_entry.grid(row=0, column=1, sticky="we", pady=2)
        ttk.Button(self, text="Browse", command=self._browse_root).grid(row=0, column=2, padx=(6, 0), pady=2)

        # Discord URL
        ttk.Label(self, text="Discord URL:").grid(row=1, column=0, sticky="w", pady=2)
        self.url_var = tk.StringVar()
        self.url_entry = ttk.Entry(self, textvariable=self.url_var, width=50)
        self.url_entry.grid(row=1, column=1, columnspan=2, sticky="we", pady=2)

        # Send as one option
        self.send_as_one_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(self, text="Send as single thread", variable=self.send_as_one_var).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))

    def _browse_root(self) -> None:
        d = filedialog.askdirectory(title="Select root directory")
        if d:
            self.root_var.set(d)

    def get_values(self) -> Tuple[Path, str]:
        root_path = self.root_var.get().strip()
        url = self.url_var.get().strip()
        return (Path(root_path) if root_path else None, url)

    def set_values(self, root_path: str, url: str) -> None:
        self.root_var.set(root_path)
        self.url_var.set(url)

    def get_send_as_one(self) -> bool:
        return self.send_as_one_var.get()


