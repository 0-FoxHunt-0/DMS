import re
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple
import sys


# Media type categories
VIDEO_EXTS = {".mp4", ".webm", ".mov", ".mkv"}
GIF_EXTS = {".gif"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

# Union of all recognized media extensions
MEDIA_EXTS = VIDEO_EXTS | GIF_EXTS | IMAGE_EXTS


_SEGMENT_PATTERNS = [
    re.compile(r"^(?P<root>.*?)[\._\-\s]?(?:part|seg|segment)[\._\-\s]*?(?P<num>\d{1,3})$", re.IGNORECASE),
    re.compile(r"^(?P<root>.*?)[\s]*\((?P<num>\d{1,3})\)$", re.IGNORECASE),
    re.compile(r"^(?P<root>.*?)[\._\-](?P<num>\d{1,3})$"),
]


def _normalize_name(stem: str) -> Tuple[str, Optional[int]]:
    stem = stem.strip()
    for pat in _SEGMENT_PATTERNS:
        m = pat.match(stem)
        if m:
            root = m.group("root").strip(" .-_")
            try:
                num = int(m.group("num"))
                if 1 <= num <= 999:
                    return root, num
            except Exception:
                pass
    return stem, None


@dataclass(frozen=True)
class PairItem:
    root_key: str
    mp4_path: Path
    gif_path: Path


@dataclass(frozen=True)
class SingleItem:
    root_key: str
    path: Path


@dataclass(frozen=True)
class ScanResult:
    pairs: List[PairItem]
    singles: List[SingleItem]

    def filter_against_filenames(self, existing: Set[str]) -> "ScanResult":
        filtered_pairs: List[PairItem] = []
        leftover_singles: List[SingleItem] = []

        # Normalize existing names to lowercase for case-insensitive compare
        existing_l = {name.lower() for name in existing}

        for pair in self.pairs:
            mp4_exists = pair.mp4_path.name.lower() in existing_l
            gif_exists = pair.gif_path.name.lower() in existing_l
            if not mp4_exists and not gif_exists:
                filtered_pairs.append(pair)
            else:
                if not mp4_exists:
                    leftover_singles.append(SingleItem(root_key=pair.root_key, path=pair.mp4_path))
                if not gif_exists:
                    leftover_singles.append(SingleItem(root_key=pair.root_key, path=pair.gif_path))

        filtered_singles: List[SingleItem] = [s for s in self.singles if s.path.name.lower() not in existing_l]
        filtered_singles.extend(leftover_singles)

        return ScanResult(pairs=filtered_pairs, singles=filtered_singles)


def scan_media(root_dir: Path) -> ScanResult:
    pairs: List[PairItem] = []
    singles: List[SingleItem] = []

    # Map: (dir_key, root_name, seg_num) -> {ext: Path}
    buckets: Dict[Tuple[str, str, Optional[int]], Dict[str, Path]] = {}

    for p in root_dir.rglob("*"):
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext not in MEDIA_EXTS:
            continue
        rel_dir = p.parent.relative_to(root_dir).as_posix()
        dir_key = rel_dir or "."
        stem = p.stem

        # For root directory files, check if this might be part of a segmentation
        # by looking at all files in the root directory
        if dir_key == ".":
            parent_dir = root_dir
        else:
            parent_dir = p.parent

        all_media_files = [f for f in parent_dir.iterdir() if f.is_file() and f.suffix.lower() in MEDIA_EXTS]

        # Check if this file is part of a segmented group
        # Only treat a file as segmented if it has a numeric suffix AND
        # there are multiple files with the same root but different segment numbers
        file_root, file_seg_num = _normalize_name(stem)

        if file_seg_num is not None:
            # Check if this file is part of a segmented group
            stems_in_dir = [f.stem for f in all_media_files]
            segmented_stems = []
            for s in stems_in_dir:
                root, seg_num = _normalize_name(s)
                if seg_num is not None:
                    segmented_stems.append((root.lower(), seg_num))

            # Check if there are multiple files with the same root but different segment numbers
            root_counts = {}
            for root, seg_num in segmented_stems:
                if root not in root_counts:
                    root_counts[root] = []
                root_counts[root].append(seg_num)

            # Only treat as segmented if this root has multiple segments
            should_check_segments = len(root_counts.get(file_root.lower(), [])) > 1
        else:
            should_check_segments = False

        if should_check_segments:
            root_name, seg_num = file_root, file_seg_num
        else:
            root_name, seg_num = stem, None

        key = (dir_key, root_name.lower(), seg_num)
        if key not in buckets:
            buckets[key] = {}
        buckets[key][ext] = p

    # Sort keys safely: place non-segmented (None) before segmented, then by segment number
    def _sort_key(item: Tuple[Tuple[str, str, Optional[int]], Dict[str, Path]]):
        (dir_key, root_name, seg_num), _files = item
        return (dir_key, root_name, seg_num is not None, seg_num or 0)

    for (dir_key, root_name, seg_num), files in sorted(buckets.items(), key=_sort_key):
        root_key = f"{dir_key}/{root_name}"
        mp4 = files.get(".mp4")
        gif = files.get(".gif")
        if mp4 and gif:
            pairs.append(PairItem(root_key=root_key, mp4_path=mp4, gif_path=gif))
        else:
            if mp4:
                singles.append(SingleItem(root_key=root_key, path=mp4))
            if gif:
                singles.append(SingleItem(root_key=root_key, path=gif))
            # Add other recognized media (non-mp4 videos and images) as singles
            for ext, p in files.items():
                if ext == ".mp4" or ext == ".gif":
                    continue
                if ext in MEDIA_EXTS:
                    singles.append(SingleItem(root_key=root_key, path=p))

    return ScanResult(pairs=pairs, singles=singles)


def list_top_level_media_subdirs(root_dir: Path) -> List[Path]:
    """Return immediate subdirectories of root_dir that contain media files.

    Uses scan_media to detect media presence and derives the first-level
    directory component from each item's root_key. The root (".") is ignored.
    The returned order is stable by first appearance.
    """
    result = scan_media(root_dir)
    seen: Set[str] = set()
    ordered: List[str] = []

    def _add_from_root_key(root_key: str) -> None:
        # root_key format: "<dir_key>/<root_name>", where dir_key is "." for root
        try:
            dir_part = root_key.split("/", 1)[0]
        except Exception:
            dir_part = "."
        if dir_part and dir_part != "." and dir_part not in seen:
            seen.add(dir_part)
            ordered.append(dir_part)

    for p in result.pairs:
        _add_from_root_key(p.root_key)
    for s in result.singles:
        _add_from_root_key(s.root_key)

    subdirs: List[Path] = []
    for name in ordered:
        p = root_dir / name
        # Only include existing directories; skip if removed between scan and listing
        if p.exists() and p.is_dir():
            subdirs.append(p)
    return subdirs


def has_root_level_media(root_dir: Path) -> bool:
    """Return True if the root directory contains media files directly (excluding subfolders).

    Uses scan_media and checks for any items whose root_key has a dir component of ".".
    """
    result = scan_media(root_dir)
    def _is_root(root_key: str) -> bool:
        try:
            return (root_key.split("/", 1)[0] == ".")
        except Exception:
            return False

    root_items = []
    for p in result.pairs:
        if _is_root(p.root_key):
            root_items.append(f"pair: {p.root_key}")
    for s in result.singles:
        if _is_root(s.root_key):
            root_items.append(f"single: {s.root_key}")

    if root_items:
        print(f"[DEBUG] Root level media found: {root_items}")

    for p in result.pairs:
        if _is_root(p.root_key):
            return True
    for s in result.singles:
        if _is_root(s.root_key):
            return True
    return False


def _infer_segment_base_from_dir(dir_path: Path) -> Optional[str]:
    """Infer a common segmented base name from filenames in a directory.

    If a strong majority of media stems match known segment patterns and share the
    same normalized root, return that root; otherwise None.
    """
    try:
        stems: List[str] = []
        for p in dir_path.iterdir():
            if p.is_file() and p.suffix.lower() in MEDIA_EXTS:
                stems.append(p.stem)
        if not stems:
            return None
        bases: List[str] = []
        segmented = 0
        for s in stems:
            root, seg = _normalize_name(s)
            if seg is not None:
                segmented += 1
                bases.append(root.lower())
        if not segmented:
            return None
        # Majority threshold: at least 70% segmented and share the same base
        ratio = segmented / max(1, len(stems))
        if ratio < 0.7:
            return None
        # Find dominant base
        from collections import Counter
        c = Counter(bases)
        base, count = c.most_common(1)[0]
        if count / max(1, segmented) >= 0.7:
            return base
    except Exception:
        return None
    return None


def suggest_thread_title_for_subdir(dir_path: Path) -> str:
    """Suggest a human-friendly thread title for a subdirectory.

    Rules:
    - If the directory name ends with "_segments", strip that suffix.
    - Else, if a common segmented base can be inferred from contents, use it.
    - Otherwise, use the directory name as-is.
    """
    name = dir_path.name
    name_l = name.lower()
    try:
        if name_l.endswith("_segments") and len(name) > len("_segments"):
            return name[: -len("_segments")]
        inferred = _infer_segment_base_from_dir(dir_path)
        if inferred:
            return inferred
    except Exception:
        pass
    return name


