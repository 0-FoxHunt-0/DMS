import re
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple


MEDIA_EXTS = {".mp4", ".gif"}


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

        for pair in self.pairs:
            mp4_exists = pair.mp4_path.name in existing
            gif_exists = pair.gif_path.name in existing
            if not mp4_exists and not gif_exists:
                filtered_pairs.append(pair)
            else:
                if not mp4_exists:
                    leftover_singles.append(SingleItem(root_key=pair.root_key, path=pair.mp4_path))
                if not gif_exists:
                    leftover_singles.append(SingleItem(root_key=pair.root_key, path=pair.gif_path))

        filtered_singles: List[SingleItem] = [s for s in self.singles if s.path.name not in existing]
        filtered_singles.extend(leftover_singles)

        return ScanResult(pairs=filtered_pairs, singles=filtered_singles)


def scan_media(root_dir: Path) -> ScanResult:
    pairs: List[PairItem] = []
    singles: List[SingleItem] = []

    # Map: (dir_key, root_name, seg_num) -> {".mp4": Path, ".gif": Path}
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
        root_name, seg_num = _normalize_name(stem)
        key = (dir_key, root_name.lower(), seg_num)
        if key not in buckets:
            buckets[key] = {}
        buckets[key][ext] = p

    for (dir_key, root_name, seg_num), files in sorted(buckets.items()):
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

    return ScanResult(pairs=pairs, singles=singles)


