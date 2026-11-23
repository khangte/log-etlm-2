"""nmt.txt(30초 간격 VM.native_memory summary)에서 범주별 committed MB를 시점별로 뽑고 최대값을 보인다."""
import re
import sys

snaps, cur = [], None
for line in open(sys.argv[1]):
    if line.startswith("=== "):
        cur = {"ts": line[4:].strip()}
        snaps.append(cur)
    elif cur is not None:
        m = re.match(r"\s*-\s+(.+?) \(reserved=\d+MB, committed=(\d+)MB\)", line)
        if m:
            cur[m.group(1).strip()] = int(m.group(2))
        m = re.match(r"Total: reserved=\d+MB, committed=(\d+)MB", line)
        if m:
            cur["Total"] = int(m.group(1))
snaps = [s for s in snaps if "Total" in s]
cats = sorted({k for s in snaps for k in s if k != "ts"}, key=lambda k: -max(s.get(k, 0) for s in snaps))
print("category".ljust(28), "max", "last", *[s["ts"][11:19] for s in snaps[::4]])
for c in cats:
    print(c.ljust(28), max(s.get(c, 0) for s in snaps), snaps[-1].get(c, 0), *[s.get(c, 0) for s in snaps[::4]])
