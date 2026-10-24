#!/usr/bin/env python3
"""
tools/eval_whistle.py — run Whistle over recent clips and score it against whisper.

Issue #15: Whistle (Cactus Compute) is speech-to-text in one 16.9 MB file on the
CPU, which would change how many streams one box can carry. The open question
is what it costs in quality on *our* audio — narrowband, clipped, jargon-heavy
dispatch radio, not LibriSpeech. This answers it from what is already on disk:
the last N transcripts that still have a clip, re-transcribed by Whistle, each
compared with the whisper text stored beside it.

    pip install cactus-needle
    python tools/eval_whistle.py --db tentfloor.db --clips clips -n 50
    python tools/eval_whistle.py -n 200 --jargon jargon.txt --csv whistle.csv

Isolated on purpose: it opens the store read-only (the same `mode=ro` as the
admin UI), imports nothing from the scanner, and writes nothing but the report
files you ask for. It is safe to run beside a live scanner, though Whistle will
compete with it for CPU.

Scoring
-------
Hamming distance is the wrong tool here: it only compares strings of equal
length position by position, so one inserted word shifts everything after it
and scores the rest of the sentence as wrong. Edit (Levenshtein) distance
allows insertions and deletions as well as substitutions, and over *words*
rather than characters it is word error rate, the standard ASR metric and the
one Whistle's own benchmarks report:

    WER = (substitutions + deletions + insertions) / words in the reference

Both sides are normalised first, so "Engine 3, copy." and "engine three copy"
are not counted as different. With `whisper-normalizer` installed (it ships
with `cactus-needle[train]`) this is the Whisper English normaliser, the same
one Whistle's published numbers use; otherwise a simpler built-in one.

Character error rate is reported alongside, because a radio transcript that
gets "Lantzville" as "Lanceville" is one word wrong but nearly right, and CER
tells those near misses apart from garbage.

The reference is whisper, not the truth. whisper hallucinates and mishears
too, so this measures *disagreement*, not error: a WER of 0.3 means the two
models disagree on 30% of words, and some of those are whisper's mistakes.
Listen to the worst rows (the admin UI plays them by clip id) before deciding
which side was wrong. Corpus WER is total edits over total reference words,
so a 2-word clip cannot swing the average the way a mean of per-clip WERs would.

Timing
------
Whistle's time is measured here, in-process with the model already loaded.
whisper's `latency_s` is what the scanner recorded at the time, which includes
whisper-cli loading the model from disk on every call and whatever else the
GPU was doing. Treat the comparison as indicative, not a benchmark.

Requirements:
    cactus-needle (Whistle), ffmpeg (to decode the Opus clips)
    optional: whisper-normalizer, for the Whisper text normaliser
"""

import argparse
import csv
import json
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
import urllib.parse
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SAMPLE_RATE = 16000
DECODE_TIMEOUT_S = 30


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------
_NUMBER_WORDS = {
    "zero": "0", "oh": "0", "one": "1", "two": "2", "three": "3", "four": "4",
    "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
    "ten": "10", "eleven": "11", "twelve": "12", "thirteen": "13",
    "fourteen": "14", "fifteen": "15", "sixteen": "16", "seventeen": "17",
    "eighteen": "18", "nineteen": "19", "twenty": "20", "thirty": "30",
    "forty": "40", "fifty": "50",
}


def basic_normalize(text):
    """Lowercase, strip accents and punctuation, spell single number words as
    digits. Deliberately simple: dispatch speech is mostly unit numbers, which
    whisper writes either way ("Engine 3" / "Engine three")."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c)).lower()
    # Keep digits joined across "10-4" and "3.5", drop every other separator.
    text = re.sub(r"(?<=\d)[-.](?=\d)", "", text)
    text = re.sub(r"[^\w\s']|_", " ", text)
    text = text.replace("'", "")
    return " ".join(_NUMBER_WORDS.get(w, w) for w in text.split())


def make_normalizer(kind):
    """Returns (name, fn). `kind` is "whisper", "basic" or "auto"."""
    if kind in ("whisper", "auto"):
        try:
            from whisper_normalizer.english import EnglishTextNormalizer
            whisper_norm = EnglishTextNormalizer()
            return "whisper", lambda text: " ".join(whisper_norm(text).split())
        except ImportError:
            if kind == "whisper":
                raise SystemExit("--normalizer whisper needs: pip install whisper-normalizer")
    return "basic", basic_normalize


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def align(ref, hyp):
    """Levenshtein alignment of two token sequences.

    Returns a list of (op, ref_token, hyp_token) with op in "=", "S", "D", "I".
    The full table is O(len(ref) * len(hyp)), which is nothing at transmission
    length (tens of words), and keeping it is what lets the report show *where*
    the two transcripts part ways rather than only how far apart they are.
    """
    rows, cols = len(ref) + 1, len(hyp) + 1
    dist = [[0] * cols for _ in range(rows)]
    for i in range(rows):
        dist[i][0] = i
    for j in range(cols):
        dist[0][j] = j
    for i in range(1, rows):
        for j in range(1, cols):
            cost = 0 if ref[i - 1] == hyp[j - 1] else 1
            dist[i][j] = min(dist[i - 1][j - 1] + cost,
                             dist[i - 1][j] + 1,
                             dist[i][j - 1] + 1)
    ops, i, j = [], len(ref), len(hyp)
    while i or j:
        if i and j and dist[i][j] == dist[i - 1][j - 1] + (ref[i - 1] != hyp[j - 1]):
            ops.append(("=" if ref[i - 1] == hyp[j - 1] else "S", ref[i - 1], hyp[j - 1]))
            i, j = i - 1, j - 1
        elif i and dist[i][j] == dist[i - 1][j] + 1:
            ops.append(("D", ref[i - 1], None))
            i -= 1
        else:
            ops.append(("I", None, hyp[j - 1]))
            j -= 1
    return ops[::-1]


def counts(ops):
    out = {"S": 0, "D": 0, "I": 0, "=": 0}
    for op, _, _ in ops:
        out[op] += 1
    return out


def score(reference, hypothesis, normalize=basic_normalize):
    """Compare two transcripts. Both are normalised; reference is whisper.

    WER and CER are edits divided by reference length, so they can exceed 1
    when Whistle inserts more than whisper said. An empty reference scores 0
    against an empty hypothesis and 1 against anything else, rather than
    dividing by zero.
    """
    ref_norm, hyp_norm = normalize(reference), normalize(hypothesis)
    ref_words, hyp_words = ref_norm.split(), hyp_norm.split()
    word_ops = align(ref_words, hyp_words)
    w = counts(word_ops)
    word_edits = w["S"] + w["D"] + w["I"]

    # CER over the normalised text without spaces, so word-boundary
    # disagreements ("fire fighter" / "firefighter") cost nothing.
    ref_chars, hyp_chars = list(ref_norm.replace(" ", "")), list(hyp_norm.replace(" ", ""))
    c = counts(align(ref_chars, hyp_chars))
    char_edits = c["S"] + c["D"] + c["I"]

    def rate(edits, length):
        return edits / length if length else float(edits > 0)

    return {
        "ref_norm": ref_norm,
        "hyp_norm": hyp_norm,
        "ref_words": len(ref_words),
        "hyp_words": len(hyp_words),
        "sub": w["S"], "del": w["D"], "ins": w["I"],
        "word_edits": word_edits,
        "wer": rate(word_edits, len(ref_words)),
        "ref_chars": len(ref_chars),
        "char_edits": char_edits,
        "cer": rate(char_edits, len(ref_chars)),
        "ops": word_ops,
    }


def render_diff(ops):
    """One line: matching words plain, [ref->hyp] substitutions, [-del], [+ins]."""
    parts = []
    for op, ref, hyp in ops:
        if op == "=":
            parts.append(ref)
        elif op == "S":
            parts.append(f"[{ref}->{hyp}]")
        elif op == "D":
            parts.append(f"[-{ref}]")
        else:
            parts.append(f"[+{hyp}]")
    return " ".join(parts)


def summarize(rows):
    """Corpus totals over scored rows."""
    scored = [r for r in rows if r.get("score")]
    ref_words = sum(r["score"]["ref_words"] for r in scored)
    word_edits = sum(r["score"]["word_edits"] for r in scored)
    ref_chars = sum(r["score"]["ref_chars"] for r in scored)
    char_edits = sum(r["score"]["char_edits"] for r in scored)
    audio_s = sum(r["duration_s"] or 0 for r in scored)
    whistle_s = sum(r["whistle_s"] for r in scored)
    whisper_lat = [r["whisper_latency_s"] for r in scored if r["whisper_latency_s"] is not None]
    wers = sorted(r["score"]["wer"] for r in scored)
    return {
        "clips": len(scored),
        "failed": len(rows) - len(scored),
        "corpus_wer": word_edits / ref_words if ref_words else 0.0,
        "corpus_cer": char_edits / ref_chars if ref_chars else 0.0,
        "median_wer": wers[len(wers) // 2] if wers else 0.0,
        "exact": sum(1 for r in scored if r["score"]["word_edits"] == 0),
        "whistle_empty": sum(1 for r in scored if not r["score"]["hyp_words"]),
        "sub": sum(r["score"]["sub"] for r in scored),
        "del": sum(r["score"]["del"] for r in scored),
        "ins": sum(r["score"]["ins"] for r in scored),
        "ref_words": ref_words,
        "audio_s": audio_s,
        "whistle_s": whistle_s,
        "whistle_rtf": whistle_s / audio_s if audio_s else 0.0,
        "whisper_s": sum(whisper_lat),
        "whisper_rtf": (sum(whisper_lat) /
                        sum(r["duration_s"] or 0 for r in scored if r["whisper_latency_s"] is not None)
                        if whisper_lat else None),
    }


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
def connect_readonly(db_path):
    if not Path(db_path).exists():
        raise SystemExit(f"no database at {db_path}")
    uri = f"file:{urllib.parse.quote(str(db_path))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def recent_transcripts(conn, limit, stream=None, group=None):
    """The newest `limit` transcripts whose clip is still on record.

    Transcripts outlive their clips by design, so the join filters to ones that
    can still be re-transcribed rather than returning N rows of which most are
    dead references.
    """
    where = ["e.type = 'transcript.final'"]
    params = []
    if stream:
        where.append("e.stream = ?")
        params.append(stream)
    if group:
        where.append("e.group_name = ?")
        params.append(group)
    params.append(limit)
    rows = conn.execute(f"""
        SELECT e.seq, e.ts, e.group_name, e.stream,
               json_extract(e.body, '$.data.text')       AS text,
               json_extract(e.body, '$.data.model')      AS model,
               json_extract(e.body, '$.data.duration_s') AS duration_s,
               json_extract(e.body, '$.data.latency_s')  AS latency_s,
               c.id AS clip_id, c.path AS clip_path
        FROM events e
        JOIN clips c ON c.id = json_extract(e.body, '$.data.audio.id')
        WHERE {' AND '.join(where)}
        ORDER BY e.seq DESC
        LIMIT ?
    """, params).fetchall()
    return [dict(r) for r in rows]


def resolve_clip(clip_path, clip_id, clips_dir):
    """The stored path is relative to wherever the scanner ran; fall back to
    the fan-out layout under --clips when it does not resolve from here."""
    path = Path(clip_path)
    if path.exists():
        return path
    candidate = Path(clips_dir) / clip_id[:2] / f"{clip_id}.opus"
    return candidate if candidate.exists() else None


def decode_clip(path, ffmpeg_bin="ffmpeg"):
    """Opus file -> 16 kHz mono float32 PCM bytes, the input Whistle takes."""
    result = subprocess.run(
        [ffmpeg_bin, "-hide_banner", "-loglevel", "error", "-i", str(path),
         "-f", "f32le", "-ac", "1", "-ar", str(SAMPLE_RATE), "pipe:1"],
        capture_output=True, timeout=DECODE_TIMEOUT_S)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", "replace").strip()[:200])
    return result.stdout


def load_terms(path):
    """jargon.txt terms, parsed the way the scanner parses them."""
    if not path:
        return None
    terms = [line.strip() for line in Path(path).read_text().splitlines()
             if line.strip() and not line.strip().startswith("#")]
    return terms or None


def load_hallucinations(path):
    p = Path(path)
    if not p.exists():
        return set()
    return {line.strip() for line in p.read_text().splitlines() if line.strip()}


def clean_like_scanner(text, hallucinations):
    """The post-processing the scanner applies to whisper output before it is
    stored, applied to Whistle's so both sides are filtered the same way."""
    text = re.sub(r"\*[^*]+\*", "", text).strip()
    text = re.sub(r"\[[^\]]*\]", "", text).strip()
    text = re.sub(r"\([^)]*\)", "", text).strip()
    return "" if text in hallucinations else text


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def print_report(rows, summary, normalizer_name, worst):
    print()
    for r in rows:
        if not r.get("score"):
            print(f"#{r['seq']:<7} {r['stream']:<22} FAILED  {r['error']}")
            continue
        s = r["score"]
        print(f"#{r['seq']:<7} {r['stream']:<22} {r['duration_s'] or 0:5.1f}s  "
              f"WER {s['wer']:5.2f}  CER {s['cer']:5.2f}  "
              f"whistle {r['whistle_s'] * 1000:5.0f} ms")

    scored = [r for r in rows if r.get("score")]
    if worst and scored:
        print(f"\nLargest disagreements (whisper -> whistle), normaliser: {normalizer_name}")
        for r in sorted(scored, key=lambda r: (-r["score"]["wer"], -r["score"]["ref_words"]))[:worst]:
            s = r["score"]
            print(f"\n  #{r['seq']} {r['stream']} {r['ts']}  WER {s['wer']:.2f}  clip {r['clip_id'][:12]}")
            print(f"    whisper: {r['whisper_text']}")
            print(f"    whistle: {r['whistle_text'] or '(empty)'}")
            print(f"    diff:    {render_diff(s['ops'])}")

    print("\nSummary")
    print(f"  clips scored        {summary['clips']}  ({summary['failed']} failed)")
    print(f"  corpus WER          {summary['corpus_wer']:.3f}   "
          f"(sub {summary['sub']}, del {summary['del']}, ins {summary['ins']} "
          f"over {summary['ref_words']} whisper words)")
    print(f"  corpus CER          {summary['corpus_cer']:.3f}")
    print(f"  median clip WER     {summary['median_wer']:.3f}")
    print(f"  exact matches       {summary['exact']} / {summary['clips']}")
    print(f"  whistle empty       {summary['whistle_empty']}  (whisper heard something, whistle nothing)")
    print(f"  audio               {summary['audio_s']:.1f} s")
    print(f"  whistle time        {summary['whistle_s']:.1f} s   RTF {summary['whistle_rtf']:.3f}")
    if summary["whisper_rtf"] is not None:
        print(f"  whisper time        {summary['whisper_s']:.1f} s   RTF {summary['whisper_rtf']:.3f}"
              "   (as recorded live; includes model load per call)")
    print("\n  WER here is disagreement with whisper, not error against the truth.")


def write_csv(path, rows):
    fields = ["seq", "ts", "group_name", "stream", "clip_id", "duration_s",
              "whisper_model", "whisper_latency_s", "whistle_s", "whistle_language",
              "wer", "cer", "sub", "del", "ins", "ref_words", "hyp_words",
              "whisper_text", "whistle_text", "diff", "error"]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            s = r.get("score") or {}
            writer.writerow({
                **{k: r.get(k) for k in fields if k in r},
                **{k: s.get(k) for k in ("wer", "cer", "sub", "del", "ins", "ref_words", "hyp_words")},
                "diff": render_diff(s["ops"]) if s else "",
            })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def load_whistle(weights):
    try:
        import needle
    except ImportError:
        raise SystemExit("Whistle is not installed: pip install cactus-needle")
    # The engine and weights are fetched from Hugging Face on first use.
    return needle.Whistle(weights=weights) if weights else needle.Whistle()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Re-transcribe recent clips with Whistle and score them against whisper.")
    parser.add_argument("--db", default="tentfloor.db", help="State store (opened read-only)")
    parser.add_argument("--clips", default="clips", help="Clip directory")
    parser.add_argument("-n", "--count", type=int, default=50, help="How many recent transcripts")
    parser.add_argument("--stream", help="Only this stream")
    parser.add_argument("--group", help="Only this group")
    parser.add_argument("--jargon", help="Terms file passed to Whistle as keywords (e.g. jargon.txt)")
    parser.add_argument("--language", default="en", help="Force a language; 'auto' to detect")
    parser.add_argument("--weights", help="A specific whistle .cact, e.g. a fine-tuned one")
    parser.add_argument("--hallucinations", default=str(REPO / "hallucinations.txt"),
                        help="Filter Whistle output through the scanner's hallucination list")
    parser.add_argument("--raw", action="store_true",
                        help="Score Whistle's raw output, without the scanner's cleanup")
    parser.add_argument("--normalizer", choices=("auto", "whisper", "basic"), default="auto")
    parser.add_argument("--worst", type=int, default=10, help="Show this many worst diffs")
    parser.add_argument("--csv", help="Write per-clip results here")
    parser.add_argument("--json", help="Write per-clip results and the summary here")
    parser.add_argument("--ffmpeg", default="ffmpeg")
    args = parser.parse_args(argv)

    normalizer_name, normalize = make_normalizer(args.normalizer)
    conn = connect_readonly(args.db)
    candidates = recent_transcripts(conn, args.count, stream=args.stream, group=args.group)
    conn.close()
    if not candidates:
        raise SystemExit("no transcripts with a stored clip matched")

    keywords = load_terms(args.jargon)
    hallucinations = set() if args.raw else load_hallucinations(args.hallucinations)
    language = None if args.language == "auto" else args.language
    whistle = load_whistle(args.weights)

    print(f"[eval] {len(candidates)} transcripts, normaliser {normalizer_name}, "
          f"keywords {len(keywords) if keywords else 0}, language {language or 'auto'}")

    rows = []
    # Oldest first, so the output reads in the order the radio traffic happened.
    for c in reversed(candidates):
        row = {
            "seq": c["seq"], "ts": c["ts"], "group_name": c["group_name"],
            "stream": c["stream"], "clip_id": c["clip_id"], "duration_s": c["duration_s"],
            "whisper_model": c["model"], "whisper_latency_s": c["latency_s"],
            "whisper_text": c["text"] or "", "whistle_text": None, "whistle_s": None,
            "whistle_language": None, "error": None, "score": None,
        }
        rows.append(row)
        path = resolve_clip(c["clip_path"], c["clip_id"], args.clips)
        if path is None:
            row["error"] = "clip file missing"
            continue
        try:
            pcm = decode_clip(path, args.ffmpeg)
        except Exception as e:
            row["error"] = f"decode: {e}"
            continue
        try:
            t0 = time.perf_counter()
            result = whistle.transcribe(pcm, language=language, keywords=keywords)
            row["whistle_s"] = time.perf_counter() - t0
        except Exception as e:
            row["error"] = f"whistle: {type(e).__name__}: {e}"
            continue
        text = (result.get("text") or "").strip()
        row["whistle_text"] = text if args.raw else clean_like_scanner(text, hallucinations)
        row["whistle_language"] = result.get("language")
        row["score"] = score(row["whisper_text"], row["whistle_text"], normalize)

    summary = summarize(rows)
    print_report(rows, summary, normalizer_name, args.worst)

    if args.csv:
        write_csv(args.csv, rows)
        print(f"\n[eval] wrote {args.csv}")
    if args.json:
        payload = {
            "summary": summary,
            "normalizer": normalizer_name,
            "rows": [{**{k: v for k, v in r.items() if k != "score"},
                      **({k: v for k, v in r["score"].items() if k != "ops"} if r["score"] else {}),
                      "diff": render_diff(r["score"]["ops"]) if r["score"] else None}
                     for r in rows],
        }
        Path(args.json).write_text(json.dumps(payload, indent=2))
        print(f"[eval] wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
