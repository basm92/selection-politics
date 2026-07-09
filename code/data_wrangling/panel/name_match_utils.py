# =============================================================================
# name_match_utils.py  [SHARED — cross-pipeline name/place normalisation]
# Used by: panel_step3_candidate_roster.py, panel_step4_candidate_person_pairs.py
# (pdc_step3_build_mp_anchor.py has its own copy of the name helpers, predating
# this module -- not refactored to avoid touching already-shipped Phase 2a code)
#
# Normalisation for cross-source person/place matching: strip diacritics,
# case, and punctuation; fold noble-rank words (jhr/ridder/baron/...) that
# different sources include inconsistently; fold the historical Dutch y/ij
# spelling variance; and for places, join OCR line-wrap word-splits by
# concatenating letters only (same trick that incidentally fixes "'s Gra
# venhage" -> "sgravenhage", matching "'s-Gravenhage").
# =============================================================================
import re
import unicodedata

_NOBLE_WORDS = {"jhr", "jonkheer", "ridder", "baron", "barones", "graaf",
                "gravin", "freule"}

_TITLE_TOKEN_RE = re.compile(r"^([A-Za-z]{2,6}\.)+$")
_INITIALS_TOKEN_RE = re.compile(r"^([A-Z][a-z]?\.){1,8}$")


def parse_name(name: str | None, voornamen: str | None = None):
    """'Dr.Mr. J.R. Thorbecke' -> ('J.R.', 'Thorbecke'). Also handles a bare
    'B.W.A.E. baron van Sloet tot Oldhuis' (no leading title) since the title
    loop simply consumes zero tokens in that case."""
    if not isinstance(name, str) or not name:
        return None, None
    toks = name.split()
    i = 0
    while i < len(toks) and _TITLE_TOKEN_RE.match(toks[i]):
        i += 1
    initials = None
    if i < len(toks) and _INITIALS_TOKEN_RE.match(toks[i]):
        initials = toks[i]
        i += 1
    surname = " ".join(toks[i:]).strip()
    if isinstance(voornamen, str) and voornamen and surname.endswith(voornamen):
        surname = surname[: -len(voornamen)].strip()
    return initials, (surname or None)


def _letters_only(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def norm_surname(s: str | None) -> str:
    if not isinstance(s, str) or not s:
        return ""
    s = _letters_only(s)
    words = re.findall(r"[a-zA-Z]+", s.lower())
    words = [w for w in words if w not in _NOBLE_WORDS]
    return "".join(words).replace("ij", "y")


def norm_ini(s: str | None) -> str:
    if not isinstance(s, str) or not s:
        return ""
    return re.sub(r"[^A-Za-z]", "", s).upper()


def norm_place(s: str | None) -> str:
    """Normalise a place name for cross-source comparison: drop a
    parenthetical province/sub-place qualifier and any trailing street
    address, then concatenate remaining letters (folds diacritics, hyphens,
    stray OCR line-wrap spaces, and the "'s Gravenhage"/"'s-Gravenhage"
    spelling variance all in one step)."""
    if not isinstance(s, str) or not s:
        return ""
    s = s.split("(")[0]
    s = re.sub(r"\s+\d.*$", "", s)  # drop from a house number onward
    s = _letters_only(s)
    return "".join(re.findall(r"[a-zA-Z]+", s.lower()))


def strip_district_suffix(district: str | None) -> str | None:
    """'Amsterdam IX' -> 'Amsterdam' (numbered sub-districts of one city);
    used to approximate a pre-1918 district by its principal town."""
    if not isinstance(district, str) or not district:
        return district
    return re.sub(r"\s+(I{1,3}|I[VX]|VI{0,3}|IX)$", "", district).strip()


def lev(a: str, b: str) -> int:
    """Levenshtein edit distance (small strings)."""
    if a == b:
        return 0
    if not a or not b:
        return len(a) + len(b)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]
