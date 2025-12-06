from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from psycopg2.extras import RealDictCursor

from database import get_db

import math

app = FastAPI(title="Corpus API")

from fastapi.middleware.cors import CORSMiddleware

# Allow cross-origin requests (needed for opening HTML via file://)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # za demo: dozvoljeno svima
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- ROOT ENDPOINT ----------


@app.get("/")
def root():
    """Simple health-check endpoint.

    Returns a short message and a link to the interactive docs.
    """
    return {
        "message": "Personality Corpus API is running.",
        "docs_url": "/docs"
    }


# ---------- HELPER FUNCTION ----------


def row_to_lemma(row: dict) -> dict:
    """
    Convert a row from the view lemma_with_example into a JSON-friendly dict.
    """
    return {
        "lemma_id": row["lemma_id"],
        "language": {
            "prefix": row["lang_prefix"],
            "iso": row["lang_iso"],
            "name": row["lang_name"],
        },
        "word_original": row["word_original"],
        "word_en": row["word_en"],
        "kernel_word": row["kernel_word"],
        "word_type": row["word_type"],
        "frequency": row["frequency"],
        "alternative_comment": row["alternative_comment"],
        "definition": row["definition"],
    }


# ---------- ENDPOINT: /languages ----------


@app.get("/languages")
def list_languages():
    """
    Returns list of all languages from the table languages.
    """
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, prefix, iso_639_1 AS iso, name, notes
                FROM languages
                ORDER BY name;
                """
            )
            rows = cur.fetchall()
    return rows


# ---------- ENDPOINT: /lemmas (advanced search) ----------


@app.get("/lemmas")
def search_lemmas(
    lang_prefix: Optional[str] = Query(
        None, description="Language prefix, e.g., SERB, POL, TURK..."
    ),
    search: Optional[str] = Query(
        None,
        description="General search – looks in word_original, word_en and definition",
    ),
    word_original: Optional[str] = Query(
        None,
        description="Search by original word (ILIKE '%...%')",
    ),
    word_en: Optional[str] = Query(
        None,
        description="Search by English equivalent (ILIKE '%...%')",
    ),
    kernel_word: Optional[str] = Query(
        None,
        description="Filtering by kernel_word (exact or partial match)",
    ),
    definition: Optional[str] = Query(
        None,
        description="Search in definitions (examples.definition ILIKE '%...%')",
    ),
    word_type: Optional[str] = Query(
        None,
        description="Word type, e.g. 'adjective', 'noun', 'verb'...",
    ),
    sort_by: str = Query(
        "lemma_id",
        description="Sorting field: lemma_id, word_original, word_en, frequency",
    ),
    sort_dir: str = Query(
        "asc",
        description="Sort direction: 'asc' or 'desc'",
    ),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
):
    """
    Advanced search over the view lemma_with_example.

    Supports:
    - filtering by language (lang_prefix)
    - text search (search)
    - specific filters (word_original, word_en, kernel_word, definition, word_type)
    - sorting (sort_by, sort_dir)
    - pagination (page, page_size)
    """

    offset = (page - 1) * page_size

    # ---- build WHERE clause
    where_clauses = []
    params = []

    if lang_prefix:
        where_clauses.append("lang_prefix = %s")
        params.append(lang_prefix)

    if word_type:
        where_clauses.append("word_type = %s")
        params.append(word_type)

    if search:
        where_clauses.append(
            "(word_original ILIKE %s OR word_en ILIKE %s OR definition ILIKE %s)"
        )
        like_pattern = f"%{search}%"
        params.extend([like_pattern, like_pattern, like_pattern])

    if word_original:
        where_clauses.append("word_original ILIKE %s")
        params.append(f"%{word_original}%")

    if word_en:
        where_clauses.append("word_en ILIKE %s")
        params.append(f"%{word_en}%")

    if kernel_word:
        where_clauses.append("kernel_word ILIKE %s")
        params.append(f"%{kernel_word}%")

    if definition:
        where_clauses.append("definition ILIKE %s")
        params.append(f"%{definition}%")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # ---- allowed sort columns
    sort_map = {
        "lemma_id": "lemma_id",
        "word_original": "word_original",
        "word_en": "word_en",
        "frequency": "frequency",
    }
    sort_column = sort_map.get(sort_by, "lemma_id")

    sort_direction = "ASC"
    if sort_dir.lower() == "desc":
        sort_direction = "DESC"

    # ---- SQL for counting (total)
    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM lemma_with_example
        {where_sql};
    """

    # ---- SQL for list of results
    list_sql = f"""
        SELECT
            lemma_id,
            lang_prefix,
            lang_iso,
            lang_name,
            word_original,
            word_en,
            kernel_word,
            word_type,
            frequency,
            alternative_comment,
            definition
        FROM lemma_with_example
        {where_sql}
        ORDER BY {sort_column} {sort_direction}
        LIMIT %s OFFSET %s;
    """

    list_params = params + [page_size, offset]

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # first total
            cur.execute(count_sql, params)
            total_row = cur.fetchone()
            total = total_row["total"] if total_row else 0

            # then results
            cur.execute(list_sql, list_params)
            rows = cur.fetchall()

    total_pages = math.ceil(total / page_size) if page_size else 1

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- ENDPOINT: /lemmas/{lemma_id} ----------


@app.get("/lemmas/{lemma_id}")
def get_lemma(lemma_id: int):
    """
    Returns one lemma by ID (with definition, if there is one).
    """

    sql = """
        SELECT
            lemma_id,
            lang_prefix,
            lang_iso,
            lang_name,
            word_original,
            word_en,
            kernel_word,
            word_type,
            frequency,
            alternative_comment,
            definition
        FROM lemma_with_example
        WHERE lemma_id = %s;
    """

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (lemma_id,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Lemma not found")

    return row_to_lemma(row)


# ---------- ENDPOINT: /kernels ----------


@app.get("/kernels")
def list_kernels(
    lang_prefix: Optional[str] = Query(
        None, description="Filtering by language prefix (e.g., SERB)"
    ),
    word_type: Optional[str] = Query(
        None, description="Word type (adjective, noun, verb...)"
    ),
    min_count: int = Query(
        1, ge=1, description="Minimum number of lemmas per kernel_word"
    ),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    page_size: int = Query(50, ge=1, le=200, description="Results per page"),
):
    """
    Returns a list of kernel_word values with counts of lemmas that have them.
    Optional filters: lang_prefix, word_type, min_count.
    """

    where_clauses = ["kernel_word IS NOT NULL"]
    params = []

    if lang_prefix:
        where_clauses.append("lang_prefix = %s")
        params.append(lang_prefix)

    if word_type:
        where_clauses.append("word_type = %s")
        params.append(word_type)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # count
    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM (
            SELECT kernel_word
            FROM lemma_with_example
            {where_sql}
            GROUP BY kernel_word
            HAVING COUNT(*) >= %s
        ) sub;
    """

    params_for_count = params + [min_count]

    # list (with pagination)
    offset = (page - 1) * page_size
    list_sql = f"""
        SELECT
            kernel_word,
            COUNT(*) AS n_lemmas
        FROM lemma_with_example
        {where_sql}
        GROUP BY kernel_word
        HAVING COUNT(*) >= %s
        ORDER BY n_lemmas DESC, kernel_word ASC
        LIMIT %s OFFSET %s;
    """

    params_for_list = params + [min_count, page_size, offset]

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(count_sql, params_for_count)
            total_row = cur.fetchone()
            total = total_row["total"] if total_row else 0

            cur.execute(list_sql, params_for_list)
            rows = cur.fetchall()

    total_pages = math.ceil(total / page_size) if page_size else 1

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "results": rows,
    }


# ---------- ENDPOINT: /lemmas/by_kernel/{kernel_word} ----------


@app.get("/lemmas/by_kernel/{kernel_word}")
def lemmas_by_kernel(
    kernel_word: str,
    lang_prefix: Optional[str] = Query(
        None, description="Filtering by language prefix (e.g., SERB)"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Returns lemmas that have the requested kernel_word.
    Optional filters: lang_prefix.
    """

    offset = (page - 1) * page_size

    where_clauses = ["kernel_word = %s"]
    params = [kernel_word]

    if lang_prefix:
        where_clauses.append("lang_prefix = %s")
        params.append(lang_prefix)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM lemma_with_example
        {where_sql};
    """

    list_sql = f"""
        SELECT
            lemma_id,
            lang_prefix,
            lang_iso,
            lang_name,
            word_original,
            word_en,
            kernel_word,
            word_type,
            frequency,
            alternative_comment,
            definition
        FROM lemma_with_example
        {where_sql}
        ORDER BY lemma_id
        LIMIT %s OFFSET %s;
    """

    list_params = params + [page_size, offset]

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(count_sql, params)
            total_row = cur.fetchone()
            total = total_row["total"] if total_row else 0

            cur.execute(list_sql, list_params)
            rows = cur.fetchall()

    total_pages = math.ceil(total / page_size) if page_size else 1

    return {
        "kernel_word": kernel_word,
        "lang_prefix": lang_prefix,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "results": [row_to_lemma(r) for r in rows],
    }


#------------ ENDPOINT: definitions search ------------


@app.get("/definitions/search")
def search_definitions(
    q: str = Query(..., description="Text to search in definitions"),
    lang_prefix: Optional[str] = Query(
        None, description="Optional language prefix (SERB, POL...)"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Definition search (definition ILIKE '%q%').
    """

    offset = (page - 1) * page_size

    where_clauses = ["definition ILIKE %s"]
    params = [f"%{q}%"]

    if lang_prefix:
        where_clauses.append("lang_prefix = %s")
        params.append(lang_prefix)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM lemma_with_example
        {where_sql};
    """

    list_sql = f"""
        SELECT
            lemma_id,
            lang_prefix,
            lang_iso,
            lang_name,
            word_original,
            word_en,
            kernel_word,
            word_type,
            frequency,
            alternative_comment,
            definition
        FROM lemma_with_example
        {where_sql}
        ORDER BY lemma_id
        LIMIT %s OFFSET %s;
    """

    list_params = params + [page_size, offset]

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(count_sql, params)
            total_row = cur.fetchone()
            total = total_row["total"] if total_row else 0

            cur.execute(list_sql, list_params)
            rows = cur.fetchall()

    total_pages = math.ceil(total / page_size) if page_size else 1

    return {
        "query": q,
        "lang_prefix": lang_prefix,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- ENDPOINT: lemmas for one language ---------


@app.get("/languages/{lang_prefix}/lemmas")
def lemmas_by_language(
    lang_prefix: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    word_type: Optional[str] = Query(
        None, description="Word type, e.g. adjective"
    ),
):
    """
    Lemmas for the requested language (by lang_prefix),
    with optional filtering by word_type.
    """

    offset = (page - 1) * page_size

    where_clauses = ["lang_prefix = %s"]
    params = [lang_prefix]

    if word_type:
        where_clauses.append("word_type = %s")
        params.append(word_type)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM lemma_with_example
        {where_sql};
    """

    list_sql = f"""
        SELECT
            lemma_id,
            lang_prefix,
            lang_iso,
            lang_name,
            word_original,
            word_en,
            kernel_word,
            word_type,
            frequency,
            alternative_comment,
            definition
        FROM lemma_with_example
        {where_sql}
        ORDER BY lemma_id
        LIMIT %s OFFSET %s;
    """

    list_params = params + [page_size, offset]

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(count_sql, params)
            total_row = cur.fetchone()
            total = total_row["total"] if total_row else 0

            cur.execute(list_sql, list_params)
            rows = cur.fetchall()

    total_pages = math.ceil(total / page_size) if page_size else 1

    return {
        "lang_prefix": lang_prefix,
        "word_type": word_type,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- SIMPLE STATS (FULL) ----------


@app.get("/stats/languages")
def stats_languages():
    """
    Number of lemmas per language (non-paginated).
    """

    sql = """
        SELECT
            lang.name AS language,
            lang.iso_639_1 AS iso,
            COUNT(l.id) AS n_lemmas
        FROM lemmas l
        JOIN languages lang ON l.language_id = lang.id
        GROUP BY lang.name, lang.iso_639_1
        ORDER BY n_lemmas DESC;
    """

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    return rows


# ---------- PAGINATED STATS /stats/languages_paged ----------


@app.get("/stats/languages_paged")
def stats_languages_paged(
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    page_size: int = Query(50, ge=1, le=200, description="Results per page"),
):
    """
    Paginirana statistika: broj lema po jeziku.

    Koristi istu logiku kao /stats/languages, ali vraća:
    - total: ukupan broj jezika
    - total_pages: ukupan broj strana
    - page, page_size
    - results: lista jezika za traženu stranu
    """

    offset = (page - 1) * page_size

    count_sql = """
        SELECT COUNT(DISTINCT lang.id) AS total_languages
        FROM lemmas l
        JOIN languages lang ON l.language_id = lang.id;
    """

    list_sql = """
        SELECT
            lang.name AS language,
            lang.iso_639_1 AS iso,
            COUNT(l.id) AS n_lemmas
        FROM lemmas l
        JOIN languages lang ON l.language_id = lang.id
        GROUP BY lang.name, lang.iso_639_1
        ORDER BY n_lemmas DESC
        LIMIT %s OFFSET %s;
    """

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(count_sql)
            row = cur.fetchone()
            total = row["total_languages"] if row else 0

            cur.execute(list_sql, (page_size, offset))
            rows = cur.fetchall()

    total_pages = math.ceil(total / page_size) if page_size else 1

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "results": rows,
    }
