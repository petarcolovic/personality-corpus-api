from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from psycopg2.extras import RealDictCursor

from database import get_db

from collections import defaultdict

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
    Return the list of all languages from the languages table.
    """
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, prefix, iso_639_1 AS iso, name
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
        description=(
            "General search – looks in word_original, word_en and definition "
            "(case-insensitive)."
        ),
    ),
    word_original: Optional[str] = Query(
        None,
        description="Search by original word (ILIKE '%...%').",
    ),
    word_en: Optional[str] = Query(
        None,
        description="Search by English equivalent (ILIKE '%...%').",
    ),
    kernel_word: Optional[str] = Query(
        None,
        description="Search by kernel_word (ILIKE '%...%').",
    ),
    definition: Optional[str] = Query(
        None,
        description="Search in definition (definition ILIKE '%...%').",
    ),
    word_type: Optional[str] = Query(
        None,
        description="Word type, e.g., 'adjective', 'noun', 'verb'...",
    ),
    sort_by: str = Query(
        "lemma_id",
        description="Sort field: lemma_id, word_original, word_en, frequency.",
    ),
    sort_dir: str = Query(
        "asc",
        description="Sort direction: 'asc' or 'desc'.",
    ),
    page: int = Query(1, ge=1, description="Page number (starts from 1)."),
    page_size: int = Query(
        20, ge=1, le=100, description="Number of results per page."
    ),
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

    # Build WHERE clause dynamically
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

    # Sorting – allowed columns
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

    # SQL for total count
    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM lemma_with_example
        {where_sql};
    """

    # SQL for list of results
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
            # total count
            cur.execute(count_sql, params)
            total_row = cur.fetchone()
            total = total_row["total"] if total_row else 0

            # result rows
            cur.execute(list_sql, list_params)
            rows = cur.fetchall()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- ENDPOINT: /lemmas/{lemma_id} ----------


@app.get("/lemmas/{lemma_id}")
def get_lemma(lemma_id: int):
    """
    Return a single lemma by ID (with definition, if available).
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



# ---------- ENDPOINT: /lemmas/{lemma_id}/concept ----------


@app.get("/lemmas/{lemma_id}/concept")
def lemma_concept(lemma_id: int):
    """
    Given a lemma_id, return the "concept view":
    all lemmas that share its kernel_word, grouped by language.
    """

    # 1) Find the lemma and its kernel_word
    sql_lemma = """
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
            cur.execute(sql_lemma, (lemma_id,))
            lemma_row = cur.fetchone()

            if not lemma_row:
                raise HTTPException(status_code=404, detail="Lemma not found")

            kernel_word = lemma_row["kernel_word"]

            if not kernel_word:
                raise HTTPException(
                    status_code=400,
                    detail="This lemma has no kernel_word defined, cannot build concept.",
                )

            # 2) Fetch all lemmas with the same kernel_word
            sql_all = """
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
                WHERE kernel_word = %s
                ORDER BY lang_name, word_original;
            """
            cur.execute(sql_all, (kernel_word,))
            rows = cur.fetchall()

    # Group by language
    grouped = {}
    for r in rows:
        prefix = r["lang_prefix"]
        if prefix not in grouped:
            grouped[prefix] = {
                "language": {
                    "prefix": prefix,
                    "iso": r["lang_iso"],
                    "name": r["lang_name"],
                },
                "lemmas": [],
            }

        grouped[prefix]["lemmas"].append(
            {
                "lemma_id": r["lemma_id"],
                "word_original": r["word_original"],
                "word_en": r["word_en"],
                "word_type": r["word_type"],
                "frequency": r["frequency"],
                "alternative_comment": r["alternative_comment"],
                "definition": r["definition"],
            }
        )

    return {
        "focus_lemma_id": lemma_id,
        "kernel_word": kernel_word,
        "total_lemmas": len(rows),
        "languages": list(grouped.values()),
    }



# ---------- ENDPOINT: /kernels ----------


@app.get("/kernels")
def list_kernels(
    lang_prefix: Optional[str] = Query(
        None, description="Filter by language prefix (e.g., SERB)."
    ),
    word_type: Optional[str] = Query(
        None, description="Word type (adjective, noun, verb...)."
    ),
    min_count: int = Query(
        1, ge=1, description="Minimum number of lemmas per kernel_word."
    ),
):
    """
    Return a list of kernel_word values with the number of lemmas using each.

    Optional filters:
    - lang_prefix
    - word_type
    - min_count
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

    sql = f"""
        SELECT
            kernel_word,
            COUNT(*) AS n_lemmas
        FROM lemma_with_example
        {where_sql}
        GROUP BY kernel_word
        HAVING COUNT(*) >= %s
        ORDER BY n_lemmas DESC, kernel_word ASC;
    """

    params.append(min_count)

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return rows


# ---------- ENDPOINT: /lemmas/by_kernel/{kernel_word} ----------


@app.get("/lemmas/by_kernel/{kernel_word}")
def lemmas_by_kernel(
    kernel_word: str,
    lang_prefix: Optional[str] = Query(
        None, description="Filter by language prefix (e.g., SERB)."
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Return lemmas that have the given kernel_word.

    Optional filters:
    - lang_prefix
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

    return {
        "kernel_word": kernel_word,
        "lang_prefix": lang_prefix,
        "page": page,
        "page_size": page_size,
        "total": total,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- ENDPOINT: /definitions/search ----------


@app.get("/definitions/search")
def search_definitions(
    q: str = Query(..., description="Text to search in definitions."),
    lang_prefix: Optional[str] = Query(
        None, description="Optional language prefix (e.g., SERB, POL...)."
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Search in definitions (definition ILIKE '%q%').
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

    return {
        "query": q,
        "lang_prefix": lang_prefix,
        "page": page,
        "page_size": page_size,
        "total": total,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- ENDPOINT: /languages/{lang_prefix}/lemmas ----------


@app.get("/languages/{lang_prefix}/lemmas")
def lemmas_by_language(
    lang_prefix: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    word_type: Optional[str] = Query(
        None, description="Word type, e.g., 'adjective'."
    ),
):
    """
    Return lemmas for the given language (by lang_prefix),
    with an optional filter by word_type.
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

    return {
        "lang_prefix": lang_prefix,
        "word_type": word_type,
        "page": page,
        "page_size": page_size,
        "total": total,
        "results": [row_to_lemma(r) for r in rows],
    }


# ---------- ENDPOINT: /concepts/by_kernel/{kernel_word} ----------


@app.get("/concepts/by_kernel/{kernel_word}")
def concept_by_kernel(
    kernel_word: str,
    lang_prefix: Optional[str] = Query(
        None, description="Optional language prefix filter (e.g., SERB)."
    ),
):
    """
    Return a "concept view" for a given kernel_word:
    all lemmas that share this kernel_word, grouped by language.
    """

    where_clauses = ["kernel_word = %s"]
    params = [kernel_word]

    if lang_prefix:
        where_clauses.append("lang_prefix = %s")
        params.append(lang_prefix)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
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
        ORDER BY lang_name, word_original;
    """

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No lemmas found for this kernel_word")

    # Group by language
    grouped = {}
    for r in rows:
        prefix = r["lang_prefix"]
        if prefix not in grouped:
            grouped[prefix] = {
                "language": {
                    "prefix": prefix,
                    "iso": r["lang_iso"],
                    "name": r["lang_name"],
                },
                "lemmas": [],
            }

        grouped[prefix]["lemmas"].append(
            {
                "lemma_id": r["lemma_id"],
                "word_original": r["word_original"],
                "word_en": r["word_en"],
                "word_type": r["word_type"],
                "frequency": r["frequency"],
                "alternative_comment": r["alternative_comment"],
                "definition": r["definition"],
            }
        )

    return {
        "kernel_word": kernel_word,
        "lang_prefix": lang_prefix,
        "total_lemmas": len(rows),
        "languages": list(grouped.values()),
    }


# ---------- ENDPOINT: /stats/languages ----------


@app.get("/stats/languages")
def stats_languages():
    """
    Return the number of lemmas per language.
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
