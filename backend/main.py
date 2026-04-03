from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import psycopg2
import psycopg2.extras
import os
from typing import Optional

app = FastAPI()

DB_URL = os.environ["DB_URL"]
S = "dash_projetos_metas"
BIENIO = "2º Biênio - 25/26"


def get_conn():
    return psycopg2.connect(DB_URL)


def rows(cur, sql, params=()):
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


@app.get("/")
def index():
    return FileResponse("frontend/index.html")


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/api/filtros")
def get_filtros():
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        unidades = rows(
            cur,
            f"SELECT DISTINCT sigla FROM {S}.new_planooperativo WHERE sigla IS NOT NULL ORDER BY sigla",
        )
        eixos = rows(
            cur,
            f"SELECT DISTINCT tag as eixo FROM {S}.new_tags WHERE tag LIKE 'Eixo%%' AND tag NOT LIKE '%%PENAJUSTA%%' ORDER BY tag",
        )
        trimestres = rows(
            cur,
            f"""
            SELECT DISTINCT EXTRACT(YEAR FROM terminoprevisto)::int as ano,
                EXTRACT(QUARTER FROM terminoprevisto)::int as trimestre
            FROM {S}.new_metas_encamihamentos
            WHERE terminoprevisto IS NOT NULL
            ORDER BY ano, trimestre
            """,
        )
        return {
            "unidades": [r["sigla"] for r in unidades],
            "eixos": [r["eixo"] for r in eixos],
            "trimestres": trimestres,
        }
    finally:
        c.close()


@app.get("/api/p1")
def p1():
    """Análise Geral: KPIs + status metas + metas por eixo"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        kpi = rows(
            cur,
            f"""
            SELECT COUNT(DISTINCT a.uuid_) as total_projetos,
                COUNT(DISTINCT p.uuid_) as total_metas,
                COUNT(DISTINCT a.unidade) as total_unidades
            FROM {S}.new_acaoprioritaria a
            LEFT JOIN {S}.new_planooperativo p ON p.fatheruuid = a.uuid_
            WHERE a.bienio = %s
            """,
            (BIENIO,),
        )[0]

        status = rows(
            cur,
            f"""
            SELECT p.status, COUNT(*) as qtd
            FROM {S}.new_planooperativo p
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE a.bienio = %s GROUP BY p.status ORDER BY qtd DESC
            """,
            (BIENIO,),
        )

        eixos = rows(
            cur,
            f"""
            SELECT t.tag as eixo, COUNT(DISTINCT p.uuid_) as qtd
            FROM {S}.new_planooperativo p
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE a.bienio = %s GROUP BY t.tag ORDER BY t.tag
            """,
            (BIENIO,),
        )

        return {"kpi": kpi, "status": status, "eixos": eixos}
    finally:
        c.close()


@app.get("/api/p2")
def p2():
    """Análise Geral 2: status dos projetos por eixo + status metas"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        proj_status = rows(
            cur,
            f"""
            SELECT t.tag as eixo, a.status, COUNT(*) as qtd
            FROM {S}.new_acaoprioritaria a
            JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE a.bienio = %s GROUP BY t.tag, a.status ORDER BY t.tag, a.status
            """,
            (BIENIO,),
        )

        metas_status = rows(
            cur,
            f"""
            SELECT p.status, COUNT(*) as qtd
            FROM {S}.new_planooperativo p
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE a.bienio = %s GROUP BY p.status ORDER BY qtd DESC
            """,
            (BIENIO,),
        )

        return {"proj_status": proj_status, "metas_status": metas_status}
    finally:
        c.close()


@app.get("/api/p3")
def p3():
    """Análise por Eixo: cards de contagem + breakdown por status"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        counts = rows(
            cur,
            f"""
            SELECT t.tag as eixo, COUNT(DISTINCT p.uuid_) as qtd
            FROM {S}.new_planooperativo p
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE a.bienio = %s GROUP BY t.tag ORDER BY t.tag
            """,
            (BIENIO,),
        )

        breakdown = rows(
            cur,
            f"""
            SELECT t.tag as eixo, p.status, COUNT(*) as qtd
            FROM {S}.new_planooperativo p
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE a.bienio = %s GROUP BY t.tag, p.status ORDER BY t.tag, p.status
            """,
            (BIENIO,),
        )

        return {"counts": counts, "breakdown": breakdown}
    finally:
        c.close()


@app.get("/api/p4")
def p4():
    """Análise por Projeto: metas por sigla × status"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        data = rows(
            cur,
            f"""
            SELECT a.sigla, p.status, COUNT(*) as qtd
            FROM {S}.new_planooperativo p
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE a.bienio = %s
            GROUP BY a.sigla, p.status ORDER BY a.sigla, p.status
            """,
            (BIENIO,),
        )
        return {"data": data}
    finally:
        c.close()


@app.get("/api/p5")
def p5():
    """Análise Pontos de Balanço: encaminhamentos por eixo × status_enc"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        eixo_status = rows(
            cur,
            f"""
            SELECT t.tag as eixo, e.status_enc, COUNT(*) as qtd
            FROM {S}.new_metas_encamihamentos e
            JOIN {S}.new_planooperativo p ON p.uuid_ = e.uuid_
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE a.bienio = %s GROUP BY t.tag, e.status_enc ORDER BY t.tag, e.status_enc
            """,
            (BIENIO,),
        )

        global_status = rows(
            cur,
            f"""
            SELECT e.status_enc, COUNT(*) as qtd
            FROM {S}.new_metas_encamihamentos e
            JOIN {S}.new_planooperativo p ON p.uuid_ = e.uuid_
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE a.bienio = %s GROUP BY e.status_enc ORDER BY qtd DESC
            """,
            (BIENIO,),
        )

        return {"eixo_status": eixo_status, "global_status": global_status}
    finally:
        c.close()


@app.get("/api/p6")
def p6():
    """Análise de Projetos Por PBM: encaminhamentos por sigla × status_enc"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        data = rows(
            cur,
            f"""
            SELECT a.sigla, e.status_enc, COUNT(*) as qtd
            FROM {S}.new_metas_encamihamentos e
            JOIN {S}.new_planooperativo p ON p.uuid_ = e.uuid_
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE a.bienio = %s
            GROUP BY a.sigla, e.status_enc ORDER BY a.sigla, e.status_enc
            """,
            (BIENIO,),
        )
        return {"data": data}
    finally:
        c.close()


@app.get("/api/p7")
def p7(
    sigla: Optional[str] = Query(None),
    eixo: Optional[str] = Query(None),
    ano: Optional[int] = Query(None),
    trimestre: Optional[int] = Query(None),
):
    """Análise por Unidade: tabela detalhada com LEAD (ponto de balanço seguinte)"""
    c = get_conn()
    cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # --- Filtros para metas (status chart) ---
        w_meta = ["a.bienio = %s"]
        p_meta = [BIENIO]
        if sigla:
            w_meta.append("pm.sigla = %s")
            p_meta.append(sigla)
        if eixo:
            w_meta.append("t.tag = %s")
            p_meta.append(eixo)

        status = rows(cur, f"""
            SELECT pm.status, COUNT(DISTINCT pm.uuid_) as qtd
            FROM {S}.new_planooperativo pm
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = pm.fatheruuid
            LEFT JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE {" AND ".join(w_meta)}
            GROUP BY pm.status ORDER BY qtd DESC
        """, tuple(p_meta))

        # --- Filtros base para encaminhamentos (sem trimestre — LEAD calculado antes) ---
        w_base = ["a.bienio = %s"]
        p_base = [BIENIO]
        if sigla:
            w_base.append("enc.acronym = %s")
            p_base.append(sigla)
        if eixo:
            w_base.append("t.tag = %s")
            p_base.append(eixo)

        # --- Status encaminhamentos chart (com filtro de trimestre) ---
        w_enc = list(w_base)
        p_enc = list(p_base)
        if ano:
            w_enc.append("EXTRACT(YEAR FROM enc.dt_previsao_enc)::int = %s")
            p_enc.append(ano)
        if trimestre:
            w_enc.append("CEILING(EXTRACT(MONTH FROM enc.dt_previsao_enc) / 3.0)::int = %s")
            p_enc.append(trimestre)

        enc = rows(cur, f"""
            SELECT enc.status_enc, COUNT(*) as qtd
            FROM {S}.new_metas_encamihamentos enc
            JOIN {S}.new_planooperativo p ON p.uuid_ = enc.uuid_
            JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            LEFT JOIN {S}.new_tags t ON t.owneruuid = a.uuid_ AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
            WHERE {" AND ".join(w_enc)}
            GROUP BY enc.status_enc ORDER BY qtd DESC
        """, tuple(p_enc))

        # --- Tabela detalhada: LEAD calculado antes do filtro de trimestre ---
        ano_filter   = "AND ano_enc = %s"   if ano       else ""
        tri_filter   = "AND tri_enc = %s"   if trimestre else ""
        p_outer = list(p_base) + ([ano] if ano else []) + ([trimestre] if trimestre else [])

        tabela = rows(cur, f"""
            SELECT meta, sigla, status, resultado_meta, encaminhamento,
                   trimestre_fmt, status_enc, previsao, ponto_balanco_seguinte
            FROM (
                SELECT
                    enc.entidade                                              AS meta,
                    enc.acronym                                               AS sigla,
                    enc.status,
                    enc.resultadometa                                         AS resultado_meta,
                    enc.encaminhamento,
                    CEILING(EXTRACT(MONTH FROM enc.dt_previsao_enc) / 3.0)::int
                        || 'º Tri / ' ||
                        EXTRACT(YEAR FROM enc.dt_previsao_enc)::int          AS trimestre_fmt,
                    EXTRACT(YEAR FROM enc.dt_previsao_enc)::int              AS ano_enc,
                    CEILING(EXTRACT(MONTH FROM enc.dt_previsao_enc) / 3.0)::int AS tri_enc,
                    enc.status_enc,
                    TO_CHAR(enc.dt_previsao_enc, 'DD/MM/YYYY')              AS previsao,
                    LEAD(enc.encaminhamento) OVER (
                        PARTITION BY enc.acronym, enc.entidade
                        ORDER BY enc.dt_previsao_enc
                    )                                                         AS ponto_balanco_seguinte
                FROM {S}.new_metas_encamihamentos enc
                JOIN {S}.new_planooperativo p ON p.uuid_ = enc.uuid_
                JOIN {S}.new_acaoprioritaria a ON a.uuid_ = p.fatheruuid
                LEFT JOIN {S}.new_tags t ON t.owneruuid = a.uuid_
                    AND t.tag LIKE 'Eixo%%' AND t.tag NOT LIKE '%%PENAJUSTA%%'
                WHERE {" AND ".join(w_base)}
            ) sub
            WHERE TRUE {ano_filter} {tri_filter}
            ORDER BY sigla, meta, previsao
        """, tuple(p_outer))

        return {
            "status": status,
            "enc": enc,
            "tabela": tabela,
            "filtros": {"sigla": sigla, "eixo": eixo, "ano": ano, "trimestre": trimestre},
        }
    finally:
        c.close()
