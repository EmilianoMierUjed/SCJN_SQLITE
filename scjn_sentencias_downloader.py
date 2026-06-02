#!/usr/bin/env python3
"""
Descargador de Sentencias (Engroses) de la SCJN
================================================
Descarga las sentencias ejecutivas de la Suprema Corte de Justicia de la
Nación desde la API oficial del SIJ y las guarda en SQLite con FTS5
(búsqueda de texto completo). Opcionalmente descarga el .docx del engrose
y extrae su texto plano.

Usos:
  1. Descarga inicial: crea la BD desde cero (~105,000 sentencias)
  2. Actualización incremental: solo baja lo nuevo, corre en minutos
  3. Solo metadatos (sin .docx): usa --sin-docx

Ejecutar:
  pip install requests python-docx
  python scjn_sentencias_downloader.py
  python scjn_sentencias_downloader.py --sin-docx
  python scjn_sentencias_downloader.py --anio-desde 2020
  python scjn_sentencias_downloader.py --limite 500
  python scjn_sentencias_downloader.py --workers 10
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =============================================================================
# CONFIGURACIÓN
# =============================================================================

@dataclass
class Config:
    """Todos los parámetros de la descarga en un solo sitio."""
    base_url: str = "https://bicentenario.scjn.gob.mx/repositorio-scjn"
    endpoint_count: str = "/api/v1/engroses/count"
    endpoint_ids: str = "/api/v1/engroses/ids"
    endpoint_sentencia: str = "/api/v1/engroses/{id_engrose}"

    db_path: Path = Path("./scjn_sentencias.db")
    log_dir: Path = Path("./logs")

    pausa_segundos: float = 0.1
    ids_por_pagina: int = 1000
    margen_paginas_api: int = 10
    max_paginas_sin_nuevos: int = 25

    workers: int = 5
    batch_size: int = 200
    pausa_min: float = 0.02
    pausa_max: float = 2.0

    commit_cada: int = 20
    sqlite_timeout: int = 60
    busy_timeout_ms: int = 60000

    retry_intentos: int = 3
    retry_backoff: float = 1.0
    timeout_json: int = 30
    timeout_docx: int = 60

    descargar_docx: bool = True
    anio_desde: Optional[int] = None
    limite: Optional[int] = None
    dry_run: bool = False

    log_path: Path = None
    history_log_path: Path = None

    def __post_init__(self):
        if self.log_path is None:
            self.log_path = self.log_dir / "ultimo_update_sentencias.log"
        if self.history_log_path is None:
            self.history_log_path = self.log_dir / "update_history_sentencias.log"
        self.log_dir.mkdir(parents=True, exist_ok=True)


# =============================================================================
# LOGGING
# =============================================================================

def configurar_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger("scjn-updater-sentencias")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formato = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    archivo = logging.FileHandler(str(config.history_log_path), encoding="utf-8")
    archivo.setFormatter(formato)
    logger.addHandler(archivo)

    consola = logging.StreamHandler(sys.stdout)
    consola.setFormatter(formato)
    logger.addHandler(consola)

    return logger


# =============================================================================
# CLIENTE HTTP
# =============================================================================

def crear_sesion_http(config: Config) -> requests.Session:
    sesion = requests.Session()
    sesion.headers.update({
        "User-Agent": "scjn-sentencias-downloader/1.0 (open-source)",
        "Accept": "application/json",
    })

    reintentos = Retry(
        total=config.retry_intentos,
        backoff_factor=config.retry_backoff,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    sesion.mount("https://", HTTPAdapter(max_retries=reintentos))
    return sesion


def pedir_json(sesion: requests.Session, url: str, timeout: int) -> Optional[dict]:
    try:
        respuesta = sesion.get(url, timeout=timeout)
        respuesta.raise_for_status()
        return respuesta.json()
    except requests.exceptions.Timeout:
        logging.getLogger("scjn-updater-sentencias").warning(f"Timeout: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.getLogger("scjn-updater-sentencias").warning(f"Error HTTP: {url} — {e}")
        return None
    except json.JSONDecodeError:
        logging.getLogger("scjn-updater-sentencias").warning(f"Respuesta no es JSON: {url}")
        return None


# =============================================================================
# EXTRACCIÓN DE TEXTO DESDE .docx
# =============================================================================

def extraer_texto_docx(docx_bytes: Optional[bytes]) -> Optional[str]:
    if not docx_bytes:
        return None
    try:
        from docx import Document
        doc = Document(BytesIO(docx_bytes))
    except ImportError:
        return None
    except Exception:
        return None

    parrafos = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    if not parrafos:
        return None
    texto = "\n".join(parrafos)
    return texto if len(texto) >= 50 else None


# =============================================================================
# FUNCIONES PARA HABLAR CON LA API
# =============================================================================

def obtener_total(sesion: requests.Session, config: Config) -> Optional[int]:
    url = f"{config.base_url}{config.endpoint_count}"
    payload = pedir_json(sesion, url, config.timeout_json)
    if isinstance(payload, int):
        return payload
    if isinstance(payload, str) and payload.isdigit():
        return int(payload)
    return None


def normalizar_lista_ids(payload) -> list:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [str(x) for x in payload]
    if isinstance(payload, dict):
        for clave in ["data", "ids", "resultados", "results", "items"]:
            valor = payload.get(clave)
            if isinstance(valor, list):
                return [str(x) for x in valor]
    return []


def obtener_ids_pagina(sesion: requests.Session, config: Config, pagina: int) -> list:
    url = f"{config.base_url}{config.endpoint_ids}?page={pagina}"
    return normalizar_lista_ids(pedir_json(sesion, url, config.timeout_json))


# =============================================================================
# ESQUEMA SQL (BASE + FTS)
# =============================================================================

ESQUEMA_TABLA = """
CREATE TABLE IF NOT EXISTS sentencias (
    id_engrose       TEXT PRIMARY KEY,
    expediente       TEXT,
    pertenencia      TEXT,
    ministro_ponente TEXT,
    tema             TEXT,
    organo_origen    TEXT,
    organo_resolvio  TEXT,
    fecha_resolucion TEXT,
    anio             INTEGER,
    resolucion       TEXT,
    votacion         TEXT,
    asuntos_acumulados TEXT,
    huella_digital   TEXT,
    url_docx         TEXT,
    texto_completo   TEXT,
    fecha_descarga   TEXT DEFAULT (datetime('now'))
);
"""

ESQUEMA_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS sentencias_fts USING fts5(
    tema, resolucion, texto_completo, expediente,
    content='sentencias',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS sentencias_fts_insert
    AFTER INSERT ON sentencias BEGIN
    INSERT INTO sentencias_fts(rowid, tema, resolucion, texto_completo, expediente)
    VALUES (new.rowid,
            COALESCE(new.tema, ''),
            COALESCE(new.resolucion, ''),
            COALESCE(new.texto_completo, ''),
            COALESCE(new.expediente, ''));
END;

CREATE TRIGGER IF NOT EXISTS sentencias_fts_update
    AFTER UPDATE ON sentencias BEGIN
    INSERT INTO sentencias_fts(sentencias_fts, rowid, tema, resolucion, texto_completo, expediente)
    VALUES ('delete', old.rowid,
            COALESCE(old.tema, ''),
            COALESCE(old.resolucion, ''),
            COALESCE(old.texto_completo, ''),
            COALESCE(old.expediente, ''));
    INSERT INTO sentencias_fts(rowid, tema, resolucion, texto_completo, expediente)
    VALUES (new.rowid,
            COALESCE(new.tema, ''),
            COALESCE(new.resolucion, ''),
            COALESCE(new.texto_completo, ''),
            COALESCE(new.expediente, ''));
END;

CREATE TRIGGER IF NOT EXISTS sentencias_fts_delete
    AFTER DELETE ON sentencias BEGIN
    INSERT INTO sentencias_fts(sentencias_fts, rowid, tema, resolucion, texto_completo, expediente)
    VALUES ('delete', old.rowid,
            COALESCE(old.tema, ''),
            COALESCE(old.resolucion, ''),
            COALESCE(old.texto_completo, ''),
            COALESCE(old.expediente, ''));
END;
"""

INSERTAR_SENTENCIA_SQL = """
    INSERT OR REPLACE INTO sentencias (
        id_engrose, expediente, pertenencia, ministro_ponente,
        tema, organo_origen, organo_resolvio, fecha_resolucion, anio,
        resolucion, votacion, asuntos_acumulados, huella_digital,
        url_docx, texto_completo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def crear_esquema_base(conn: sqlite3.Connection) -> None:
    conn.execute(ESQUEMA_TABLA)
    conn.commit()


def crear_esquema_completo(conn: sqlite3.Connection) -> None:
    conn.executescript(ESQUEMA_TABLA + ESQUEMA_FTS)
    conn.commit()


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def extraer_anio(fecha: Optional[str]) -> Optional[int]:
    if not fecha:
        return None
    try:
        partes = fecha.split("/")
        if len(partes) >= 3:
            return int(partes[-1])
    except (ValueError, IndexError):
        pass
    return None


def extraer_fila_sentencia(meta: dict, id_engrose: str, texto_completo: Optional[str]) -> tuple:
    return (
        id_engrose,
        meta.get("expediente", ""),
        meta.get("pertenencia", ""),
        meta.get("ministro", ""),
        meta.get("tema", ""),
        meta.get("organoJurisdiccionalOrigen", ""),
        meta.get("organoResolvio", ""),
        meta.get("fechaResolucion", ""),
        extraer_anio(meta.get("fechaResolucion")),
        meta.get("resolucion", ""),
        meta.get("votacion", ""),
        meta.get("asuntosAcumulados", ""),
        meta.get("huellaDigital", ""),
        meta.get("urlInternet", ""),
        texto_completo,
    )


def descargar_sentencia_worker(
    id_engrose: str, config: Config
) -> Optional[tuple]:
    """
    Descarga metadata + opcionalmente .docx de una sentencia.
    Devuelve la fila lista para insertar, o None si error de metadata.
    Cada worker crea su propia sesión HTTP (thread-safe).
    """
    sesion = crear_sesion_http(config)
    try:
        url = f"{config.base_url}{config.endpoint_sentencia.format(id_engrose=id_engrose)}"
        meta = pedir_json(sesion, url, config.timeout_json)
        if not isinstance(meta, dict):
            return None

        texto = None
        if config.descargar_docx:
            url_docx = meta.get("urlInternet")
            if url_docx:
                try:
                    r = sesion.get(url_docx, timeout=config.timeout_docx)
                    if r.status_code == 200:
                        texto = extraer_texto_docx(r.content)
                except requests.exceptions.RequestException:
                    pass

        return extraer_fila_sentencia(meta, id_engrose, texto)
    finally:
        sesion.close()


# =============================================================================
# LÓGICA PRINCIPAL DE DESCARGA
# =============================================================================

def descargar(config: Config, sesion: requests.Session, logger: logging.Logger) -> int:
    # ── Dry run ────────────────────────────────────────────────────────
    if config.dry_run:
        logger.info("=== MODO DRY RUN — no se descarga nada ===")
        logger.info(f"API base:        {config.base_url}")
        logger.info(f"Endpoint count:  {config.endpoint_count}")
        logger.info(f"Endpoint ids:    {config.endpoint_ids} (hasta {config.ids_por_pagina}/pág)")
        logger.info(f"Endpoint detail: {config.endpoint_sentencia}")
        logger.info(f"BD destino:      {config.db_path}")
        logger.info(f"Workers:         {config.workers}")
        logger.info(f"Batch size:      {config.batch_size}")
        logger.info(f"Descargar .docx: {config.descargar_docx}")
        logger.info(f"Año desde:       {config.anio_desde}")
        logger.info(f"Límite:          {config.limite}")
        logger.info("")
        logger.info("Esquema SQL que se crearía:")
        for linea in ESQUEMA_TABLA.strip().split("\n"):
            logger.info(f"  {linea}")
        logger.info("")
        logger.info("Índice FTS5 que se crearía al final (carga inicial):")
        for linea in ESQUEMA_FTS.strip().split("\n"):
            logger.info(f"  {linea}")
        logger.info("")
        logger.info("Dry run completado — no se realizó ningún cambio.")
        return 0

    # ── Conectar a la BD ───────────────────────────────────────────────
    conn = sqlite3.connect(str(config.db_path), timeout=config.sqlite_timeout)
    conn.execute(f"PRAGMA busy_timeout={config.busy_timeout_ms}")

    # ── Detectar si es descarga inicial ────────────────────────────────
    es_inicial = not config.db_path.exists()
    if es_inicial:
        logger.info("BD no encontrada — descarga inicial (~105,000 sentencias)")
        logger.info("  Optimizaciones: workers=%d, batch inserts, FTS diferido, PRAGMA tuning", config.workers)
        config.db_path.parent.mkdir(parents=True, exist_ok=True)
        crear_esquema_base(conn)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
    else:
        logger.info(f"BD encontrada: {config.db_path}")
        conn.execute("PRAGMA journal_mode=WAL")
        crear_esquema_completo(conn)

    if config.anio_desde:
        logger.info(f"Filtro activo: solo sentencias desde {config.anio_desde}")

    if not config.descargar_docx:
        logger.info("Modo solo metadatos: no se descargarán .docx ni se extraerá texto")

    # ── Consultar total en la API ──────────────────────────────────────
    total_api = obtener_total(sesion, config)
    if total_api is not None:
        logger.info(f"Total reportado por la API: {total_api:,}")
        max_pagina = (
            total_api // config.ids_por_pagina
            + (1 if total_api % config.ids_por_pagina else 0)
            + config.margen_paginas_api
        )
    else:
        logger.warning("No se pudo obtener el total de la API — sin límite de páginas")
        max_pagina = None

    # ── Cargar los IDs que ya tenemos ──────────────────────────────────
    ids_existentes = obtener_ids_existentes(conn)
    logger.info(f"Sentencias ya en BD: {len(ids_existentes):,}")

    # ── Variables de control ───────────────────────────────────────────
    nuevas = 0
    errores_meta = 0
    texto_extraido = 0
    saltadas_por_anio = 0
    inicio = time.time()
    pagina = 1
    paginas_vacias = 0
    paginas_sin_nuevos = 0
    buffer: list = []

    # ── Bucle principal ────────────────────────────────────────────────
    while True:
        if max_pagina is not None and pagina > max_pagina:
            logger.info(f"Límite de páginas alcanzado ({max_pagina})")
            break

        inicio_pagina = time.time()
        ids = obtener_ids_pagina(sesion, config, pagina)

        if not ids:
            paginas_vacias += 1
            if paginas_vacias >= 3:
                logger.info("3 páginas vacías consecutivas — finalizando")
                break
            pagina += 1
            continue
        paginas_vacias = 0

        ids_nuevos = [i for i in ids if i not in ids_existentes]

        if not ids_nuevos:
            paginas_sin_nuevos += 1
            if (
                not es_inicial
                and paginas_sin_nuevos >= config.max_paginas_sin_nuevos
                and (total_api is None or len(ids_existentes) >= total_api)
            ):
                logger.info(
                    f"{paginas_sin_nuevos} páginas sin nuevos — "
                    "BD actualizada, finalizando"
                )
                break
            pagina += 1
            continue
        paginas_sin_nuevos = 0

        # Filtro por año: los que no pasan se guardan sin procesar en serie
        ids_a_saltar = []
        ids_a_procesar = []
        for id_e in ids_nuevos:
            if config.anio_desde:
                # Necesitamos la metadata para saber el año
                # La descargamos en el worker igual, filtrar después
                pass
            ids_a_procesar.append(id_e)

        logger.info(f"Página {pagina}: {len(ids_nuevos)} nueva(s) de {len(ids)} IDs")

        with ThreadPoolExecutor(max_workers=config.workers) as pool:
            futuros = {
                pool.submit(descargar_sentencia_worker, id_e, config): id_e
                for id_e in ids_a_procesar
            }
            for fut in as_completed(futuros):
                id_e = futuros[fut]
                try:
                    resultado = fut.result(timeout=config.timeout_json + config.timeout_docx + 10)
                    if resultado is None:
                        errores_meta += 1
                        continue

                    # Aplicar filtro de año después de descargar
                    if config.anio_desde:
                        anio_sent = resultado[8]  # índice 8 = anio
                        if anio_sent is not None and anio_sent < config.anio_desde:
                            # Guardar sin texto
                            fila_sin_texto = list(resultado)
                            fila_sin_texto[14] = None  # índice 14 = texto_completo
                            buffer.append(tuple(fila_sin_texto))
                            ids_existentes.add(id_e)
                            saltadas_por_anio += 1
                            if saltadas_por_anio % 500 == 0:
                                conn.execute("BEGIN")
                                conn.executemany(INSERTAR_SENTENCIA_SQL, buffer)
                                conn.commit()
                                buffer.clear()
                                logger.info(
                                    f"  {saltadas_por_anio} sentencias pre-{config.anio_desde} "
                                    f"omitidas (metadata guardada, sin .docx)"
                                )
                            continue

                    buffer.append(resultado)
                    ids_existentes.add(id_e)
                    nuevas += 1

                    if resultado[14] is not None:
                        texto_extraido += 1

                    if len(buffer) >= config.batch_size:
                        conn.execute("BEGIN")
                        conn.executemany(INSERTAR_SENTENCIA_SQL, buffer)
                        conn.commit()
                        buffer.clear()

                except Exception:
                    errores_meta += 1

        if buffer:
            conn.execute("BEGIN")
            conn.executemany(INSERTAR_SENTENCIA_SQL, buffer)
            conn.commit()
            buffer.clear()

        transcurrido = time.time() - inicio
        vel = nuevas / transcurrido if transcurrido > 0 else 0
        pagina_ms = (time.time() - inicio_pagina) * 1000
        logger.info(
            f"  {nuevas} nuevas | texto={texto_extraido} | "
            f"{errores_meta} errores | {vel:.1f}/seg | {pagina_ms:.0f}ms/pág"
        )

        if config.limite and nuevas >= config.limite:
            logger.info(f"Límite alcanzado ({config.limite}), deteniendo.")
            break

        pagina += 1

    if buffer:
        conn.execute("BEGIN")
        conn.executemany(INSERTAR_SENTENCIA_SQL, buffer)
        conn.commit()
        buffer.clear()

    # ── Reconstruir FTS si fue descarga inicial ─────────────────────────
    if es_inicial and nuevas > 0:
        logger.info("Creando índice FTS5...")
        inicio_fts = time.time()
        total_para_fts = conn.execute("SELECT COUNT(*) FROM sentencias").fetchone()[0]
        conn.executescript(ESQUEMA_FTS)
        conn.execute("INSERT INTO sentencias_fts(sentencias_fts) VALUES('rebuild')")
        conn.commit()
        conn.execute("PRAGMA synchronous=NORMAL")
        logger.info(f"Índice FTS5 listo ({total_para_fts:,} sentencias, {time.time()-inicio_fts:.1f}s)")

    conn.commit()
    total_final = conn.execute("SELECT COUNT(*) FROM sentencias").fetchone()[0]
    conn.close()

    # ── Resumen final ──────────────────────────────────────────────────
    transcurrido = time.time() - inicio
    minutos = int(transcurrido // 60)
    segundos = int(transcurrido % 60)

    logger.info("=" * 50)
    logger.info("RESUMEN")
    logger.info(f"  Nuevas descargadas:     {nuevas}")
    logger.info(f"  Con texto extraído:     {texto_extraido}")
    logger.info(f"  Errores metadata:       {errores_meta}")
    if saltadas_por_anio:
        logger.info(f"  Omitidas (pre-{config.anio_desde}): {saltadas_por_anio}")
    logger.info(f"  Total en BD:            {total_final:,}")
    logger.info(f"  Tiempo:                 {minutos}m {segundos}s")
    if transcurrido > 0:
        logger.info(f"  Velocidad media:        {nuevas/transcurrido:.1f}/seg")
    logger.info("=" * 50)

    if total_api is not None and total_final < total_api:
        logger.warning(
            f"En BD: {total_final:,} vs API: {total_api:,} "
            f"— faltan {total_api - total_final:,}"
        )

    return nuevas


def obtener_ids_existentes(conn: sqlite3.Connection) -> set:
    return {fila[0] for fila in conn.execute("SELECT id_engrose FROM sentencias")}


# =============================================================================
# ARCHIVO DE ESTADO
# =============================================================================

def escribir_estado(config: Config, mensaje: str, nuevas: int, total: int, errores: int = 0):
    contenido = (
        f"Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"Estado: {mensaje}\n"
        f"Sentencias nuevas: {nuevas}\n"
        f"Total en BD: {total:,}\n"
        f"Errores: {errores}\n"
    )
    config.log_path.write_text(contenido, encoding="utf-8")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Descarga sentencias (engroses) de la SCJN a una base SQLite con FTS5",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python scjn_sentencias_downloader.py --dry-run         # solo mostrar qué haría
  python scjn_sentencias_downloader.py                   # descarga completa con .docx
  python scjn_sentencias_downloader.py --sin-docx         # solo metadatos
  python scjn_sentencias_downloader.py --anio-desde 2020
  python scjn_sentencias_downloader.py --limite 500
  python scjn_sentencias_downloader.py --workers 10       # más paralelo
        """,
    )
    parser.add_argument("--db", type=Path, default=Path("./scjn_sentencias.db"),
                        help="Ruta de la BD (default: ./scjn_sentencias.db)")
    parser.add_argument("--log-dir", type=Path, default=Path("./logs"),
                        help="Directorio para logs (default: ./logs)")
    parser.add_argument("--pausa", type=float, default=0.1,
                        help="Segundos entre cada request (default: 0.1)")
    parser.add_argument("--max-paginas-sin-nuevos", type=int, default=25,
                        help="En incremental, parar tras N páginas sin IDs nuevos (default: 25)")
    parser.add_argument("--sin-docx", action="store_true",
                        help="No descargar .docx ni extraer texto (solo metadatos)")
    parser.add_argument("--anio-desde", type=int, default=None,
                        help="Solo sentencias desde este año en adelante")
    parser.add_argument("--limite", type=int, default=None,
                        help="Detenerse tras N sentencias (para pruebas)")
    parser.add_argument("--dry-run", action="store_true",
                        help="No descargar nada, solo mostrar qué haría")
    parser.add_argument("--workers", type=int, default=5,
                        help="Descargas en paralelo (default: 5). 1 = modo secuencial")

    args = parser.parse_args()

    config = Config(
        db_path=args.db,
        log_dir=args.log_dir,
        pausa_segundos=args.pausa,
        max_paginas_sin_nuevos=args.max_paginas_sin_nuevos,
        descargar_docx=not args.sin_docx,
        anio_desde=args.anio_desde,
        limite=args.limite,
        dry_run=args.dry_run,
        workers=args.workers,
    )

    logger = configurar_logging(config)

    logger.info("=" * 50)
    logger.info("Descargador de Sentencias SCJN")
    logger.info(f"BD destino: {config.db_path}")
    logger.info(f"Workers:    {config.workers}")
    logger.info(f"Descargar .docx: {'Sí' if config.descargar_docx else 'No'}")

    sesion = crear_sesion_http(config) if not config.dry_run else None

    try:
        nuevas = descargar(config, sesion, logger)

        total = 0
        if config.db_path.exists():
            conn = sqlite3.connect(str(config.db_path))
            total = conn.execute("SELECT COUNT(*) FROM sentencias").fetchone()[0]
            conn.close()

        if nuevas > 0:
            mensaje = f"Éxito — {nuevas} sentencias nuevas descargadas"
        else:
            mensaje = "BD ya actualizada (sin sentencias nuevas)"
        escribir_estado(config, mensaje, nuevas, total)

        logger.info("¡Listo!")
        return 0

    except KeyboardInterrupt:
        logger.warning("Interrumpido por el usuario — progreso guardado hasta el último commit")
        return 130
    except Exception as e:
        logger.exception(f"Error fatal: {e}")
        escribir_estado(config, f"ERROR: {e}", 0, 0, 1)
        return 1
    finally:
        if sesion is not None:
            sesion.close()


if __name__ == "__main__":
    sys.exit(main())
