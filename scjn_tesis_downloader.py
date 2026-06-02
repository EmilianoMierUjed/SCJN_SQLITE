#!/usr/bin/env python3
"""
Descargador de Tesis de la SCJN
================================
Descarga las tesis/jurisprudencias del Semanario Judicial de la Federación
desde la API oficial y las guarda en SQLite con FTS5 (búsqueda de texto completo).

Usos:
  1. Descarga inicial: crea la BD desde cero (~311,000 criterios)
  2. Actualización incremental: solo baja lo nuevo, corre en minutos

Ejecutar:
  pip install requests
  python scjn_tesis_downloader.py
  python scjn_tesis_downloader.py --db ./mi_bd.db
  python scjn_tesis_downloader.py --workers 10   # más rápido
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
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
    endpoint_count: str = "/api/v1/tesis/count"
    endpoint_ids: str = "/api/v1/tesis/ids"
    endpoint_tesis: str = "/api/v1/tesis/{id_tesis}"

    db_path: Path = Path("./scjn_tesis.db")
    log_dir: Path = Path("./logs")

    pausa_segundos: float = 0.3
    ids_por_pagina: int = 100
    margen_paginas_api: int = 10
    max_paginas_sin_nuevos: int = 25

    workers: int = 5
    batch_size: int = 500
    pausa_min: float = 0.02
    pausa_max: float = 2.0

    commit_cada: int = 50
    sqlite_timeout: int = 60
    busy_timeout_ms: int = 60000

    retry_intentos: int = 3
    retry_backoff: float = 1.0
    timeout_segundos: int = 30

    log_path: Path = None
    history_log_path: Path = None

    def __post_init__(self):
        if self.log_path is None:
            self.log_path = self.log_dir / "ultimo_update.log"
        if self.history_log_path is None:
            self.history_log_path = self.log_dir / "update_history.log"
        self.log_dir.mkdir(parents=True, exist_ok=True)


# =============================================================================
# LOGGING
# =============================================================================

def configurar_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger("scjn-updater")
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
        "User-Agent": "scjn-tesis-downloader/1.0 (open-source)",
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
        logging.getLogger("scjn-updater").warning(f"Timeout: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.getLogger("scjn-updater").warning(f"Error HTTP: {url} — {e}")
        return None
    except json.JSONDecodeError:
        logging.getLogger("scjn-updater").warning(f"Respuesta no es JSON: {url}")
        return None


# =============================================================================
# FUNCIONES PARA HABLAR CON LA API
# =============================================================================

def obtener_total_tesis(sesion: requests.Session, config: Config) -> Optional[int]:
    url = f"{config.base_url}{config.endpoint_count}"
    payload = pedir_json(sesion, url, config.timeout_segundos)
    if isinstance(payload, int):
        return payload
    if isinstance(payload, str) and payload.isdigit():
        return int(payload)
    return None


def normalizar_lista_ids(payload) -> list:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for clave in ["data", "ids", "resultados", "results", "items"]:
            valor = payload.get(clave)
            if isinstance(valor, list):
                return valor
    return []


def obtener_ids_pagina(sesion: requests.Session, config: Config, pagina: int) -> list:
    url = f"{config.base_url}{config.endpoint_ids}?page={pagina}"
    return normalizar_lista_ids(pedir_json(sesion, url, config.timeout_segundos))


def formatear_materias(tesis: dict) -> str:
    materias = tesis.get("materias", [])
    if isinstance(materias, list):
        return ", ".join(materias)
    return str(materias)


# =============================================================================
# ESQUEMA SQL (BASE + FTS)
# =============================================================================

ESQUEMA_TABLA = """
CREATE TABLE IF NOT EXISTS tesis (
    id_tesis       TEXT PRIMARY KEY,
    rubro          TEXT,
    epoca          TEXT,
    instancia      TEXT,
    organo_juris   TEXT,
    fuente         TEXT,
    tipo_tesis     TEXT,
    anio           INTEGER,
    mes            TEXT,
    materias       TEXT,
    tesis_codigo   TEXT,
    huella_digital TEXT,
    texto          TEXT,
    precedentes    TEXT,
    fecha_descarga TEXT DEFAULT (datetime('now'))
);
"""

ESQUEMA_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS tesis_fts USING fts5(
    rubro, texto, precedentes, materias,
    content='tesis',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS tesis_fts_insert
    AFTER INSERT ON tesis BEGIN
    INSERT INTO tesis_fts(rowid, rubro, texto, precedentes, materias)
    VALUES (new.rowid, new.rubro, new.texto, new.precedentes, new.materias);
END;

CREATE TRIGGER IF NOT EXISTS tesis_fts_update
    AFTER UPDATE ON tesis BEGIN
    UPDATE tesis_fts SET
        rubro=new.rubro, texto=new.texto,
        precedentes=new.precedentes, materias=new.materias
    WHERE rowid=old.rowid;
END;

CREATE TRIGGER IF NOT EXISTS tesis_fts_delete
    AFTER DELETE ON tesis BEGIN
    DELETE FROM tesis_fts WHERE rowid=old.rowid;
END;
"""

INSERTAR_TESIS_SQL = """
    INSERT OR REPLACE INTO tesis (
        id_tesis, rubro, epoca, instancia, organo_juris, fuente,
        tipo_tesis, anio, mes, materias, tesis_codigo, huella_digital,
        texto, precedentes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def crear_esquema_base(conn: sqlite3.Connection) -> None:
    """Solo la tabla de datos (sin FTS). Para carga inicial."""
    conn.execute(ESQUEMA_TABLA)
    conn.commit()


def crear_esquema_completo(conn: sqlite3.Connection) -> None:
    """Tabla + FTS + triggers. Para incremental o después de carga inicial."""
    conn.executescript(ESQUEMA_TABLA + ESQUEMA_FTS)
    conn.commit()


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def extraer_fila_tesis(tesis: dict, id_tesis: str) -> tuple:
    """Convierte el JSON de la API en una tupla para INSERT por lotes."""
    return (
        str(tesis.get("idTesis", id_tesis)),
        tesis.get("rubro", ""),
        tesis.get("epoca", ""),
        tesis.get("instancia", ""),
        tesis.get("organoJuris", ""),
        tesis.get("fuente", ""),
        tesis.get("tipoTesis", ""),
        tesis.get("anio", None),
        tesis.get("mes", ""),
        formatear_materias(tesis),
        tesis.get("tesis", ""),
        tesis.get("huellaDigital", ""),
        tesis.get("texto", ""),
        tesis.get("precedentes", ""),
    )


def descargar_tesis_worker(id_tesis: str, config: Config) -> Optional[tuple]:
    """
    Descarga una tesis desde la API y devuelve la fila lista para insertar.
    Cada worker crea su propia sesión HTTP (thread-safe).
    """
    sesion = crear_sesion_http(config)
    try:
        url = f"{config.base_url}{config.endpoint_tesis.format(id_tesis=id_tesis)}"
        payload = pedir_json(sesion, url, config.timeout_segundos)
        if not isinstance(payload, dict):
            return None
        return extraer_fila_tesis(payload, id_tesis)
    finally:
        sesion.close()


# =============================================================================
# LÓGICA PRINCIPAL DE DESCARGA
# =============================================================================

def descargar(config: Config, sesion: requests.Session, logger: logging.Logger) -> int:
    conn = sqlite3.connect(str(config.db_path), timeout=config.sqlite_timeout)
    conn.execute(f"PRAGMA busy_timeout={config.busy_timeout_ms}")

    es_inicial = not config.db_path.exists()
    if es_inicial:
        logger.info("BD no encontrada — descarga inicial (~311,000 tesis)")
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

    total_api = obtener_total_tesis(sesion, config)
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

    ids_existentes = obtener_ids_existentes(conn)
    logger.info(f"Tesis ya en BD: {len(ids_existentes):,}")

    nuevas = 0
    errores = 0
    inicio = time.time()
    pagina = 1
    paginas_vacias = 0
    paginas_sin_nuevos = 0

    buffer: list = []

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

        ids_nuevos = [str(i) for i in ids if str(i) not in ids_existentes]

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

        logger.info(f"Página {pagina}: {len(ids_nuevos)} nueva(s) de {len(ids)} IDs")

        with ThreadPoolExecutor(max_workers=config.workers) as pool:
            futuros = {
                pool.submit(descargar_tesis_worker, id_t, config): id_t
                for id_t in ids_nuevos
            }
            for fut in as_completed(futuros):
                id_t = futuros[fut]
                try:
                    fila = fut.result(timeout=config.timeout_segundos + 10)
                    if fila is None:
                        errores += 1
                    else:
                        buffer.append(fila)
                        ids_existentes.add(id_t)
                        nuevas += 1

                        if len(buffer) >= config.batch_size:
                            conn.execute("BEGIN")
                            conn.executemany(INSERTAR_TESIS_SQL, buffer)
                            conn.commit()
                            buffer.clear()
                except Exception:
                    errores += 1

        if buffer:
            conn.execute("BEGIN")
            conn.executemany(INSERTAR_TESIS_SQL, buffer)
            conn.commit()
            buffer.clear()

        transcurrido = time.time() - inicio
        vel = nuevas / transcurrido if transcurrido > 0 else 0
        pagina_ms = (time.time() - inicio_pagina) * 1000
        logger.info(f"  {nuevas} nuevas | {errores} errores | {vel:.1f}/seg | {pagina_ms:.0f}ms/pág")

        pagina += 1

    if buffer:
        conn.execute("BEGIN")
        conn.executemany(INSERTAR_TESIS_SQL, buffer)
        conn.commit()
        buffer.clear()

    if es_inicial and nuevas > 0:
        logger.info("Creando índice FTS5...")
        inicio_fts = time.time()
        conn.execute("SELECT count(*) FROM tesis")
        total_para_fts = conn.execute("SELECT COUNT(*) FROM tesis").fetchone()[0]
        conn.executescript(ESQUEMA_FTS)
        conn.execute("INSERT INTO tesis_fts(tesis_fts) VALUES('rebuild')")
        conn.commit()
        conn.execute("PRAGMA synchronous=NORMAL")
        logger.info(f"Índice FTS5 listo ({total_para_fts:,} tesis, {time.time()-inicio_fts:.1f}s)")

    conn.commit()
    total_final = conn.execute("SELECT COUNT(*) FROM tesis").fetchone()[0]
    conn.close()

    transcurrido = time.time() - inicio
    minutos = int(transcurrido // 60)
    segundos = int(transcurrido % 60)

    logger.info("=" * 50)
    logger.info("RESUMEN")
    logger.info(f"  Nuevas descargadas: {nuevas}")
    logger.info(f"  Errores:            {errores}")
    logger.info(f"  Total en BD:        {total_final:,}")
    logger.info(f"  Tiempo:             {minutos}m {segundos}s")
    logger.info(f"  Velocidad media:    {nuevas/transcurrido:.1f}/seg" if transcurrido > 0 else "")
    logger.info("=" * 50)

    if total_api is not None and total_final < total_api:
        logger.warning(
            f"En BD: {total_final:,} vs API: {total_api:,} "
            f"— faltan {total_api - total_final:,}"
        )

    return nuevas


def obtener_ids_existentes(conn: sqlite3.Connection) -> set:
    cursor = conn.execute("SELECT id_tesis FROM tesis")
    return {fila[0] for fila in cursor.fetchall()}


# =============================================================================
# ARCHIVO DE ESTADO
# =============================================================================

def escribir_estado(config: Config, mensaje: str, nuevas: int, errores: int, total: int):
    contenido = (
        f"Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"Estado: {mensaje}\n"
        f"Tesis nuevas: {nuevas}\n"
        f"Total en BD: {total:,}\n"
        f"Errores: {errores}\n"
    )
    config.log_path.write_text(contenido, encoding="utf-8")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Descarga tesis de la SCJN a una base SQLite con FTS5",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python scjn_tesis_downloader.py              # descarga en ./scjn_tesis.db
  python scjn_tesis_downloader.py --db ./datos/tesis.db
  python scjn_tesis_downloader.py --workers 10  # más rápido (paralelo)
        """,
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("./scjn_tesis.db"),
        help="Ruta de la BD (default: ./scjn_tesis.db)",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("./logs"),
        help="Directorio para logs (default: ./logs)",
    )
    parser.add_argument(
        "--pausa",
        type=float,
        default=0.3,
        help="Segundos entre cada request (default: 0.3)",
    )
    parser.add_argument(
        "--max-paginas-sin-nuevos",
        type=int,
        default=25,
        help="En incremental, parar tras N páginas sin IDs nuevos (default: 25)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Descargas en paralelo (default: 5). 1 = modo secuencial original",
    )

    args = parser.parse_args()

    config = Config(
        db_path=args.db,
        log_dir=args.log_dir,
        pausa_segundos=args.pausa,
        max_paginas_sin_nuevos=args.max_paginas_sin_nuevos,
        workers=args.workers,
    )

    logger = configurar_logging(config)

    logger.info("=" * 50)
    logger.info("Descargador de Tesis SCJN")
    logger.info(f"BD destino: {config.db_path}")
    logger.info(f"Workers:    {config.workers}")

    sesion = crear_sesion_http(config)

    try:
        nuevas = descargar(config, sesion, logger)

        total = 0
        if config.db_path.exists():
            conn = sqlite3.connect(str(config.db_path))
            total = conn.execute("SELECT COUNT(*) FROM tesis").fetchone()[0]
            conn.close()

        if nuevas > 0:
            mensaje = f"Éxito — {nuevas} tesis nuevas descargadas"
        else:
            mensaje = "BD ya actualizada (sin tesis nuevas)"
        escribir_estado(config, mensaje, nuevas, 0, total)

        logger.info("¡Listo!")
        return 0

    except KeyboardInterrupt:
        logger.warning("Interrumpido por el usuario — progreso guardado hasta el último commit")
        return 130
    except Exception as e:
        logger.exception(f"Error fatal: {e}")
        escribir_estado(config, f"ERROR: {e}", 0, 1, 0)
        return 1
    finally:
        sesion.close()


if __name__ == "__main__":
    sys.exit(main())
