from io import BytesIO
from pathlib import Path

import pytest

import scjn_sentencias_downloader as sentencias


def test_crear_esquema_sentencias_incluye_fts_y_triggers(sqlite_conn):
    sentencias.crear_esquema_completo(sqlite_conn)

    objetos = {
        (fila[0], fila[1])
        for fila in sqlite_conn.execute(
            "SELECT name, type FROM sqlite_master WHERE name LIKE 'sentencias%';"
        )
    }

    assert ("sentencias", "table") in objetos
    assert ("sentencias_fts", "table") in objetos
    assert ("sentencias_fts_insert", "trigger") in objetos
    assert ("sentencias_fts_update", "trigger") in objetos
    assert ("sentencias_fts_delete", "trigger") in objetos


def test_insert_roundtrip_sentencias_y_busqueda_sin_acentos(sqlite_conn):
    sentencias.crear_esquema_completo(sqlite_conn)
    fila = sentencias.extraer_fila_sentencia(
        {
            "expediente": "AR 123/2024",
            "tema": "Libertad de expresión",
            "resolucion": "Se analiza la constitución y su alcance.",
            "fechaResolucion": "10/02/2024",
            "urlInternet": "https://example.com/docx",
        },
        "E-001",
        "Texto completo con constitución y derechos fundamentales en detalle.",
    )

    sqlite_conn.execute(sentencias.INSERTAR_SENTENCIA_SQL, fila)
    sqlite_conn.commit()

    assert sqlite_conn.execute(
        "SELECT id_engrose FROM sentencias WHERE id_engrose = ?", ("E-001",)
    ).fetchone() == ("E-001",)

    assert sqlite_conn.execute(
        "SELECT rowid FROM sentencias_fts WHERE sentencias_fts MATCH ?",
        ("constitucion",),
    ).fetchone() is not None


def test_helpers_sentencias():
    assert sentencias.extraer_anio("01/12/2023") == 2023
    assert sentencias.extraer_anio("invalida") is None
    assert sentencias.extraer_anio(None) is None

    fila = sentencias.extraer_fila_sentencia(
        {
            "expediente": "EXP-1",
            "pertenencia": "Pleno",
            "ministro": "Ministra X",
            "tema": "Tema",
            "organoJurisdiccionalOrigen": "Origen",
            "organoResolvio": "Resolvio",
            "fechaResolucion": "15/03/2022",
            "resolucion": "Resolución",
            "votacion": "Unánime",
            "asuntosAcumulados": "N/A",
            "huellaDigital": "abc",
            "urlInternet": "https://example.com",
        },
        "ID-1",
        "Texto",
    )

    assert fila[0] == "ID-1"
    assert fila[1] == "EXP-1"
    assert fila[8] == 2022
    assert fila[14] == "Texto"


def test_normalizar_lista_ids_sentencias():
    assert sentencias.normalizar_lista_ids(None) == []
    assert sentencias.normalizar_lista_ids([1, "2"]) == ["1", "2"]
    assert sentencias.normalizar_lista_ids({"data": [1, 2]}) == ["1", "2"]
    assert sentencias.normalizar_lista_ids({"items": ["x"]}) == ["x"]
    assert sentencias.normalizar_lista_ids({"otro": []}) == []


def test_extraer_texto_docx_basico():
    assert sentencias.extraer_texto_docx(None) is None
    assert sentencias.extraer_texto_docx(b"") is None


def test_extraer_texto_docx_contenido():
    docx = pytest.importorskip("docx")
    doc = docx.Document()
    doc.add_paragraph("Este es un párrafo de prueba con más de cincuenta caracteres.")
    doc.add_paragraph("Segunda línea para confirmar extracción.")

    buffer = BytesIO()
    doc.save(buffer)

    texto = sentencias.extraer_texto_docx(buffer.getvalue())
    assert texto is not None
    assert "párrafo de prueba" in texto


def test_extraer_texto_docx_retorna_none_si_muy_corto():
    docx = pytest.importorskip("docx")
    doc = docx.Document()
    doc.add_paragraph("Texto corto")

    buffer = BytesIO()
    doc.save(buffer)

    assert sentencias.extraer_texto_docx(buffer.getvalue()) is None


def test_config_sentencias_defaults_y_overrides(tmp_path):
    cfg = sentencias.Config(db_path=tmp_path / "sentencias.db", log_dir=tmp_path / "logs")
    assert cfg.log_path == Path(tmp_path / "logs" / "ultimo_update_sentencias.log")
    assert cfg.history_log_path == Path(tmp_path / "logs" / "update_history_sentencias.log")

    log_path = tmp_path / "custom_sent.log"
    history_path = tmp_path / "history_sent.log"
    cfg_custom = sentencias.Config(
        db_path=tmp_path / "sentencias.db",
        log_dir=tmp_path / "logs_custom",
        log_path=log_path,
        history_log_path=history_path,
    )
    assert cfg_custom.log_path == log_path
    assert cfg_custom.history_log_path == history_path
