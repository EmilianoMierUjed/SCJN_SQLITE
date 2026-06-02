from pathlib import Path

import scjn_tesis_downloader as tesis


def test_crear_esquema_tesis_incluye_fts_y_triggers(sqlite_conn):
    tesis.crear_esquema_completo(sqlite_conn)

    objetos = {
        (fila[0], fila[1])
        for fila in sqlite_conn.execute(
            "SELECT name, type FROM sqlite_master WHERE name LIKE 'tesis%';"
        )
    }

    assert ("tesis", "table") in objetos
    assert ("tesis_fts", "table") in objetos
    assert ("tesis_fts_insert", "trigger") in objetos
    assert ("tesis_fts_update", "trigger") in objetos
    assert ("tesis_fts_delete", "trigger") in objetos


def test_insert_roundtrip_tesis_y_busqueda_sin_acentos(sqlite_conn):
    tesis.crear_esquema_completo(sqlite_conn)
    fila = tesis.extraer_fila_tesis(
        {
            "idTesis": "T-001",
            "rubro": "Control de constitucionalidad",
            "texto": "La constitución protege derechos fundamentales.",
            "precedentes": "Precedente A",
            "materias": ["Constitucional", "Derechos Humanos"],
        },
        "fallback-id",
    )

    sqlite_conn.execute(tesis.INSERTAR_TESIS_SQL, fila)
    sqlite_conn.commit()

    assert sqlite_conn.execute(
        "SELECT id_tesis FROM tesis WHERE id_tesis = ?", ("T-001",)
    ).fetchone() == ("T-001",)

    assert sqlite_conn.execute(
        "SELECT rowid FROM tesis_fts WHERE tesis_fts MATCH ?", ("constitucion",)
    ).fetchone() is not None


def test_helpers_tesis():
    payload = {
        "idTesis": "123",
        "rubro": "Rubro",
        "anio": 2024,
        "materias": ["Civil", "Mercantil"],
    }
    fila = tesis.extraer_fila_tesis(payload, "fallback")

    assert fila[0] == "123"
    assert fila[1] == "Rubro"
    assert fila[7] == 2024
    assert fila[9] == "Civil, Mercantil"

    assert tesis.formatear_materias({"materias": ["A", "B"]}) == "A, B"
    assert tesis.formatear_materias({"materias": "Unica"}) == "Unica"


def test_normalizar_lista_ids_tesis():
    assert tesis.normalizar_lista_ids(None) == []
    assert tesis.normalizar_lista_ids([1, 2]) == [1, 2]
    assert tesis.normalizar_lista_ids({"ids": ["a", "b"]}) == ["a", "b"]
    assert tesis.normalizar_lista_ids({"items": [1]}) == [1]
    assert tesis.normalizar_lista_ids({"otro": []}) == []


def test_config_tesis_defaults_y_overrides(tmp_path):
    cfg = tesis.Config(db_path=tmp_path / "tesis.db", log_dir=tmp_path / "logs")
    assert cfg.log_path == Path(tmp_path / "logs" / "ultimo_update.log")
    assert cfg.history_log_path == Path(tmp_path / "logs" / "update_history.log")

    log_path = tmp_path / "custom.log"
    history_path = tmp_path / "history.log"
    cfg_custom = tesis.Config(
        db_path=tmp_path / "tesis.db",
        log_dir=tmp_path / "logs_custom",
        log_path=log_path,
        history_log_path=history_path,
    )
    assert cfg_custom.log_path == log_path
    assert cfg_custom.history_log_path == history_path
