# scjn-scraper ⚖️

Descarga **tesis/jurisprudencias** y **sentencias (engroses)** de la SCJN (México) desde la API oficial del Sistema de Informática Jurídica y las guarda en bases **SQLite con búsqueda de texto completo (FTS5)**.

Incluye actualización incremental: la primera vez descarga todo, después solo baja lo nuevo.

## Requisitos

- Python 3.9 o superior
- Conexión a internet (solo la primera vez pesado, después son megas)

## Instalación

```bash
pip install -r requirements.txt
```

Mínimas dependencias: `requests`. Opcionalmente `python-docx` para extraer texto de los .docx de sentencias.

## Scripts

### `scjn_tesis_downloader.py` — Tesis y jurisprudencias

Descarga los ~311,000 criterios del Semanario Judicial de la Federación.

```bash
# Descarga inicial o actualización
python scjn_tesis_downloader.py

# Ruta personalizada
python scjn_tesis_downloader.py --db ./datos/tesis.db
```

La primera vez tarda **4-6 horas**. Después, **minutos**.

| Argumento | Default | Qué hace |
|---|---|---|
| `--db` | `./scjn_tesis.db` | Ruta de la BD |
| `--log-dir` | `./logs` | Carpeta para logs |
| `--pausa` | `0.3` seg | Espera entre requests |
| `--max-paginas-sin-nuevos` | `25` | En incremental, tras N páginas sin IDs nuevos se detiene |

### `scjn_sentencias_downloader.py` — Sentencias (engroses)

Descarga las ~105,000 sentencias ejecutivas de la SCJN. Opcionalmente descarga el .docx de cada engrose y extrae su texto plano.

```bash
# Prueba sin descargar nada
python scjn_sentencias_downloader.py --dry-run

# Descarga completa con .docx (4-8 h la primera vez)
python scjn_sentencias_downloader.py

# Solo metadatos, sin .docx (mucho más rápido)
python scjn_sentencias_downloader.py --sin-docx

# Solo sentencias de 2020 en adelante
python scjn_sentencias_downloader.py --anio-desde 2020

# Prueba rápida (solo 500)
python scjn_sentencias_downloader.py --limite 500
```

| Argumento | Default | Qué hace |
|---|---|---|
| `--db` | `./scjn_sentencias.db` | Ruta de la BD |
| `--log-dir` | `./logs` | Carpeta para logs |
| `--pausa` | `0.1` seg | Espera entre requests |
| `--max-paginas-sin-nuevos` | `25` | En incremental, tras N páginas sin IDs nuevos se detiene |
| `--sin-docx` | `False` | No descargar .docx ni extraer texto |
| `--anio-desde` | — | Solo sentencias desde X año |
| `--limite` | — | Detenerse tras N sentencias (pruebas) |
| `--dry-run` | `False` | No descargar nada, solo mostrar qué haría |

## Bases de datos generadas

### `scjn_tesis.db`

- **Tabla `tesis`**: rubro, texto, epoca, instancia, materia, precedentes, etc.
- **Índice FTS5 `tesis_fts`**: búsqueda sobre rubro, texto, precedentes y materias

### `scjn_sentencias.db`

- **Tabla `sentencias`**: expediente, pertenencia, ministro ponente, tema, resolucion, votacion, url_docx, texto_completo, etc.
- **Índice FTS5 `sentencias_fts`**: búsqueda sobre tema, resolucion, texto_completo y expediente

Ambos índices soportan búsqueda **sin acentos** (`constitucion` encuentra `constitución`) y se sincronizan automáticamente via triggers.

### Consultas de ejemplo

```sql
-- Buscar tesis
SELECT rubro, epoca FROM tesis_fts WHERE tesis_fts MATCH 'amparo directo';

-- Buscar sentencias
SELECT expediente, tema FROM sentencias_fts WHERE sentencias_fts MATCH 'derecho a la salud';

-- Contar por pertenencia (Pleno/Salas)
SELECT pertenencia, COUNT(*) FROM sentencias GROUP BY pertenencia;
```

## Estructura del proyecto

```
scjn-scraper/
├── scjn_tesis_downloader.py        # Descarga tesis/jurisprudencias
├── scjn_sentencias_downloader.py   # Descarga sentencias (engroses)
├── requirements.txt                # Dependencias
├── README.md                       # Este archivo
└── .gitignore                      # Archivos que git ignora
```

## Motivacion
Para utilizar herramientas de inteligencia artificial dentro de la profesion juridica es indispensable la verificacion de fuentes, ademas, las LLM estan principalmente entrenadas dentro del sistema juridico anglosajon, esta herramienta pretende democratizar la trazabilidad, investigacion, o demas integraciones que el abogado, estudiante o ciudadano quiera hacer, para que el uso de LLM en el derecho mexicano se convierta en un uso mas responsable.
Sientase libre de utilizar la herramienta como mas le parezca, se agradece profundamente cualquier aportacion a este proyecto.
Dentro del ejercicio de la profesion, desgraciadamente la calidad y rapidez de la investigacion juridica se ve condicionado por el capital de firmas de abogados con capacidad, lo que crea una barrera de entrada y de competencia mucho mayor para los miles de abogados independientes en mexico, asi como los millones de ciudadanos que no pueden gozar de un acceso material a la justicia, por este motivo, considero que el conocimiento tecnico y el avance no debe de elitizarse o cerrarse, sobre todo en un pais donde el acceso a la justicia es limitado, y el campo laboral del abogado independiente lo es tambien.
Personalmente recomiendo utilizar la base de datos generada como fuente utilizando cualquier CLI (Claude Code, OpenCode, Codex, etc).

## Notas tecnicas

- API oficial: `https://bicentenario.scjn.gob.mx/repositorio-scjn`
  - Tesis: `/api/v1/tesis/*`
  - Sentencias: `/api/v1/engroses/*`
- Limite estimado: se respetuoso, no abuses con pausas muy cortas
- Ambos scripts son **idempotentes**: puedes correrlos mil veces sin danar la BD

## Datos

Las **tesis y jurisprudencias** contenidas en las bases de datos generadas son propiedad de la **Suprema Corte de Justicia de la Nacion** y forman parte del Semanario Judicial de la Federacion. Las **sentencias (engroses)** son tambien propiedad de la SCJN, publicadas a traves de su Sistema de Informatica Juridica. Este proyecto unicamente provee las herramientas para descargar y consultar dichos datos; no reclama derechos de propiedad sobre ellos.

## Licencia

**Codigo:** MIT — haz lo que quieras con el codigo.

**Datos:** propiedad de la Suprema Corte de Justicia de la Nacion. Consulta los terminos de uso en `https://www.scjn.gob.mx/terminos-condiciones`.
