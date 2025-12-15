from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
import io
import logging
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

import configparser
import requests
from dotenv import dotenv_values
import xml.etree.ElementTree as ET
from lxml import etree
from pymarc import Record, Field, Subfield, record_to_xml
from pymarc.marcxml import parse_xml_to_array
from urllib.parse import quote

from almapiwrapper.inventory import Item, IzBib, Holding

# =============================================================================
# CONSTANTS PAR DÉFAUT
# =============================================================================

CHECK_XSD = True
INSTITUTION_CODE = "HPH"
ENV = "S"

LOGGER_NAME = "Get records from Infoscience"
INFO_LOG = "create_records.log"
ERROR_LOG = "errors.log"
REPORT_CSV = f"rapport_{date.today().isoformat()}.csv"
REPORT_DIR = "repports"

HOLDING_INFO_DEFAULT = {
    "locations": ["E02XA", "E02SP"],
    "library_code": "hph_bjnbecip",
    "call_number_prefix": "ZTK",
}

ITEM_INFO_DEFAULT = {
    "po_line": None,
    "department_code": "AcqDepthph_bjnbecip",
    "material_type_code": "THESIS",
}


# =============================================================================
# LOGGER CONFIGURATION
# =============================================================================


class NoTracebackFormatter(logging.Formatter):
    """Formatter qui masque la traceback même si exc_info est présent."""

    def format(self, record: logging.LogRecord) -> str:
        exc_info_bak, exc_text_bak = record.exc_info, getattr(record, "exc_text", None)
        try:
            record.exc_info = None
            if hasattr(record, "exc_text"):
                record.exc_text = None
            return super().format(record)
        finally:
            record.exc_info = exc_info_bak
            if exc_text_bak is not None:
                record.exc_text = exc_text_bak


def get_logger(console_level: int = logging.INFO) -> logging.Logger:
    """Configure et retourne un logger sans duplication de handlers."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    # Console (stderr)
    console = logging.StreamHandler(stream=sys.stderr)
    console.setLevel(console_level)
    console.setFormatter(NoTracebackFormatter("%(levelname)s - %(message)s"))

    # Fichier global (INFO+)
    info_fh = logging.FileHandler(INFO_LOG, encoding="utf-8")
    info_fh.setLevel(logging.INFO)
    info_fh.setFormatter(
        NoTracebackFormatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    )

    # Fichier erreurs (ERROR+, avec traceback)
    error_fh = logging.FileHandler(ERROR_LOG, encoding="utf-8", delay=True)
    error_fh.setLevel(logging.ERROR)
    error_fh.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    )

    logger.addHandler(console)
    logger.addHandler(info_fh)
    logger.addHandler(error_fh)

    return logger


def handle_error(
    message: str,
    *,
    with_traceback: bool = True,
    stop: bool = True,
    exit_code: int = 1,
    exc: Optional[BaseException] = None,
) -> None:
    """
    Loggue une erreur ; la traceback ne sera écrite que dans erreurs.log.
    """
    logger = logging.getLogger(LOGGER_NAME)
    if with_traceback:
        logger.error(message, exc_info=(exc if exc is not None else True))
    else:
        logger.error(message)

    if stop:
        raise SystemExit(exit_code)


class SuppressIzBibNoHolding(logging.Filter):
    """Filtre pour ignorer les logs 'IzBib(...): no holding found'."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = str(record.getMessage())
        if "IzBib(" in msg and "no holding found" in msg:
            return False
        return True


# Appliquer le filtre sur le root logger dès l'import du module
root_logger = logging.getLogger()
root_logger.addFilter(SuppressIzBibNoHolding())


# =============================================================================
# MARC PROCESSING
# =============================================================================


def fget(r: Record, tag: str, code: str) -> Optional[str]:
    """Retourne la première valeur du sous-champ `code` pour le champ `tag`, ou None."""
    f = r.get(tag)
    return f.get(code) if (f and code in f) else None


def fget_all(r: Record, tag: str, code: str) -> list[str]:
    """Retourne toutes les valeurs du sous-champ `code` pour le champ `tag`."""
    vals: list[str] = []
    for f in r.get_fields(tag):
        vals.extend(f.get_subfields(code))
    return vals


def first(it: Iterable[str]) -> Optional[str]:
    """Renvoie le premier élément d’un itérable ou None si vide."""
    for x in it:
        return x
    return None


def expand_epfl(v: Optional[str]) -> Optional[str]:
    """Convertit 'EPFL' → 'Ecole Polytechnique Fédérale de Lausanne'."""
    if not v:
        return v
    return "Ecole Polytechnique Fédérale de Lausanne" if v.strip().upper() == "EPFL" else v


def invert_name_comma(name: Optional[str]) -> Optional[str]:
    """Inverse 'Nom, Prénom(s)' → 'Prénom(s) Nom'."""
    if not name:
        return name
    parts = [p.strip() for p in name.split(",")]
    if len(parts) >= 2:
        last = parts[0]
        firsts = " ".join(parts[1:]).strip()
        return f"{firsts} {last}".strip()
    return name


def ensure_pages_suffix(val: Optional[str]) -> Optional[str]:
    """Ajoute ' pages' à la valeur si ce n’est pas déjà présent."""
    if not val:
        return val
    if "pages" in val.lower():
        return val
    return f"{val.strip()} pages"


def build_final_record(src: Record) -> Record:
    """
    Construit une notice MARC finale (EPFL) à partir d'une notice source Infoscience.
    """
    dst = Record(force_utf8=True)

    # 001 : Identifiant Infoscience
    cf001 = src["001"].data if src["001"] is not None else None
    if cf001:
        dst.add_field(Field(tag="001", data=cf001))

    # LDR / 008 : Champs de contrôle (statiques)
    dst.leader = "00000nam a2200000 c 4500"
    dst.add_field(Field(tag="008", data="||||||s2025    sz   a   m    00| | eng  "))

    # 040 : Agence de catalogage (statiques)
    dst.add_field(
        Field(
            tag="040",
            indicators=[" ", " "],
            subfields=[
                Subfield("a", "CH-ZuSLS EPFL"),
                Subfield("b", "fre"),
                Subfield("e", "rda"),
            ],
        )
    )

    # 100 1_ : Auteur principal
    author700a = first(f.get("a") for f in src.get_fields("700") if "a" in f)
    f100_subs = [Subfield("4", "aut")]
    if author700a:
        f100_subs.insert(0, Subfield("a", author700a))
    dst.add_field(Field(tag="100", indicators=["1", " "], subfields=f100_subs))

    # 245 10 : Titre principal
    t_a = fget(src, "245", "a")
    t_b = fget(src, "245", "b")
    t_c = invert_name_comma(author700a)
    subf_245: list[Subfield] = []
    if t_a:
        subf_245.append(Subfield("a", t_a))
    if t_b:
        subf_245.append(Subfield("b", t_b))
    if t_c:
        subf_245.append(Subfield("c", t_c))
    if subf_245:
        dst.add_field(Field(tag="245", indicators=["1", "0"], subfields=subf_245))

    # 264 _1 : Mention d’édition
    place_260a = fget(src, "260", "a")
    publ_260b = expand_epfl(fget(src, "260", "b"))
    date_260c = fget(src, "260", "c")
    subf_264: list[Subfield] = []
    if place_260a:
        subf_264.append(Subfield("a", place_260a))
    if publ_260b:
        subf_264.append(Subfield("b", publ_260b))
    if date_260c:
        subf_264.append(Subfield("c", date_260c))
    if subf_264:
        dst.add_field(Field(tag="264", indicators=[" ", "1"], subfields=subf_264))

    # 300 __ : Description matérielle
    pages_300a = ensure_pages_suffix(fget(src, "300", "a"))
    subf_300: list[Subfield] = []
    if pages_300a:
        subf_300.append(Subfield("a", pages_300a))
    subf_300.extend(
        [
            Subfield("b", "illustrations"),
            Subfield("c", "28 cm"),
        ]
    )
    dst.add_field(Field(tag="300", indicators=[" ", " "], subfields=subf_300))

    # 336 / 337 / 338 : RDA - statiques
    dst.add_field(
        Field(
            tag="336",
            indicators=[" ", " "],
            subfields=[Subfield("b", "txt"), Subfield("2", "rdacontent")],
        )
    )
    dst.add_field(
        Field(
            tag="337",
            indicators=[" ", " "],
            subfields=[Subfield("b", "n"), Subfield("2", "rdamedia")],
        )
    )
    dst.add_field(
        Field(
            tag="338",
            indicators=[" ", " "],
            subfields=[Subfield("b", "nc"), Subfield("2", "rdacarrier")],
        )
    )

    # 502 __ : Note de thèse (unique)
    subf_502: list[Subfield] = []

    b_336a = fget(src, "336", "a")
    if b_336a:
        subf_502.append(
            Subfield("b", "Thèse" if b_336a.strip().lower() == "theses" else b_336a)
        )

    a260a = fget(src, "260", "a")
    a260b = fget(src, "260", "b")
    if a260a or a260b:
        subf_502.append(Subfield("c", f"{a260b or ''} {a260a or ''}".strip()))

    d_920b = fget(src, "920", "b")
    if d_920b:
        subf_502.append(Subfield("d", d_920b))

    a088a = fget(src, "088", "a")
    if a088a:
        subf_502.append(Subfield("o", f"n° {a088a}"))

    if subf_502:
        dst.add_field(Field(tag="502", indicators=[" ", " "], subfields=subf_502))

    # 520 __ : Résumé (unique)
    keywords = [k.strip() for k in fget_all(src, "653", "a") if k and k.strip()]
    seen: set[str] = set()
    deduped: list[str] = []
    for k in keywords:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    subf_520: list[Subfield] = [Subfield("5", "CH-ZuSLS EPFL")]
    if deduped:
        joined = "; ".join(deduped).replace("||", "; ")
        subf_520.insert(0, Subfield("a", f"Mots-clés de l'auteur : {joined}"))
    dst.add_field(Field(tag="520", indicators=[" ", " "], subfields=subf_520))

    # 655 _7 : Type de document (statique)
    dst.add_field(
        Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=[
                Subfield("a", "Thèses et écrits académiques"),
                Subfield("0", "(IDREF)027253139"),
                Subfield("2", "idref"),
            ],
        )
    )
    dst.add_field(
        Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=[
                Subfield("a", "Hochschulschrift"),
                Subfield("2", "gnd-content"),
            ],
        )
    )
    dst.add_field(
        Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=[
                Subfield("a", "Tesi"),
                Subfield("2", "sbt12-content"),
            ],
        )
    )

    # 700 1_ : Directeur (720$a, priorité ind2='2')
    cand_720 = None
    for f in src.get_fields("720"):
        if f.indicators[1] == "2" and "a" in f:
            cand_720 = f
            break
    if not cand_720:
        cand_720 = next((f for f in src.get_fields("720") if "a" in f), None)
    if cand_720:
        dst.add_field(
            Field(
                tag="700",
                indicators=["1", " "],
                subfields=[Subfield("a", cand_720["a"]), Subfield("4", "dgs")],
            )
        )
    else:
        dst.add_field(
            Field(
                tag="700",
                indicators=["1", " "],
                subfields=[Subfield("4", "dgs")],
            )
        )

    return dst


def extract_marc_info(record: Record) -> Dict[str, Optional[str]]:
    """
    Extrait les informations principales d'un objet pymarc.record.Record.
    """
    infoscience_id: Optional[str] = None
    title: Optional[str] = None
    responsibility: Optional[str] = None
    author: Optional[str] = None

    if record["001"]:
        infoscience_id = record["001"].data

    # 245 : titre + mention de responsabilité
    title_fields = record.get_fields("245")
    if title_fields:
        f245 = title_fields[0]
        title_parts = [f245.get("a"), f245.get("b")]
        raw_title = " ".join(p for p in title_parts if p)
        title = raw_title.strip(" /") if raw_title else None
        responsibility = f245.get("c")

    # 100 : auteur principal
    f100s = record.get_fields("100")
    if f100s:
        author = f100s[0].get("a")

    return {
        "infoscience_id": infoscience_id,
        "title": title,
        "author": author,
        "responsibility": responsibility,
    }


# =============================================================================
# XML / XSD VALIDATION
# =============================================================================


def load_xml_schema(xsd_path: str) -> Optional[etree.XMLSchema]:
    """
    Charge un fichier XSD et retourne un objet XMLSchema.
    Retourne None en cas d'erreur (fichier manquant ou XSD invalide).
    """
    try:
        xsd_file = Path(xsd_path)
        if not xsd_file.exists():
            print(f"⚠️ XSD non trouvé : {xsd_path}")
            return None

        schema_doc = etree.parse(str(xsd_file))
        return etree.XMLSchema(schema_doc)

    except Exception as e:
        print(f"⚠️ Erreur lors du chargement XSD '{xsd_path}' : {e}")
        return None


def validate_xml_element(
    element: Optional[etree._Element],
    schema: Optional[etree.XMLSchema] = None,
    *,
    element_label: str = "element",
    required: bool = True,
) -> Tuple[bool, List[str]]:
    """
    Validation XSD générique d'un élément XML.
    """
    if element is None:
        if required:
            return False, [f"No <{element_label}> element found"]
        return True, []

    if schema is None:
        return True, []

    if not schema.validate(element):
        errors = [
            f"{err.message} (line {err.line}, col {err.column})"
            for err in schema.error_log
        ]
        return False, errors

    return True, []


def validate_bib_and_record(
    bib_element: etree._Element,
    marc_schema: Optional[etree.XMLSchema] = None,
    bib_schema: Optional[etree.XMLSchema] = None,
) -> Dict[str, Any]:
    """
    Valide :
      - le <record> MARC interne avec un schéma MARC21 (marc_schema)
      - le <bib> Alma avec un schéma Alma (bib_schema)
    """
    ns_marc = {"marc": "http://www.loc.gov/MARC21/slim"}
    rec_element = bib_element.find(".//marc:record", namespaces=ns_marc)

    record_valid, record_errors = validate_xml_element(
        rec_element,
        marc_schema,
        element_label="record",
        required=True,
    )

    bib_valid, bib_errors = validate_xml_element(
        bib_element,
        bib_schema,
        element_label="bib",
        required=True,
    )

    return {
        "record_valid": record_valid,
        "record_errors": record_errors,
        "bib_valid": bib_valid,
        "bib_errors": bib_errors,
    }


def validate_holding_xml(
    holding_el: etree._Element,
    holding_schema: Optional[etree.XMLSchema] = None,
) -> Tuple[bool, List[str]]:
    """Valide un élément <holding> Alma avec un schéma XSD."""
    return validate_xml_element(
        holding_el,
        holding_schema,
        element_label="holding",
        required=True,
    )


def validate_item_xml(
    item_el: etree._Element,
    item_schema: Optional[etree.XMLSchema] = None,
) -> Tuple[bool, List[str]]:
    """Valide un élément <item> Alma avec le schéma XSD fourni."""
    return validate_xml_element(
        item_el,
        item_schema,
        element_label="item",
        required=True,
    )


# =============================================================================
# INFOSCIENCE & ANALYTICS HELPERS
# =============================================================================


def get_last_call_number_from_analytics() -> Tuple[Optional[int], Optional[str]]:
    """
    Appelle l'API Analytics et retourne (valeur, erreur).
    """
    try:
        config = dotenv_values(".env")
        base_url = config.get("ALMA_API_URL")
        analytics_path = config.get("ALMA_API_ANALYTICS_PATH")
        api_key = config.get("ALMA_API_KEY")

        if not base_url or not analytics_path or not api_key:
            error = "Variables manquantes dans .env"
            return None, error

        api_url = f"{base_url}{analytics_path}"

        headers = {"Authorization": f"apikey {api_key}"}
        resp = requests.get(api_url, headers=headers, timeout=30)

        if resp.status_code != 200:
            error = f"Erreur API: HTTP {resp.status_code}"
            return None, error

        root = ET.fromstring(resp.text)
        ns = {"ns": "urn:schemas-microsoft-com:xml-analysis:rowset"}
        first_row = root.find(".//ns:Row", ns)
        if first_row is None:
            error = "Aucune Row dans la réponse."
            return None, error

        col3 = first_row.find("ns:Column3", ns)

        if col3 is None or not col3.text or not col3.text.strip().isdigit():
            error = "Column3 introuvable ou non numérique."
            return None, error

        value = int(col3.text.strip())
        return value, None

    except Exception as e:  # réseau, parsing, etc.
        error = f"Erreur: {e}"
        return None, error


def safe(v: Any) -> str:
    """Représentation sûre pour les logs."""
    return v if isinstance(v, (str, int, float)) else repr(v)


def first_day_previous_month(ref: Optional[date] = None) -> str:
    """
    Renvoie la chaîne 'YYYY-MM-01' correspondant au premier jour du mois précédent.
    """
    if ref is None:
        ref = date.today()
    year, month = ref.year, ref.month
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    return f"{year:04d}-{month:02d}-01"


def build_infoscience_url(
    spc_page: int = 1,
    spc_rpp: int = 100,
    ref: Optional[date] = None,
    of_format: str = "xm",
) -> str:
    """
    Construit l'URL Infoscience 'discover/export' pour UNE page (spc.page),
    avec spc.rpp résultats par page.

    - spc_page : numéro de page (1, 2, 3, ...)
    - spc_rpp  : nombre de résultats par page (ex. 100)
    - ref      : date de référence pour la fenêtre temporelle
    - of_format: format de sortie (xm, xmJ, etc.)
    """
    base = "https://infoscience.epfl.ch/server/api/discover/export"
    configuration = "researchoutputs"
    f_types = "thesis-coar-types:c_db06,authority"

    start = first_day_previous_month(ref)
    query_value = f"dc.publisher:EPFL dc.date.created:[{start} TO *]"
    query_enc = quote(query_value, safe="")

    params = (
        f"configuration={configuration}"
        f"&spc.page={spc_page}"
        f"&spc.rpp={spc_rpp}"
        f"&f.types={quote(f_types, safe='')}"
        f"&query={query_enc}"
        f"&spc.sf=dc.date.accessioned"
        f"&spc.sd=DESC"
        f"&of={of_format}"
    )

    return f"{base}?{params}"


def iter_infoscience_records(
    *,
    use_static_url: bool,
    start_spc_page: int,
    spc_rpp: int,
    logger: logging.Logger,
    ref: Optional[date] = None,
    of_format: str = "xm",
) -> Iterable[Record]:
    """
    Itère sur toutes les notices MARCXML renvoyées par Infoscience
    en gérant la pagination via spc.page.

    - use_static_url=True  : un seul appel à l'URL statique.
    - use_static_url=False : boucle sur spc.page = start_spc_page, start_spc_page+1, ...
                             jusqu'à ce qu'une page ne renvoie plus de notice.
    """
    if use_static_url:
        # Cas test : une URL figée, une seule "page"
        url = (
            "https://infoscience.epfl.ch/server/api/discover/export?"
            "spc.page=1&"
            "query=Applications%20of%20Data-driven%20Predictive%20Control%20to%20Building%20Energy%20Systems&"
            "configuration=researchoutputs&"
            "scope=4af344ef-0fb2-4593-a234-78d57f3df621&f.types=thesis-coar-types:c_db06,authority&"
            "f.dateIssued.min=2025&f.author_editor=koch,%20manuel%20pascal,equals&of=xm"
        )
        logger.info("Téléchargement Infoscience (URL statique) : %s", url)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        records = parse_xml_to_array(io.BytesIO(resp.content))
        logger.info("%d notice(s) récupérée(s) depuis l'URL statique.", len(records))
        for r in records:
            yield r
        return

    # Cas URL dynamique : pagination via spc.page
    spc_page = start_spc_page
    total = 0

    while True:
        url = build_infoscience_url(
            spc_page=spc_page,
            spc_rpp=spc_rpp,
            ref=ref,
            of_format=of_format,
        )
        logger.info("Téléchargement Infoscience spc.page=%d : %s", spc_page, url)

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Erreur HTTP sur spc.page=%d : %s", spc_page, e)
            break

        try:
            records = parse_xml_to_array(io.BytesIO(resp.content))
        except Exception as e:
            logger.error("Erreur de parsing MARCXML sur spc.page=%d : %s", spc_page, e)
            break

        if not records:
            logger.info(
                "Aucune notice sur spc.page=%d -> fin de la pagination Infoscience.",
                spc_page,
            )
            break

        logger.info("%d notice(s) récupérée(s) sur spc.page=%d.", len(records), spc_page)
        total += len(records)
        for r in records:
            yield r

        spc_page += 1  # page suivante

    logger.info("Total de %d notices récupérées au total depuis Infoscience.", total)


# =============================================================================
# SRU CHECK (SWISSCOVERY)
# =============================================================================


def fetch_marc_record_from_sru(
    title: str,
    author: str,
    institution_code: str = "NETWORK",
) -> bool:
    """
    Vérifie si une notice existe déjà dans swisscovery via SRU.
    """
    base_url = f"https://swisscovery.ch/view/sru/41SLSP_{institution_code}"
    params = {
        "version": "1.2",
        "operation": "searchRetrieve",
        "query": f'title="{title}" AND creator="{author}"',
        "maximumRecords": 1,
    }
    response = requests.get(base_url, params=params, timeout=30)
    if response.status_code != 200:
        print(f"HTTP error: {response.status_code}")
        return False

    root = etree.fromstring(response.content)
    ns = {
        "srw": "http://www.loc.gov/zing/srw/",
        "marc": "http://www.loc.gov/MARC21/slim",
    }

    record_data = root.find(".//srw:recordData", ns)
    if record_data is None:
        return False

    marc_record = record_data.find("marc:record", ns)
    if marc_record is None:
        print("No MARC record found.")
        return False

    def get_subfield(tag: str, code: str) -> Optional[str]:
        for field in marc_record.findall(f"marc:datafield[@tag='{tag}']", ns):
            for subfield in field.findall("marc:subfield", ns):
                if subfield.get("code") == code:
                    return subfield.text
        return None

    title_field = get_subfield("245", "a")
    author_field = get_subfield("100", "a") or get_subfield("700", "a")
    publisher_field = get_subfield("260", "b") or get_subfield("264", "b")
    year_field = get_subfield("260", "c") or get_subfield("264", "c")

    print("--- MARC Record already exist into Alma (swisscovery) ---")
    print(f"Title: {title_field}")
    print(f"Author: {author_field}")
    print(f"Publisher: {publisher_field}")
    print(f"Year: {year_field}")
    print("--- No need to create again ---")

    return True


# =============================================================================
# ALMA — BIB
# =============================================================================


def build_bib_with_record(src_record: Record) -> Tuple[etree._Element, etree._Element]:
    """
    Construit l'élément <bib> Alma à partir d'une notice source Infoscience.
    """
    final_record = build_final_record(src_record)

    marcxml_bytes = record_to_xml(final_record, namespace=True)
    rec_el = etree.fromstring(marcxml_bytes)

    bib_el = etree.Element("bib")
    bib_el.append(rec_el)

    return rec_el, bib_el


# =============================================================================
# ALMA — HOLDINGS
# =============================================================================


def delete_holding(
    bib_obj: IzBib,
    holding_id: str,
    zone: str = "HPH",
    env: str = "S",
    force: bool = False,
) -> Tuple[bool, Optional[str]]:
    """
    Supprime une holding Alma à partir de son holding_id.
    """
    mms_id = bib_obj.get_mms_id()
    if not mms_id:
        return False, "MMS ID introuvable."

    holding = Holding(
        mms_id=mms_id,
        holding_id=holding_id,
        zone=zone,
        env=env,
        bib=bib_obj,
    )

    holding.delete(force=force)

    if holding.error:
        return False, holding.error_msg

    if hasattr(bib_obj, "_holdings"):
        bib_obj._holdings = None

    return True, None


def find_existing_holding(
    bib_obj: IzBib,
    library_code: str,
    location: str,
) -> Optional[Holding]:
    """
    Recherche une holding existante dans Alma pour une combinaison
    (library_code, location).
    """
    if hasattr(bib_obj, "_holdings"):
        bib_obj._holdings = None

    try:
        holdings = bib_obj.get_holdings()
    except Exception as e:
        print(f"⚠️ Impossible de récupérer les holdings : {e}")
        return None

    if not holdings:
        return None

    for h in holdings:
        if h.error:
            print(f"⚠️ Holding ignorée (erreur wrapper) : {h.error_msg}")
            continue

        lib = h.library
        loc = h.location

        if not lib or not loc:
            print(
                f"⚠️ Holding ignorée : library/location manquants (lib={lib}, loc={loc})"
            )
            continue

        if lib == library_code and loc == location:
            return h

    return None


def build_holding_marc(
    library_code: str,
    location: str,
    call_number: str,
) -> Record:
    """
    Construit un record MARC interne pour une holding Alma.
    """
    record = Record(force_utf8=True)
    record.leader = "00000nx a2200061zn 450"

    record.add_field(Field(tag="008", data="1011252u 8 4001uueng0000000"))
    record.add_field(
        Field(
            tag="852",
            indicators=["4", " "],
            subfields=[
                Subfield("b", str(library_code)),
                Subfield("c", str(location)),
                Subfield("j", str(call_number)),
            ],
        )
    )
    return record


def build_holding_xml(record: Record) -> etree._Element:
    """
    Convertit un record MARC (pymarc.Record) en XML Alma <holding>.
    """
    marcxml_bytes = record_to_xml(record, namespace=True)
    record_el = etree.fromstring(marcxml_bytes)

    holding_el = etree.Element("holding")
    etree.SubElement(holding_el, "holding_id")
    holding_el.append(record_el)

    return holding_el


def create_holding_in_alma(
    bib_obj: IzBib,
    holding_el: etree._Element,
    zone: str = "HPH",
    env: str = "S",
) -> Tuple[Optional[Holding], Optional[str]]:
    """
    Crée une holding dans Alma via almapiwrapper.
    """
    mms_id = bib_obj.get_mms_id()
    if not mms_id:
        return None, "MMS ID introuvable."

    holding = Holding(
        data=holding_el,
        mms_id=mms_id,
        zone=zone,
        env=env,
        create_holding=True,
        bib=bib_obj,
    )

    if holding.error:
        return None, holding.error_msg

    return holding, None


def creer_holding(
    bib_obj: IzBib,
    library_code: str,
    location: str,
    call_number: str,
    holding_schema: Optional[etree.XMLSchema] = None,
    zone: str = "HPH",
    env: str = "S",
    logger: Optional[logging.Logger] = None,
) -> Optional[Holding]:
    """
    Crée (ou récupère) une holding dans Alma pour (library_code, location).
    Retourne l'objet Holding ou None si erreur.
    """
    log = logger.info if logger else print

    existing = find_existing_holding(bib_obj, library_code, location)
    if existing:
        log(
            "ℹ️ Holding déjà existante (%s/%s) : %s",
            library_code,
            location,
            existing.holding_id,
        )
        return existing

    record = build_holding_marc(library_code, location, call_number)
    holding_el = build_holding_xml(record)

    is_valid, errors = validate_holding_xml(holding_el, holding_schema=holding_schema)
    if not is_valid:
        log("❌ Holding Alma invalide :")
        for e in errors:
            log(" - %s", e)
        return None

    log("Holding valide, on peut l'envoyer à Alma.")

    holding, error = create_holding_in_alma(
        bib_obj,
        holding_el,
        zone=zone,
        env=env,
    )

    if error:
        log("❌ Erreur Alma : %s", error)
        return None

    log("✅ Holding créée avec succès. ID : %s", holding.get_holding_id())
    return holding


# =============================================================================
# ALMA — ITEMS
# =============================================================================


def build_item_xml_for_holding(
    holding: Holding,
    base_status: str,
    po_line: Optional[str] = None,
    arrival_date: Optional[str] = None,
    department_code: Optional[str] = None,
    material_type_code: str = "THESIS",
    item_policy_code: Optional[str] = None,
) -> etree._Element:
    """
    Construit un élément <item> (XML Alma) pour une holding donnée.
    """
    if arrival_date is None:
        arrival_date = date.today().isoformat()

    item_el = etree.Element("item")

    holding_data_el = etree.SubElement(item_el, "holding_data")
    etree.SubElement(holding_data_el, "holding_id").text = holding.holding_id

    item_data_el = etree.SubElement(item_el, "item_data")

    etree.SubElement(item_data_el, "base_status").text = base_status

    if material_type_code:
        etree.SubElement(item_data_el, "physical_material_type").text = material_type_code

    if item_policy_code:
        etree.SubElement(item_data_el, "policy").text = item_policy_code

    if po_line:
        etree.SubElement(item_data_el, "po_line").text = po_line

    etree.SubElement(item_data_el, "arrival_date").text = arrival_date

    if holding.library:
        etree.SubElement(item_data_el, "library").text = holding.library
    if holding.location:
        etree.SubElement(item_data_el, "location").text = holding.location

    if department_code:
        etree.SubElement(item_data_el, "process_type").text = "WORK_ORDER_DEPARTMENT"
        etree.SubElement(item_data_el, "work_order_at").text = department_code

    return item_el


def creer_item_pour_une_holding(
    holding: Holding,
    po_line: Optional[str] = None,
    department_code: Optional[str] = "AcqDepthph_bjnbecip",
    material_type_code: str = "THESIS",
    item_schema: Optional[etree.XMLSchema] = None,
    zone: str = "HPH",
    env: str = "S",
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    """
    Crée un item pour une holding Alma déjà existante.
    """
    log_info = logger.info if logger else print
    log_error = logger.error if logger else print

    loc = holding.location
    lib = holding.library

    if loc == "E02SP":
        base_status = "04"
        item_policy_code = "04"
    elif loc == "E02XA":
        base_status = "70"
        item_policy_code = "70"
    else:
        log_info("⚠️ Localisation '%s' non gérée -> aucun item créé.", loc)
        return None

    item_el = build_item_xml_for_holding(
        holding=holding,
        base_status=base_status,
        po_line=po_line,
        department_code=department_code,
        material_type_code=material_type_code,
        item_policy_code=item_policy_code,
    )

    valid, errors = validate_item_xml(item_el, item_schema=item_schema)
    if not valid:
        log_error("❌ Item invalide (holding %s) :", holding.holding_id)
        for err in errors:
            log_error("   - %s", err)
        return None

    item = Item(
        holding=holding,
        data=item_el,
        create_item=True,
        zone=zone,
        env=env,
    )

    if item.error:
        log_error(
            "❌ Erreur Alma création item pour holding %s (%s/%s) : %s",
            holding.holding_id,
            lib,
            loc,
            item.error_msg,
        )
        return None

    item_id = item.get_item_id()
    log_info(
        "✅ Item créé pour holding %s (%s/%s) – item_id = %s",
        holding.holding_id,
        lib,
        loc,
        item_id,
    )
    return item_id


# =============================================================================
# REPORT MODEL
# =============================================================================


@dataclass
class NoticeReport:
    record_index: int
    infoscience_id: Optional[str]
    title: Optional[str]
    author: Optional[str]
    call_number: Optional[str]

    sru_exists: Optional[bool] = None

    mms_id: Optional[str] = None
    bib_status: Optional[str] = None
    bib_error: Optional[str] = None

    locations: List[Dict[str, Any]] = field(default_factory=list)

    def add_location(
        self,
        location: str,
        holding_id: Optional[str] = None,
        holding_status: Optional[str] = None,
        holding_error: Optional[str] = None,
        item_id: Optional[str] = None,
        item_status: Optional[str] = None,
        item_error: Optional[str] = None,
    ) -> None:
        self.locations.append(
            {
                "location": location,
                "holding_id": holding_id,
                "holding_status": holding_status,
                "holding_error": holding_error,
                "item_id": item_id,
                "item_status": item_status,
                "item_error": item_error,
            }
        )

    def to_csv_row(self) -> Dict[str, str]:
        """
        Retourne UNE seule ligne (dict) pour le CSV,
        en agrégeant holdings/items sur des colonnes concaténées.
        """

        holding_ids: List[str] = []
        holding_locations: List[str] = []
        holding_statuses: List[str] = []
        holding_errors: List[str] = []

        item_ids: List[str] = []
        item_statuses: List[str] = []
        item_errors: List[str] = []

        for loc in self.locations:
            loc_code = loc.get("location") or ""
            h_id = loc.get("holding_id") or ""
            h_status = loc.get("holding_status") or ""
            h_error = loc.get("holding_error") or ""
            i_id = loc.get("item_id") or ""
            i_status = loc.get("item_status") or ""
            i_error = loc.get("item_error") or ""

            if h_id:
                holding_ids.append(h_id)
            if loc_code:
                holding_locations.append(loc_code)
            if h_status:
                holding_statuses.append(f"{loc_code}:{h_status}")
            if h_error:
                holding_errors.append(f"{loc_code}:{h_error}")

            if i_id:
                item_ids.append(i_id)
            if i_status:
                item_statuses.append(f"{loc_code}:{i_status}")
            if i_error:
                item_errors.append(f"{loc_code}:{i_error}")

        def join_or_empty(values: List[str]) -> str:
            return " | ".join(values) if values else ""

        return {
            "record_index": str(self.record_index),
            "infoscience_id": self.infoscience_id or "",
            "title": self.title or "",
            "author": self.author or "",
            "call_number": self.call_number or "",
            "sru_exists": "" if self.sru_exists is None else str(self.sru_exists),
            "mms_id": self.mms_id or "",
            "bib_status": self.bib_status or "",
            "bib_error": self.bib_error or "",
            "holding_locations": join_or_empty(holding_locations),
            "holding_ids": join_or_empty(holding_ids),
            "holding_statuses": join_or_empty(holding_statuses),
            "holding_errors": join_or_empty(holding_errors),
            "item_ids": join_or_empty(item_ids),
            "item_statuses": join_or_empty(item_statuses),
            "item_errors": join_or_empty(item_errors),
        }


# =============================================================================
# CONFIG HOLDING/ITEM VIA FICHIER INI
# =============================================================================

def load_config(
    config_file: Optional[str],
    logger: logging.Logger,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Charge la configuration globale depuis un fichier INI.

    Retourne 5 dicts :
      - general_cfg
      - infoscience_cfg
      - xsd_cfg
      - holding_info
      - item_info
    """

    # Valeurs par défaut (fallback si le INI ne les définit pas)
    general_cfg: Dict[str, Any] = {
        "env": "S",
        "institution_code": "HPH",
        "check_xsd": True,
        "report_prefix": "rapport_",
        "skip_sru_check": False,
    }

    infoscience_cfg: Dict[str, Any] = {
        "spc_rpp": 100,
        "of_format": "xm",
        "since_strategy": "previous_month",
    }

    xsd_cfg: Dict[str, Any] = {
        "marc21": "xsd/MARC21slim.xsd",
        "bib": "xsd/rest_bib.xsd",
        "holding": "xsd/rest_holding.xsd",
        "item": "xsd/rest_item.xsd",
    }

    holding_info: Dict[str, Any] = {
        "library_code": "hph_bjnbecip",
        "locations": ["E02XA", "E02SP"],
        "call_number_prefix": "ZTK",
    }

    item_info: Dict[str, Any] = {
        "po_line": None,
        "department_code": "AcqDepthph_bjnbecip",
        "material_type_code": "THESIS",
    }

    if not config_file:
        logger.info("Aucun fichier de config fourni, utilisation des valeurs par défaut.")
        return general_cfg, infoscience_cfg, xsd_cfg, holding_info, item_info

    cfg_path = Path(config_file)
    if not cfg_path.exists():
        logger.warning(
            "Fichier de configuration %s introuvable, utilisation des valeurs par défaut.",
            cfg_path,
        )
        return general_cfg, infoscience_cfg, xsd_cfg, holding_info, item_info

    parser = configparser.ConfigParser()
    try:
        parser.read(cfg_path, encoding="utf-8")
    except Exception as e:
        logger.warning(
            "Erreur lors de la lecture de %s : %s. Utilisation des valeurs par défaut.",
            cfg_path,
            e,
        )
        return general_cfg, infoscience_cfg, xsd_cfg, holding_info, item_info

    # [general]
    if parser.has_section("general"):
        sec = parser["general"]
        if "env" in sec:
            general_cfg["env"] = sec.get("env", general_cfg["env"])
        if "institution_code" in sec:
            general_cfg["institution_code"] = sec.get(
                "institution_code", general_cfg["institution_code"]
            )
        if "check_xsd" in sec:
            general_cfg["check_xsd"] = sec.getboolean(
                "check_xsd", fallback=general_cfg["check_xsd"]
            )
        if "report_prefix" in sec:
            general_cfg["report_prefix"] = sec.get(
                "report_prefix", general_cfg["report_prefix"]
            )
        if "skip_sru_check" in sec:
            general_cfg["skip_sru_check"] = sec.getboolean(
                "skip_sru_check", fallback=general_cfg["skip_sru_check"]
            )

    # [infoscience]
    if parser.has_section("infoscience"):
        sec = parser["infoscience"]
        if "spc_rpp" in sec:
            infoscience_cfg["spc_rpp"] = sec.getint(
                "spc_rpp", fallback=infoscience_cfg["spc_rpp"]
            )
        if "of_format" in sec:
            infoscience_cfg["of_format"] = sec.get(
                "of_format", infoscience_cfg["of_format"]
            )
        if "since_strategy" in sec:
            infoscience_cfg["since_strategy"] = sec.get(
                "since_strategy", infoscience_cfg["since_strategy"]
            )

    # [xsd]
    if parser.has_section("xsd"):
        sec = parser["xsd"]
        for key in ["marc21", "bib", "holding", "item"]:
            if key in sec:
                xsd_cfg[key] = sec.get(key, xsd_cfg[key])

    # [holding]
    if parser.has_section("holding"):
        sec = parser["holding"]
        if "library_code" in sec:
            holding_info["library_code"] = sec.get(
                "library_code", holding_info["library_code"]
            )
        if "locations" in sec:
            locs_raw = sec.get("locations", "")
            locs = [l.strip() for l in locs_raw.split(",") if l.strip()]
            if locs:
                holding_info["locations"] = locs
        if "call_number_prefix" in sec:
            holding_info["call_number_prefix"] = sec.get(
                "call_number_prefix", holding_info["call_number_prefix"]
            )

    # [item]
    if parser.has_section("item"):
        sec = parser["item"]
        if "po_line" in sec:
            val = sec.get("po_line", "").strip()
            item_info["po_line"] = val or None
        if "department_code" in sec:
            item_info["department_code"] = sec.get(
                "department_code", item_info["department_code"]
            )
        if "material_type_code" in sec:
            item_info["material_type_code"] = sec.get(
                "material_type_code", item_info["material_type_code"]
            )

    logger.info(
        "Configuration chargée depuis %s : general=%s, infoscience=%s, holding=%s, item=%s",
        cfg_path,
        general_cfg,
        infoscience_cfg,
        holding_info,
        item_info,
    )
    return general_cfg, infoscience_cfg, xsd_cfg, holding_info, item_info

# =============================================================================
# MAIN PIPELINE
# =============================================================================


def main(
    dry_run: bool = False,
    use_static_url: bool = False,
    spc_page: int = 1,
    spc_rpp: int = 100,
    env: str = None,
    institution_code: str = None,
    check_xsd: bool = True,
    max_records: int = 0,
    config_file: Optional[str] = None,
    skip_sru_check: bool = False,
) -> None:
    # Logger
    logger = get_logger()
    logger.info(
        "Début du script. %s (dry_run=%s, use_static_url=%s, env=%s, inst=%s, spc_page=%d, "
        "spc_rpp=%d, check_xsd=%s, skip_sru_check=%s, max_records=%d, config_file=%s)",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        dry_run,
        use_static_url,
        env,
        institution_code,
        spc_page,
        spc_rpp,
        check_xsd,
        skip_sru_check,
        max_records,
        config_file,
    )

    # Charger toute la configuration (general / infoscience / xsd / holding / item)
    general_cfg, infoscience_cfg, xsd_cfg, holding_info, item_info = load_config(
        config_file, logger
    )

    # Env, institution, check_xsd : config INI + override par les arguments CLI
    if env is None:
        env = general_cfg["env"]
    if institution_code is None:
        institution_code = general_cfg["institution_code"]
    # check_xsd CLI a déjà été converti en bool dans l'appel à main()
    check_xsd = check_xsd and general_cfg["check_xsd"]
    # skip_sru_check : config INI + override par les arguments CLI
    skip_sru_check = skip_sru_check or general_cfg["skip_sru_check"]

    # spc_rpp : si l’argument CLI est laissé par défaut, tu peux décider qui gagne :
    if spc_rpp <= 0:
        spc_rpp = infoscience_cfg["spc_rpp"]

    of_format = infoscience_cfg["of_format"]

    # 1) Récupère la cote dans Analytics
    current_call_number, error = get_last_call_number_from_analytics()
    if error or current_call_number is None:
        handle_error(
            "Problème lors de la récupération de la Cote dans Analytics --> Fin du script",
            exc=RuntimeError(error or "Erreur inconnue"),
            stop=True,
        )

    # 2) Chargement des schémas XSD si nécessaire
    marc_schema: Optional[etree.XMLSchema] = None
    bib_schema: Optional[etree.XMLSchema] = None
    holding_schema: Optional[etree.XMLSchema] = None
    item_schema: Optional[etree.XMLSchema] = None

    if check_xsd:
        marc_schema = load_xml_schema(xsd_cfg["marc21"])
        bib_schema = load_xml_schema(xsd_cfg["bib"])
        holding_schema = load_xml_schema(xsd_cfg["holding"])
        item_schema = load_xml_schema(xsd_cfg["item"])
        logger.info("Schémas XSD chargés avec succès")

    record_index = 0
    reports: List[NoticeReport] = []

    # 3+4) Récupérer toutes les notices avec pagination spc.page
    call_number_value = int(current_call_number)

    records_iter = iter_infoscience_records(
        use_static_url=use_static_url,
        start_spc_page=spc_page,
        spc_rpp=spc_rpp,
        logger=logger,
        ref=None,
        of_format=of_format,
    )

    # 5) Pour chaque notice (toutes pages confondues)
    for record in records_iter:
        record_index += 1
        if max_records > 0 and record_index > max_records:
            logger.info(
                "max_records=%d atteint, arrêt du traitement des notices.",
                max_records,
            )
            break

        call_number_value += 1
        call_number_str = f"{holding_info['call_number_prefix']} {call_number_value}"

        logger.info("Traitement de la notice n°%d", record_index)

        # Construction du record MARC final (pour extractions & debug)
        try:
            current_record = build_final_record(record)
            logger.info("Construction du record %d réussie", record_index)
        except Exception:
            logger.exception(
                "Erreur lors de la construction du record %d --> Passage au record suivant",
                record_index,
            )
            continue

        # Extraction des infos pour la recherche SRU
        try:
            info_marc_current_record = extract_marc_info(current_record)
            logger.info(
                "Récupération du 'title', 'author', 'responsibility' réussie",
            )
        except Exception:
            logger.exception(
                "Problème lors de la récupération du 'title', 'author', 'responsibility' "
                "du record %d --> La vérification dans SRU ne sera pas possible. Passage au record suivant",
                record_index,
            )
            continue

        logger.info(
            "Recherche SRU title=%s author=%s responsibility=%s",
            safe(info_marc_current_record.get("title")),
            safe(info_marc_current_record.get("author")),
            safe(info_marc_current_record.get("responsibility")),
        )

        notice_report = NoticeReport(
            record_index=record_index,
            infoscience_id=info_marc_current_record.get("infoscience_id"),
            title=info_marc_current_record.get("title"),
            author=info_marc_current_record.get("author"),
            call_number=call_number_str,
        )

        if skip_sru_check:
            logger.info("Vérification SRU ignorée (--skip-sru-check activé).")
            notice_report.sru_exists = None
            exists = False
        else:
            # Vérifie si l'enregistrement existe déjà dans SRU
            try:
                exists = fetch_marc_record_from_sru(
                    info_marc_current_record["title"] or "",
                    info_marc_current_record["author"] or "",
                    institution_code=institution_code,
                )
                notice_report.sru_exists = exists
            except Exception:
                logger.exception(
                    "Erreur lors de la recherche SRU pour le record %d --> Passage au record suivant",
                    record_index,
                )
                continue

        if exists:
            logger.info("Notice déjà présente dans Alma, création ignorée.")
            notice_report.bib_status = "SKIPPED_SRU_EXISTS"
            reports.append(notice_report)
            continue

        logger.info("Pas présent dans SRU, préparation de la notice.")

        # 1) Construire <bib> + <record> à partir de la notice source
        try:
            rec_el, bib_el = build_bib_with_record(record)
            logger.info("Élément <bib> construit, lancement éventuel de la validation XSD.")
        except Exception:
            logger.exception(
                "Problème lors de la préparation de la notice pour le record %d --> Passage au record suivant",
                record_index,
            )
            continue

        # 2) Validation XSD Bib + Record
        schema_bib_and_record_valid = True
        if check_xsd:
            logger.info(
                "Validation XSD Bib + Record activée pour le record %d.",
                record_index,
            )

            try:
                result = validate_bib_and_record(
                    bib_element=bib_el,
                    marc_schema=marc_schema,
                    bib_schema=bib_schema,
                )
            except Exception:
                logger.exception(
                    "Problème avec la fonction validate_bib_and_record pour le record %d.",
                    record_index,
                )
                schema_bib_and_record_valid = False
            else:
                record_valid = result["record_valid"]
                bib_valid = result["bib_valid"]

                if not record_valid:
                    logger.error("Validation MARC record KO pour le record %d", record_index)
                    for err in result["record_errors"]:
                        logger.error("  - %s", err)

                if not bib_valid:
                    logger.error("Validation Bib KO pour le record %d", record_index)
                    for err in result["bib_errors"]:
                        logger.error("  - %s", err)

                schema_bib_and_record_valid = record_valid and bib_valid

                if schema_bib_and_record_valid:
                    logger.info(
                        "Vérification des schémas Bib et Record correcte pour le record %d.",
                        record_index,
                    )
                else:
                    logger.error(
                        "Problème de validation des schémas Bib et/ou Record pour le record %d.",
                        record_index,
                    )
        else:
            logger.info("Validation XSD désactivée (check_xsd = False).")

        if not schema_bib_and_record_valid:
            logger.error(
                "Schémas Bib/Record non valides pour le record %d --> Passage au record suivant",
                record_index,
            )
            notice_report.bib_status = "XSD_ERROR"
            notice_report.bib_error = "Bib/Record XSD validation failed (see logs)"
            reports.append(notice_report)
            continue

        # DRY-RUN : on ne crée rien dans Alma, mais on note que tout est OK jusqu'ici
        if dry_run:
            logger.info(
                "DRY-RUN: BIB/holdings/items NON créés pour le record %d (tout est valide jusqu'ici).",
                record_index,
            )
            notice_report.bib_status = "DRY_RUN_OK"
            notice_report.bib_error = ""
            reports.append(notice_report)
            continue

        # 3) Création de la notice Bib dans Alma
        logger.info("Les schémas Bib et Record sont corrects. --> Création de la notice Bib.")
        try:
            bib_obj = IzBib(
                data=bib_el,
                zone=institution_code,
                env=env,
                create_bib=True,
            )
            logger.info("✅ Notice créée avec succès. MMS ID : %s", bib_obj.get_mms_id())
            notice_report.mms_id = str(bib_obj.get_mms_id())
            notice_report.bib_status = "CREATED"
        except Exception as e:
            handle_error(
                "❌ Erreur lors de la création de la notice Bib",
                exc=e,
                stop=False,
            )
            notice_report.bib_status = "ERROR"
            notice_report.bib_error = str(e)
            reports.append(notice_report)
            continue

        # 4) Création des holdings + items
        for loc in holding_info["locations"]:
            try:
                holding = creer_holding(
                    bib_obj=bib_obj,
                    library_code=holding_info["library_code"],
                    location=loc,
                    call_number=call_number_str,
                    holding_schema=holding_schema,
                    zone=institution_code,
                    env=env,
                    logger=logger,
                )

                if holding:
                    logger.info(
                        "Holding créée ou récupérée pour location %s, ID %s",
                        loc,
                        holding.get_holding_id(),
                    )

                    item_id = creer_item_pour_une_holding(
                        holding=holding,
                        po_line=item_info["po_line"],
                        department_code=item_info["department_code"],
                        material_type_code=item_info["material_type_code"],
                        item_schema=item_schema,
                        zone=institution_code,
                        env=env,
                        logger=logger,
                    )

                    if item_id:
                        logger.info(
                            "Item créé pour location %s, item_id=%s",
                            loc,
                            item_id,
                        )
                        notice_report.add_location(
                            location=loc,
                            holding_id=str(holding.get_holding_id()),
                            holding_status="CREATED",
                            item_id=str(item_id),
                            item_status="CREATED",
                        )
                    else:
                        logger.error(
                            "Création de l'item échouée pour holding %s (%s).",
                            holding.holding_id,
                            loc,
                        )
                        notice_report.add_location(
                            location=loc,
                            holding_id=str(holding.get_holding_id()),
                            holding_status="CREATED",
                            item_status="ERROR",
                            item_error="Item creation failed (see logs)",
                        )

                else:
                    logger.error(
                        "Impossible de créer la holding pour location %s -> aucun item créé.",
                        loc,
                    )
                    notice_report.add_location(
                        location=loc,
                        holding_status="ERROR",
                        holding_error="Holding creation failed (see logs)",
                        item_status="SKIPPED",
                    )

            except Exception as e:
                handle_error(
                    f"❌ Erreur lors de la création de la holding {safe(holding_info.get('library_code'))}",
                    exc=e,
                    stop=False,
                )
                notice_report.add_location(
                    location=loc,
                    holding_status="ERROR",
                    holding_error=str(e),
                    item_status="SKIPPED",
                )

        reports.append(notice_report)

    # 6) Génération du rapport CSV
    if reports:
        rows = [rep.to_csv_row() for rep in reports]

        fieldnames = [
            "record_index",
            "infoscience_id",
            "title",
            "author",
            "call_number",
            "sru_exists",
            "mms_id",
            "bib_status",
            "bib_error",
            "holding_locations",
            "holding_ids",
            "holding_statuses",
            "holding_errors",
            "item_ids",
            "item_statuses",
            "item_errors",
        ]

        report_filename = f"{general_cfg['report_prefix']}{date.today().isoformat()}.csv"
        report_dir = Path(REPORT_DIR)
        report_dir.mkdir(parents=True, exist_ok=True)
        csv_path = report_dir / report_filename
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)

        logger.info("Rapport CSV écrit dans %s", csv_path.resolve())
    else:
        logger.info("Aucun rapport à écrire (aucune notice traitée).")

    # 7) Sauvegarde info : dernière cote utilisée (sans fallback)
    try:
        last_call_path = Path("last_call_number.txt")
        with last_call_path.open("w", encoding="utf-8") as f:
            f.write(str(call_number_value))
        logger.info(
            "Dernière cote utilisée enregistrée dans %s", last_call_path.resolve()
        )
    except Exception as e:
        logger.warning("Impossible d'écrire last_call_number.txt : %s", e)

    logger.info(
        "Fin du script. %s",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


# =============================================================================
# ARGUMENTS CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Création de notices Alma à partir d'Infoscience"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ne PAS créer de notices/holdings/items dans Alma (validation + logs uniquement).",
    )
    parser.add_argument(
        "--use-static-url",
        action="store_true",
        help="Utiliser l'URL Infoscience statique de test au lieu de la pagination dynamique.",
    )
    parser.add_argument(
        "--spc-page",
        type=int,
        default=1,
        help="Valeur initiale de spc.page pour la pagination Infoscience (défaut: 1).",
    )
    parser.add_argument(
        "--spc-rpp",
        type=int,
        default=-1,  # pour distinguer "non fourni"
        help="Nombre de résultats par page (spc.rpp). Si omis, pris depuis la config.",
    )
    parser.add_argument(
        "--env",
        choices=["S", "P"],
        default=None,
        help="Environnement Alma: S (sandbox) ou P (prod). Si omis, pris depuis le fichier de config.",
    )
    parser.add_argument(
        "--institution-code",
        default=None,
        help="Code Alma de l'institution (zone), ex. HPH, EPF... Si omis, pris depuis le fichier de config.",
    )
    parser.add_argument(
        "--no-xsd-check",
        action="store_true",
        help="Désactive la validation XSD des Bib/Holding/Item.",
    )
    parser.add_argument(
        "--skip-sru-check",
        action="store_true",
        help="Ignore la vérification SRU (swisscovery) et force la création des notices.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Nombre maximum de notices à traiter (0 = toutes).",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        default=None,
        help="Fichier INI de configuration pour holdings/items.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        dry_run=args.dry_run,
        use_static_url=args.use_static_url,
        spc_page=args.spc_page,
        spc_rpp=args.spc_rpp,
        env=args.env,
        institution_code=args.institution_code,
        check_xsd=not args.no_xsd_check,
        max_records=args.max_records,
        config_file=args.config_file,
        skip_sru_check=args.skip_sru_check
    )
