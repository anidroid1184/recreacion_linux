from __future__ import annotations
from typing import List, Dict
import json
import os
import logging


class TrackerService:
    """Business rules for status normalization, alerts, and decisions.
    Local copy for recreacion_linux. Loads mappings from recreacion_linux/data.
    """

    NORM_MAP = {
        "entregado": "ENTREGADO",
        "transito": "EN_TRANSITO",
        "tránsito": "EN_TRANSITO",
        "camino": "EN_TRANSITO",
        "ruta": "EN_TRANSITO",
        "centro": "EN_TRANSITO",
        "pendiente": "PENDIENTE",
        "origen": "PENDIENTE",
        "recibimos": "EN_TRANSITO",
        "devuelto": "DEVUELTO",
        "devolución": "DEVUELTO",
        "retorno": "DEVUELTO",
        "agencia": "EN_AGENCIA",
        "recoger": "EN_AGENCIA",
        "guia_generada": "GUIA_GENERADA",
        "guía generada": "GUIA_GENERADA",
        "preparado_para_transportadora": "GUIA_GENERADA",
        "preparado para transportadora": "GUIA_GENERADA",
    }

    OVERRIDES = {
        "envío pendiente por admitir": "PENDIENTE",
        "envio pendiente por admitir": "PENDIENTE",
        "pendiente por admitir": "PENDIENTE",
    }

    _COMPILED_MAP: Dict[str, str] | None = None

    @staticmethod
    def _load_mappings() -> Dict[str, str]:
        """Load and compile keyword->status mapping from JSON files once.

        It merges both Dropi and Inter mappings into a single lowercase keyword
        dictionary. Later, OVERRIDES and NORM_MAP provide precedence/fallbacks.
        """
        if TrackerService._COMPILED_MAP is not None:
            return TrackerService._COMPILED_MAP

        # Base directories
        # recreacion_linux/services -> recreacion_linux
        base_dir = os.path.dirname(os.path.dirname(__file__))
        project_root = os.path.dirname(base_dir)
        data_dir = os.path.join(base_dir, "data")

        # Allow overrides via environment variables
        dropi_env = os.path.normpath(os.path.expandvars(os.path.expanduser(os.getenv("DROPI_MAP_PATH", "").strip()))) if os.getenv("DROPI_MAP_PATH") else ""
        inter_env = os.path.normpath(os.path.expandvars(os.path.expanduser(os.getenv("INTER_MAP_PATH", "").strip()))) if os.getenv("INTER_MAP_PATH") else ""

        # Candidate paths (first existing wins)
        dropi_candidates = [
            dropi_env if dropi_env else "",
            os.path.join(data_dir, "dropi_map.json"),
            os.path.join(base_dir, "dropi_map.json"),  # if user pasted here
            os.path.join(project_root, "dropi_map.json"),  # repo root
        ]
        inter_candidates = [
            inter_env if inter_env else "",
            os.path.join(data_dir, "interrapidisimo_traking_map.json"),
            os.path.join(base_dir, "interrapidisimo_traking_map.json"),  # if user pasted here
            os.path.join(project_root, "interrapidisimo_traking_map.json"),  # repo root
        ]

        compiled: Dict[str, str] = {}
        used_paths: List[str] = []

        def _ingest(path: str):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # data format: { "STATUS": ["keyword1", ...] }
                for status, keywords in data.items():
                    for kw in keywords:
                        if isinstance(kw, str):
                            compiled[kw.strip().lower()] = status.strip().upper()
                used_paths.append(path)
            except Exception:
                # Skip if missing/invalid to keep runtime resilient
                pass

        # Resolve first valid candidate per file
        for path in dropi_candidates:
            if path and os.path.isfile(path):
                _ingest(path)
                break
        for path in inter_candidates:
            if path and os.path.isfile(path):
                _ingest(path)
                break

        if used_paths:
            logging.info("TrackerService mappings loaded from: %s", used_paths)
        else:
            logging.warning("TrackerService mappings: no mapping files found, using heuristics/overrides only")

        TrackerService._COMPILED_MAP = compiled
        return compiled

    @staticmethod
    def normalize_status(s: str) -> str:
        if not s:
            return "PENDIENTE"
        text = s.strip().lower()

        for phrase, status in TrackerService.OVERRIDES.items():
            if phrase in text:
                return status

        compiled = TrackerService._load_mappings()
        for kw, status in compiled.items():
            if kw in text:
                return status

        for k, v in TrackerService.NORM_MAP.items():
            if k in text:
                return v

        return "EN_TRANSITO"

    @staticmethod
    def explain_normalization(s: str) -> dict:
        raw = s or ""
        if not raw:
            return {"matched": False, "via": "fallback", "keyword": None, "status": "PENDIENTE", "raw": raw}

        text = raw.strip().lower()

        for phrase, status in TrackerService.OVERRIDES.items():
            if phrase in text:
                return {"matched": True, "via": "override", "keyword": phrase, "status": status, "raw": raw}

        compiled = TrackerService._load_mappings()
        for kw, status in compiled.items():
            if kw in text:
                return {"matched": True, "via": "mapping", "keyword": kw, "status": status, "raw": raw}

        for k, v in TrackerService.NORM_MAP.items():
            if k in text:
                return {"matched": True, "via": "heuristic", "keyword": k, "status": v, "raw": raw}

        return {"matched": False, "via": "fallback", "keyword": None, "status": "EN_TRANSITO", "raw": raw}

    @staticmethod
    def compute_alert(dropi: str, tracking: str) -> str:
        d, w = dropi, tracking
        if d == "GUIA_GENERADA" and w == "ENTREGADO":
            return "TRUE"
        if d == "ENTREGADO" and w != "ENTREGADO":
            return "TRUE"
        if d == "DEVUELTO" and w != "DEVUELTO":
            return "TRUE"
        if d != w:
            return "TRUE"
        return "FALSE"

    @staticmethod
    def can_query(dropi: str) -> bool:
        return dropi in {
            "GUIA_GENERADA",
            "PENDIENTE",
            "EN_PROCESAMIENTO",
            "EN_BODEGA_TRANSPORTADORA",
            "EN_TRANSITO",
            "EN_BODEGA_DESTINO",
            "EN_REPARTO",
            "INTENTO_DE_ENTREGA",
            "NOVEDAD",
            "REEXPEDICION",
            "REENVIO",
            "EN_AGENCIA",
        }

    @staticmethod
    def terminal(dropi: str, tracking: str) -> bool:
        return ("ENTREGADO" in {dropi, tracking}) or ("DEVUELTO" in {dropi, tracking})

    @staticmethod
    def prepare_new_rows(source_data: List[Dict], existing_guias: set) -> List[List[str]]:
        rows = []
        for item in source_data:
            guia = item.get("ID TRACKING", "").strip()
            if not guia or guia in existing_guias:
                continue
            rows.append([
                item.get("ID DROPI", ""),
                guia,
                item.get("STATUS DROPI", ""),
                "",
                "FALSE",
            ])
            existing_guias.add(guia)
        return rows
