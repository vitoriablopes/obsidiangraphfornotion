# generate_graph.py
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import itertools
import urllib.request
import urllib.error
import urllib.parse
from typing import Dict, Any, List, Set, Optional, Tuple

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# =============================
# CONFIGURAÇÕES VIA GITHUB SECRETS / ENV
# =============================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

SOURCE_MATERIAL_PROP = os.environ.get("SOURCE_MATERIAL_PROP", "Source material notes")
MAIN_NOTES_PROP = os.environ.get("MAIN_NOTES_PROP", "Main notes")

OUTPUT_DIR = "dist"
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "index.html")
# =============================

API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

_PAGE_CACHE: Dict[str, Dict[str, Any]] = {}
_DATABASE_CACHE: Dict[str, Dict[str, Any]] = {}
_TARGET_INDEX_CACHE: Dict[str, Dict[str, str]] = {}


def notion_request(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    for attempt in range(3):
        req = urllib.request.Request(
            url=url,
            data=data,
            headers=HEADERS,
            method=method
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code in (429, 502, 503, 504) and attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"{method} {url} falhou [{e.code}]: {body}")

        except urllib.error.URLError as e:
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"Erro de conexão em {method} {url}: {e}")

    raise RuntimeError(f"{method} {url} falhou após retries")


def retrieve_database(database_id: str) -> Dict[str, Any]:
    if database_id in _DATABASE_CACHE:
        return _DATABASE_CACHE[database_id]

    data = notion_request("GET", f"{API_BASE}/databases/{database_id}")
    _DATABASE_CACHE[database_id] = data
    return data


def query_database_all(database_id: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/databases/{database_id}/query"
    results: List[Dict[str, Any]] = []
    payload: Dict[str, Any] = {}

    while True:
        data = notion_request("POST", url, payload)
        results.extend(data.get("results", []))

        if not data.get("has_more"):
            break

        payload["start_cursor"] = data.get("next_cursor")

    return results


def retrieve_page(page_id: str) -> Dict[str, Any]:
    if page_id in _PAGE_CACHE:
        return _PAGE_CACHE[page_id]

    data = notion_request("GET", f"{API_BASE}/pages/{page_id}")
    _PAGE_CACHE[page_id] = data
    return data


def retrieve_page_property_all(page_id: str, property_id: str) -> Dict[str, Any]:
    encoded_prop_id = urllib.parse.quote(property_id, safe="%")
    base_url = f"{API_BASE}/pages/{page_id}/properties/{encoded_prop_id}"
    url = base_url

    all_results: List[Dict[str, Any]] = []

    while True:
        data = notion_request("GET", url)

        if data.get("object") != "list":
            return data

        all_results.extend(data.get("results", []))

        if not data.get("has_more"):
            data["results"] = all_results
            return data

        next_url = data.get("next_url")
        if next_url:
            url = next_url
        else:
            next_cursor = data.get("next_cursor")
            if not next_cursor:
                data["results"] = all_results
                return data

            url = base_url + "?start_cursor=" + urllib.parse.quote(next_cursor, safe="%")


def normalize_name(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def get_prop_meta(schema: Dict[str, Any], prop_name: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    props = schema.get("properties", {})

    if prop_name in props:
        return prop_name, props[prop_name]

    target = normalize_name(prop_name)
    for name, meta in props.items():
        if normalize_name(name) == target:
            return name, meta

    return None


def title_of(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})

    for _, value in props.items():
        if value.get("type") == "title":
            arr = value.get("title", [])
            title = "".join([x.get("plain_text", "") for x in arr]).strip()
            if title:
                return title

    return page.get("url", page.get("id", "")[:8])


def url_of(page: Dict[str, Any]) -> str:
    return page.get("url", "")


def build_title_index(pages: List[Dict[str, Any]]) -> Dict[str, str]:
    idx: Dict[str, str] = {}

    for p in pages:
        idx[normalize_name(title_of(p))] = p["id"]

    return idx


def find_property_by_id(schema: Dict[str, Any], prop_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    for name, meta in (schema.get("properties") or {}).items():
        if meta.get("id") == prop_id:
            return name, meta

    return None


def relation_target_id_from_meta(rel_meta: Dict[str, Any]) -> Optional[str]:
    if not rel_meta or rel_meta.get("type") != "relation":
        return None

    relation = rel_meta.get("relation", {})
    return relation.get("database_id") or relation.get("data_source_id")


def detect_target_db_from_rollup(schema: Dict[str, Any], meta: Dict[str, Any]) -> Optional[str]:
    if not meta or meta.get("type") != "rollup":
        return None

    roll = meta.get("rollup", {})
    rel_prop_id = roll.get("relation_property_id")
    rel_prop_name = roll.get("relation_property_name")

    if rel_prop_id:
        found = find_property_by_id(schema, rel_prop_id)
        if found:
            _, rel_meta = found
            target = relation_target_id_from_meta(rel_meta)
            if target:
                return target

    if rel_prop_name:
        found = get_prop_meta(schema, rel_prop_name)
        if found:
            _, rel_meta = found
            target = relation_target_id_from_meta(rel_meta)
            if target:
                return target

    return None


def detect_target_db_from_relation(meta: Dict[str, Any]) -> Optional[str]:
    return relation_target_id_from_meta(meta)


def iter_page_mentions_from_rich_text(arr: Any):
    if isinstance(arr, dict):
        arr = [arr]

    if not isinstance(arr, list):
        return

    for node in arr:
        if not isinstance(node, dict):
            continue

        if node.get("type") == "mention":
            mention = node.get("mention", {})
            if mention.get("type") == "page":
                pid = mention.get("page", {}).get("id")
                if pid:
                    yield pid


def split_text_candidates(text: str) -> List[str]:
    if not text:
        return []

    separators = [",", ";", "|", "\n", "\r", "•", "·"]
    for sep in separators:
        text = text.replace(sep, "§")

    return [x.strip() for x in text.split("§") if x.strip()]


def resolve_title_to_id(title_text: str, idx_primary: Dict[str, str], idx_fallback: Dict[str, str]) -> Optional[str]:
    key = normalize_name(title_text)
    return idx_primary.get(key) or idx_fallback.get(key)


def rich_text_to_text(arr: Any) -> str:
    if isinstance(arr, dict):
        arr = [arr]

    if not isinstance(arr, list):
        return ""

    return "".join([x.get("plain_text", "") for x in arr if isinstance(x, dict)]).strip()


def add_relation_ids(ids: Set[str], relation_value: Any) -> None:
    if isinstance(relation_value, dict):
        rid = relation_value.get("id")
        if rid:
            ids.add(rid)

    elif isinstance(relation_value, list):
        for r in relation_value:
            if isinstance(r, dict):
                rid = r.get("id")
                if rid:
                    ids.add(rid)


def extract_ids_from_property_object(
    obj: Optional[Dict[str, Any]],
    idx_primary: Dict[str, str],
    idx_fallback: Dict[str, str]
) -> Set[str]:
    ids: Set[str] = set()

    if not obj or not isinstance(obj, dict):
        return ids

    if obj.get("object") == "list":
        for item in obj.get("results", []):
            ids.update(extract_ids_from_property_object(item, idx_primary, idx_fallback))

        if isinstance(obj.get("property_item"), dict):
            ids.update(extract_ids_from_property_object(obj.get("property_item"), idx_primary, idx_fallback))

        return ids

    ptype = obj.get("type")

    if ptype == "relation":
        add_relation_ids(ids, obj.get("relation"))
        return ids

    if ptype == "page":
        pid = obj.get("page", {}).get("id")
        if pid:
            ids.add(pid)
        return ids

    if ptype == "rollup":
        roll = obj.get("rollup", {})

        if roll.get("type") == "array":
            for item in roll.get("array", []):
                ids.update(extract_ids_from_property_object(item, idx_primary, idx_fallback))

        elif roll.get("type") == "relation":
            add_relation_ids(ids, roll.get("relation"))

        elif roll.get("type") == "title":
            pseudo = {"type": "title", "title": roll.get("title", [])}
            ids.update(extract_ids_from_property_object(pseudo, idx_primary, idx_fallback))

        elif roll.get("type") == "rich_text":
            pseudo = {"type": "rich_text", "rich_text": roll.get("rich_text", [])}
            ids.update(extract_ids_from_property_object(pseudo, idx_primary, idx_fallback))

        return ids

    if ptype in ("title", "rich_text"):
        arr = obj.get(ptype, [])

        found_mention = False
        for pid in iter_page_mentions_from_rich_text(arr):
            ids.add(pid)
            found_mention = True

        if not found_mention:
            text = rich_text_to_text(arr)
            for cand in split_text_candidates(text):
                rid = resolve_title_to_id(cand, idx_primary, idx_fallback)
                if rid:
                    ids.add(rid)

        return ids

    if ptype == "formula":
        f = obj.get("formula", {})
        ftype = f.get("type")

        if ftype == "rich_text":
            arr = f.get("rich_text", [])

            found_mention = False
            for pid in iter_page_mentions_from_rich_text(arr):
                ids.add(pid)
                found_mention = True

            if not found_mention:
                text = rich_text_to_text(arr)
                for cand in split_text_candidates(text):
                    rid = resolve_title_to_id(cand, idx_primary, idx_fallback)
                    if rid:
                        ids.add(rid)

        elif ftype == "string":
            for cand in split_text_candidates(f.get("string") or ""):
                rid = resolve_title_to_id(cand, idx_primary, idx_fallback)
                if rid:
                    ids.add(rid)

        return ids

    return ids


def get_resolution_context(
    schema: Dict[str, Any],
    pages_main: List[Dict[str, Any]],
    prop_meta: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    idx_main = build_title_index(pages_main)

    target_db_id = None
    if prop_meta:
        target_db_id = detect_target_db_from_rollup(schema, prop_meta) or detect_target_db_from_relation(prop_meta)

    idx_target: Dict[str, str] = {}

    if target_db_id:
        if target_db_id in _TARGET_INDEX_CACHE:
            idx_target = _TARGET_INDEX_CACHE[target_db_id]
        else:
            try:
                target_pages = query_database_all(target_db_id)
                idx_target = build_title_index(target_pages)
                _TARGET_INDEX_CACHE[target_db_id] = idx_target
                print(f"DB alvo detectado: {target_db_id} | páginas alvo: {len(target_pages)}")
            except Exception as e:
                print(f"Não consegui ler o DB alvo {target_db_id}: {e}")

    return {
        "idx_primary": idx_target or idx_main,
        "idx_fallback": idx_main
    }


def get_full_property_ids(
    page: Dict[str, Any],
    prop_name: str,
    prop_meta: Optional[Dict[str, Any]],
    ctx: Dict[str, Any]
) -> Set[str]:
    if not prop_meta:
        print(f"Aviso: propriedade '{prop_name}' não encontrada no schema.")
        return set()

    prop_id = prop_meta.get("id")

    if not prop_id:
        print(f"Aviso: propriedade '{prop_name}' não tem ID no schema.")
        return set()

    try:
        full_prop = retrieve_page_property_all(page["id"], prop_id)

        ids = extract_ids_from_property_object(
            full_prop,
            ctx["idx_primary"],
            ctx["idx_fallback"]
        )

        if len(ids) == 0:
            obj_type = full_prop.get("object")
            prop_type = full_prop.get("type")
            result_count = len(full_prop.get("results", [])) if isinstance(full_prop.get("results"), list) else 0

            print(
                f"[debug] {title_of(page)} | {prop_name} | "
                f"object={obj_type} | type={prop_type} | results={result_count} | ids_extraidos=0"
            )

            if result_count > 0:
                first = full_prop.get("results", [])[0]
                print(
                    f"        primeiro item: object={first.get('object')} | "
                    f"type={first.get('type')} | keys={list(first.keys())}"
                )

        return ids

    except Exception as e:
        print(f"Aviso: falha ao ler '{prop_name}' de '{title_of(page)}': {e}")
        return set()


def build_graph_data() -> Dict[str, Any]:
    pages_main = query_database_all(DATABASE_ID)
    schema = retrieve_database(DATABASE_ID)

    source_found = get_prop_meta(schema, SOURCE_MATERIAL_PROP)
    main_found = get_prop_meta(schema, MAIN_NOTES_PROP)

    print("\nPROPRIEDADES DISPONÍVEIS NO SCHEMA:")
    props = schema.get("properties", {})
    for prop_name, prop_meta in props.items():
        print(f" - {prop_name} | type: {prop_meta.get('type')} | id: {prop_meta.get('id')}")
    print()

    source_real_name, source_meta = source_found if source_found else (SOURCE_MATERIAL_PROP, None)
    main_real_name, main_meta = main_found if main_found else (MAIN_NOTES_PROP, None)

    print(
        f"[Schema] '{SOURCE_MATERIAL_PROP}' encontrado como '{source_real_name}' | "
        f"type: {source_meta.get('type') if source_meta else 'NÃO ENCONTRADA'} | "
        f"id: {source_meta.get('id') if source_meta else '-'}"
    )

    print(
        f"[Schema] '{MAIN_NOTES_PROP}' encontrado como '{main_real_name}' | "
        f"type: {main_meta.get('type') if main_meta else 'NÃO ENCONTRADA'} | "
        f"id: {main_meta.get('id') if main_meta else '-'}"
    )

    source_ctx = get_resolution_context(schema, pages_main, source_meta)
    main_ctx = get_resolution_context(schema, pages_main, main_meta)

    nodes_by_id: Dict[str, Dict[str, Any]] = {}
    links_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}

    page_to_source_ids: Dict[str, Set[str]] = {}
    page_to_main_ids: Dict[str, Set[str]] = {}

    def add_link(a: str, b: str, kind: str, shared_id: str):
        if not a or not b or a == b:
            return

        key = tuple(sorted([a, b]))

        if key not in links_by_key:
            links_by_key[key] = {
                "source": key[0],
                "target": key[1],
                "kind": kind,
                "shared_count": 1,
                "shared_ids": [shared_id],
            }
        else:
            if kind not in links_by_key[key]["kind"]:
                links_by_key[key]["kind"] += "+" + kind

            if shared_id not in links_by_key[key]["shared_ids"]:
                links_by_key[key]["shared_ids"].append(shared_id)
                links_by_key[key]["shared_count"] += 1

    sample_counts = []

    for page in pages_main:
        pid = page["id"]

        nodes_by_id[pid] = {
            "id": pid,
            "name": title_of(page),
            "url": url_of(page),
            "source_count": 0,
            "main_count": 0,
            "total_listed": 0,
            "val": 1,
        }

        source_ids = get_full_property_ids(page, SOURCE_MATERIAL_PROP, source_meta, source_ctx)
        main_ids = get_full_property_ids(page, MAIN_NOTES_PROP, main_meta, main_ctx)

        page_to_source_ids[pid] = source_ids
        page_to_main_ids[pid] = main_ids

        source_count = len(source_ids)
        main_count = len(main_ids)
        total_listed = source_count + main_count

        nodes_by_id[pid]["source_count"] = source_count
        nodes_by_id[pid]["main_count"] = main_count
        nodes_by_id[pid]["total_listed"] = total_listed
        nodes_by_id[pid]["val"] = max(1, total_listed)

        sample_counts.append((title_of(page), source_count, main_count))

    source_note_to_pages: Dict[str, Set[str]] = {}
    for page_id, source_ids in page_to_source_ids.items():
        for note_id in source_ids:
            source_note_to_pages.setdefault(note_id, set()).add(page_id)

    main_note_to_pages: Dict[str, Set[str]] = {}
    for page_id, main_ids in page_to_main_ids.items():
        for note_id in main_ids:
            main_note_to_pages.setdefault(note_id, set()).add(page_id)

    source_edges_raw = 0
    for note_id, page_ids in source_note_to_pages.items():
        if len(page_ids) < 2:
            continue

        for a, b in itertools.combinations(sorted(page_ids), 2):
            add_link(a, b, "source_material", note_id)
            source_edges_raw += 1

    main_edges_raw = 0
    for note_id, page_ids in main_note_to_pages.items():
        if len(page_ids) < 2:
            continue

        for a, b in itertools.combinations(sorted(page_ids), 2):
            add_link(a, b, "main_notes", note_id)
            main_edges_raw += 1

    print("Amostra de contagem por página:")
    for name, sc, mc in sample_counts[:8]:
        print(f" - {name}: Source material notes={sc} | Main notes={mc} | total={sc + mc}")

    print(f"Conexões por compartilhamento de '{SOURCE_MATERIAL_PROP}': {source_edges_raw}")
    print(f"Conexões por compartilhamento de '{MAIN_NOTES_PROP}': {main_edges_raw}")
    print(f"Total de nós: {len(nodes_by_id)}")
    print(f"Total de links únicos: {len(links_by_key)}")

    return {
        "nodes": list(nodes_by_id.values()),
        "links": list(links_by_key.values()),
        "meta": {
            "source_edges": source_edges_raw,
            "main_edges": main_edges_raw,
            "node_count": len(nodes_by_id),
            "link_count": len(links_by_key),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    }


def write_html(graph_data: Dict[str, Any]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    graph_json = json.dumps(graph_data, ensure_ascii=False)

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <title>Notion Graph 3D</title>
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      background: white;
      font-family: Arial, sans-serif;
    }}

    #graph {{
      width: 100vw;
      height: 100vh;
      background: white;
    }}

    #panel {{
      position: fixed;
      top: 12px;
      left: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: #111;
      padding: 12px 14px;
      border: 1px solid #ddd;
      border-radius: 10px;
      font-size: 13px;
      z-index: 10;
      max-width: 460px;
      box-shadow: 0 3px 12px rgba(0,0,0,0.08);
    }}

    #refreshBtn {{
      margin-top: 10px;
      padding: 8px 12px;
      border: 1px solid #006400;
      background: #006400;
      color: white;
      border-radius: 7px;
      cursor: pointer;
      font-weight: 600;
    }}

    #status {{
      margin-top: 8px;
      color: #333;
      font-size: 12px;
      line-height: 1.35;
    }}
  </style>
  <script src="https://unpkg.com/3d-force-graph"></script>
</head>

<body>
  <div id="panel">
    <b>Notion Graph 3D</b><br>
    Tamanho do nó = Source material notes + Main notes<br>
    Linha = páginas que compartilham Source material notes ou Main notes<br>
    Arraste para girar · Scroll para zoom · Clique em um ponto para abrir a página<br>

    <button id="refreshBtn">Recarregar visualização</button>
    <div id="status"></div>
  </div>

  <div id="graph"></div>

  <script>
    const graphData = {graph_json};

    const Graph = ForceGraph3D()(document.getElementById("graph"))
      .graphData(graphData)
      .backgroundColor("#ffffff")
      .nodeLabel(node => `
        <b>${{node.name}}</b><br>
        Source material notes: ${{node.source_count || 0}}<br>
        Main notes: ${{node.main_count || 0}}<br>
        Total usado no tamanho: ${{node.total_listed || 0}}<br>
        Clique para abrir
      `)
      .nodeColor(() => "#006400")
      .nodeVal(node => Math.max(1, node.val || 1))
      .nodeRelSize(5)
      .linkColor(() => "#000000")
      .linkWidth(() => 1)
      .linkOpacity(1)
      .linkLabel(link => `
        Tipo: ${{link.kind}}<br>
        Páginas compartilhadas: ${{link.shared_count || 1}}
      `)
      .linkDirectionalParticles(0)
      .onNodeClick(node => {{
        if (node.url) window.open(node.url, "_blank");
      }});

    Graph.d3Force("charge").strength(-130);
    Graph.d3Force("link").distance(110);

    const meta = graphData.meta || {{}};
    document.getElementById("status").innerHTML =
      "Atualizado em: " + (meta.updated_at || "—") + "<br>" +
      "Nós: " + (meta.node_count ?? graphData.nodes.length) + " · " +
      "Links: " + (meta.link_count ?? graphData.links.length) + "<br>" +
      "Source material: " + (meta.source_edges ?? 0) + " · " +
      "Main notes: " + (meta.main_edges ?? 0);

    document.getElementById("refreshBtn").addEventListener("click", () => {{
      window.location.reload();
    }});
  </script>
</body>
</html>
"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML gerado em: {OUTPUT_HTML}")


if __name__ == "__main__":
    print("Gerando grafo a partir do Notion...")
    data = build_graph_data()
    write_html(data)
    print("Pronto.")
