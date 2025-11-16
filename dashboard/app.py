import json
import requests
import streamlit as st

DEFAULT_API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(
    page_title="Dynamic ETL Pipeline Dashboard",
    layout="wide",
)


def get_api_base() -> str:
    if "api_base" not in st.session_state:
        st.session_state.api_base = DEFAULT_API_BASE
    return st.session_state.api_base


def call_api(method: str, path: str, **kwargs):
    base = get_api_base()
    url = f"{base}{path}"
    try:
        resp = requests.request(method, url, timeout=30, **kwargs)
        return resp
    except Exception as e:
        st.error(f"Request error: {e}")
        return None


def pretty_json(data):
    st.json(data)


st.sidebar.title("Settings")

api_base_input = st.sidebar.text_input(
    "API base URL",
    value=get_api_base(),
    help="Backend FastAPI base (e.g., http://localhost:8000/api/v1)",
)
st.session_state.api_base = api_base_input.rstrip("/")

st.sidebar.markdown("---")
mode = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "Upload",
        "Sources & Files",
        "File Fragments",
        "Schema Explorer",
        "Schema Diff",
    ],
)


def page_overview():
    st.title("Dynamic ETL Pipeline Dashboard")
    st.markdown("### Overview")
    st.write(
        "This dashboard lets you interact with your Dynamic ETL backend:\n"
        "- Upload unstructured files\n"
        "- Inspect detected fragments (JSON, CSV, KV, HTML tables, text)\n"
        "- Infer and version schemas per source\n"
        "- Compare schema versions and detect drift"
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### Health Check")
        resp = call_api("get", "/health")
        if resp is not None and resp.status_code == 200:
            st.success("Backend is healthy ✅")
            try:
                st.json(resp.json())
            except Exception:
                st.write(resp.text)
        else:
            st.error("Backend is not reachable or unhealthy ❌")

    with col2:
        st.markdown("#### Sample: Sources")
        resp = call_api("get", "/sources")
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list) and data:
                    st.write(f"Total sources: {len(data)}")
                    st.dataframe(data)
                else:
                    st.info("No sources yet. Upload files with a source_id.")
            except Exception:
                st.write(resp.text)
        else:
            st.info("Upload some files first to see sources.")

    with col3:
        st.markdown("#### Quick Tips")
        st.write(
            "- Use **Upload** page to send files\n"
            "- Use **Sources & Files** to locate file IDs\n"
            "- Use **File Fragments** to inspect parsed content\n"
            "- Use **Schema Explorer** to infer and view schemas\n"
            "- Use **Schema Diff** to compare schema versions"
        )


def page_upload():
    st.title("Upload")
    st.markdown("#### Upload a new file for ETL processing")

    source_id = st.text_input("Source ID (optional)", "")
    file = st.file_uploader("Choose a file")

    extra_meta = st.text_area(
        "Optional metadata (raw text, not used programmatically yet)",
        "",
        help="You can leave this empty for now.",
    )

    if st.button("Upload file"):
        if file is None:
            st.warning("Please select a file first.")
            return

        files = {"file": (file.name, file.read(), file.type)}
        data = {}
        if source_id:
            data["source_id"] = source_id
        if extra_meta:
            data["metadata"] = extra_meta

        st.info("Uploading file to backend...")
        resp = call_api("post", "/upload", data=data, files=files)
        if resp is None:
            return

        st.write(f"Status: {resp.status_code}")
        try:
            payload = resp.json()
            st.subheader("Backend Response")
            pretty_json(payload)

            if "file_id" in payload:
                st.success(f"File ID: {payload['file_id']}")
                st.code(payload["file_id"], language="text")

        except Exception:
            st.write(resp.text)


def page_sources_files():
    st.title("Sources & Files")

    st.markdown("### Sources")
    if st.button("Refresh sources"):
        resp = call_api("get", "/sources")
        if resp is not None:
            try:
                data = resp.json()
                if isinstance(data, list) and data:
                    st.success(f"Found {len(data)} sources")
                    st.dataframe(data)
                else:
                    st.info("No sources found yet.")
            except Exception:
                st.write(resp.text)

    st.markdown("---")
    st.markdown("### Files")

    col1, col2 = st.columns(2)
    with col1:
        limit = st.number_input("Limit", min_value=1, max_value=500, value=50)
    with col2:
        offset = st.number_input("Offset", min_value=0, value=0)

    if st.button("Load files"):
        resp = call_api("get", f"/files?limit={limit}&offset={offset}")
        if resp is not None:
            try:
                data = resp.json()
                if isinstance(data, list) and data:
                    st.success(f"Returned {len(data)} files")
                    st.dataframe(data)
                    st.info("Tip: Copy a file_id from this table and use it in the File Fragments page.")
                else:
                    st.info("No files found.")
            except Exception:
                st.write(resp.text)


def page_file_fragments():
    st.title("File Fragments")

    file_id = st.text_input("File ID", help="Paste a file_id from the Sources & Files page")
    fragment_type = st.selectbox(
        "Fragment type (optional filter)",
        ["", "json", "csv", "kv", "html", "text"],
    )

    if st.button("Load fragments"):
        if not file_id:
            st.warning("Enter a file ID first.")
        else:
            params = {}
            if fragment_type:
                params["fragment_type"] = fragment_type
            resp = call_api("get", f"/files/{file_id}/fragments", params=params)
            if resp is not None and resp.status_code == 200:
                st.session_state.fragments_data = resp.json()
            elif resp is not None:
                st.error(f"Error: {resp.status_code} - {resp.text}")
                st.session_state.fragments_data = None
            else:
                st.session_state.fragments_data = None

    if "fragments_data" in st.session_state and st.session_state.fragments_data:
        data = st.session_state.fragments_data
        st.success(f"Total fragments: {len(data)}")
        by_type = {}
        for frag in data:
            t = frag.get("fragment_type", "unknown")
            by_type.setdefault(t, []).append(frag)

        for t, frags in by_type.items():
            st.markdown(f"### Fragment type: `{t}` ({len(frags)})")
            for frag in frags:
                label = f"ID: {frag['id']} | records={frag.get('record_count')}"
                with st.expander(label):
                    st.markdown("**Fragment metadata (Postgres):**")
                    st.json({k: v for k, v in frag.items() if k != "preview_json"})
                    preview = frag.get("preview_json")
                    st.markdown("**Preview (parsed if possible):**")
                    if preview is None:
                        st.write("No preview_json stored.")
                    elif isinstance(preview, list):
                        if len(preview) > 0 and isinstance(preview[0], dict):
                            st.dataframe(preview)
                        else:
                            st.json(preview)
                    elif isinstance(preview, dict):
                        st.json(preview)
                    else:
                        st.text(str(preview))


def page_schema_explorer():
    st.title("Schema Explorer")

    source_id = st.text_input("Source ID", help="Must match the source_id used when uploading files")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Infer schema now"):
            if not source_id:
                st.warning("Enter a source ID first.")
            else:
                resp = call_api("post", f"/schema/infer/{source_id}")
                if resp is not None and resp.status_code == 200:
                    st.session_state.infer_schema_data = resp.json()
                elif resp is not None:
                    st.error(f"Error: {resp.status_code} - {resp.text}")
                    st.session_state.infer_schema_data = None
                else:
                    st.session_state.infer_schema_data = None

        if "infer_schema_data" in st.session_state and st.session_state.infer_schema_data:
            payload = st.session_state.infer_schema_data
            st.subheader("Inference Result Row")
            st.json(payload)
            schema_raw = payload.get("schema")
            if schema_raw:
                st.subheader("Parsed Schema")
                st.json(schema_raw)

    with col2:
        if st.button("Get latest schema"):
            if not source_id:
                st.warning("Enter a source ID first.")
            else:
                resp = call_api("get", f"/schema/{source_id}/latest")
                if resp is not None and resp.status_code == 200:
                    st.session_state.latest_schema_data = resp.json()
                elif resp is not None:
                    st.error(f"Error: {resp.status_code} - {resp.text}")
                    st.session_state.latest_schema_data = None
                else:
                    st.session_state.latest_schema_data = None

        if "latest_schema_data" in st.session_state and st.session_state.latest_schema_data:
            payload = st.session_state.latest_schema_data
            st.subheader("Latest Schema Row")
            st.json(payload)
            schema_raw = payload.get("schema")
            if schema_raw:
                st.subheader("Parsed Schema")
                st.json(schema_raw)

    with col3:
        if st.button("List all schema versions"):
            if not source_id:
                st.warning("Enter a source ID first.")
            else:
                resp = call_api("get", f"/schema/{source_id}/versions")
                if resp is not None:
                    try:
                        data = resp.json()
                        if isinstance(data, list) and data:
                            st.subheader("Schema Versions")
                            st.dataframe(data)
                            st.info("Use these version numbers in the Schema Diff page.")
                        else:
                            st.info("No schema versions for this source yet.")
                    except Exception:
                        st.write(resp.text)


def page_schema_diff():
    st.title("Schema Diff")

    source_id = st.text_input("Source ID", help="Same source_id you used during upload")

    st.markdown("#### 1. Load available schema versions")
    if st.button("Load versions for diff"):
        if not source_id:
            st.warning("Enter a source ID first.")
        else:
            resp = call_api("get", f"/schema/{source_id}/versions")
            if resp is not None:
                try:
                    data = resp.json()
                    if isinstance(data, list) and data:
                        st.session_state.schema_versions = data
                        st.success(f"Loaded {len(data)} schema versions.")
                        st.dataframe(data)
                    else:
                        st.info("No schema versions found for this source.")
                except Exception:
                    st.write(resp.text)

    versions = st.session_state.get("schema_versions", [])

    if versions:
        available_versions = sorted([int(v["version"]) for v in versions])
    else:
        available_versions = []

    st.markdown("#### 2. Select versions to compare")

    col1, col2 = st.columns(2)

    if available_versions:
        default_v1 = available_versions[0]
        default_v2 = available_versions[-1]
    else:
        default_v1 = 1
        default_v2 = 2

    with col1:
        v1 = st.number_input(
            "Version 1",
            min_value=1,
            value=default_v1,
            step=1,
        )
    with col2:
        v2 = st.number_input(
            "Version 2",
            min_value=1,
            value=default_v2,
            step=1,
        )

    if st.button("Compare versions"):
        if not source_id:
            st.warning("Enter a source ID first.")
        elif v1 == v2:
            st.warning("Choose two different versions.")
        else:
            params = {"v1": int(v1), "v2": int(v2)}
            resp = call_api("get", f"/schema/compare/{source_id}", params=params)
            if resp is not None and resp.status_code == 200:
                st.session_state.schema_diff_data = resp.json()
            elif resp is not None:
                st.error(f"Error: {resp.status_code} - {resp.text}")
                st.session_state.schema_diff_data = None
            else:
                st.session_state.schema_diff_data = None

    if "schema_diff_data" in st.session_state and st.session_state.schema_diff_data:
        payload = st.session_state.schema_diff_data
        st.subheader("Raw Diff Response")
        st.json(payload)
        diff = payload.get("diff")
        if diff:
            added = diff.get("added_fields", [])
            removed = diff.get("removed_fields", [])
            changed = diff.get("changed_fields", {})
            st.markdown("### Summary")
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("Added fields", len(added))
            with col_b:
                st.metric("Removed fields", len(removed))
            with col_c:
                st.metric("Changed fields", len(changed))
            st.markdown("### Details")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown("#### Added Fields")
                st.json(added) if added else st.write("None")
            with col2:
                st.markdown("#### Removed Fields")
                st.json(removed) if removed else st.write("None")
            with col3:
                st.markdown("#### Changed Fields")
                st.json(changed) if changed else st.write("None")

if mode == "Overview":
    page_overview()
elif mode == "Upload":
    page_upload()
elif mode == "Sources & Files":
    page_sources_files()
elif mode == "File Fragments":
    page_file_fragments()
elif mode == "Schema Explorer":
    page_schema_explorer()
elif mode == "Schema Diff":
    page_schema_diff()
