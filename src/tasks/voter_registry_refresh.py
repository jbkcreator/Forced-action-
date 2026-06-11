"""
Hillsborough SOE voter registry auto-refresh.

Polls the MediaFire public folder (key: 91e7q622dhkgk) for new monthly subfolders.
When a new month's folder appears, downloads "All Eligible Voters.zip", extracts the
inner .txt file, and runs it through VoterRegistryLoader.

Cron schedule: daily 19th–25th of each month at 04:45 UTC (publication date drifts;
the folder appears ~1 month after the data month). Exits fast on days with no new folder.

State: last-processed folder key stored in county_sources.meta_data JSONB for the
hillsborough voter_registry source row.

Pinellas stays manual-upload until SOE confirms delivery method.
"""

import io
import logging
import re
import zipfile
from typing import Optional

import requests

from src.core.database import get_db_context
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

_MEDIAFIRE_FOLDER_API = "https://www.mediafire.com/api/1.5/folder/get_content.php"
_ROOT_FOLDER_KEY = "91e7q622dhkgk"
_COUNTY_ID = "hillsborough"
_STATE_KEY = "voter_refresh_last_folder_key"


def _get_subfolders(folder_key: str) -> list[dict]:
    """Return list of subfolder dicts from a MediaFire public folder."""
    params = {
        "folder_key": folder_key,
        "content_type": "folders",
        "order_by": "created",
        "order_direction": "desc",
        "response_format": "json",
    }
    try:
        resp = requests_get_with_retry(_MEDIAFIRE_FOLDER_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", {}).get("folder_content", {}).get("folders", []) or []
    except Exception as e:
        logger.error("[VoterRefresh] Failed to list subfolders for key=%s: %s", folder_key, e)
        raise


def _get_files_in_folder(folder_key: str) -> list[dict]:
    """Return list of file dicts from a MediaFire public folder."""
    params = {
        "folder_key": folder_key,
        "content_type": "files",
        "response_format": "json",
    }
    try:
        resp = requests_get_with_retry(_MEDIAFIRE_FOLDER_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", {}).get("folder_content", {}).get("files", []) or []
    except Exception as e:
        logger.error("[VoterRefresh] Failed to list files for key=%s: %s", folder_key, e)
        raise


def _find_voter_zip_link(files: list[dict]) -> Optional[str]:
    """Return the download link for 'All Eligible Voters.zip' in a file list."""
    for f in files:
        name = f.get("filename", "")
        if "eligible" in name.lower() and name.lower().endswith(".zip"):
            # MediaFire file page URL — extract direct download link
            page_url = f.get("links", {}).get("normal_download") or f.get("links", {}).get("view")
            if page_url:
                return page_url
    return None


def _resolve_direct_download(page_url: str) -> Optional[str]:
    """
    Follow a MediaFire file page to find the direct download href.
    MediaFire embeds the download link in the page HTML.
    """
    try:
        resp = requests_get_with_retry(page_url, timeout=30)
        resp.raise_for_status()
        html = resp.text
        # Look for aria-label="Download file" or direct download href
        m = re.search(r'href="(https://download\d+\.mediafire\.com/[^"]+)"', html)
        if m:
            return m.group(1)
        m = re.search(r'"(https://download[^"]+\.zip[^"]*)"', html)
        if m:
            return m.group(1)
    except Exception as e:
        logger.error("[VoterRefresh] Failed to resolve direct link from %s: %s", page_url, e)
    return None


def _get_last_processed_key() -> Optional[str]:
    with get_db_context() as session:
        row = session.execute(
            __import__("sqlalchemy").text("""
                SELECT meta_data FROM county_sources
                WHERE county_id = :cid AND signal_type = 'voter_registry'
                LIMIT 1
            """),
            {"cid": _COUNTY_ID},
        ).mappings().first()
        if row and row["meta_data"]:
            return row["meta_data"].get(_STATE_KEY)
    return None


def _save_last_processed_key(folder_key: str) -> None:
    with get_db_context() as session:
        session.execute(
            __import__("sqlalchemy").text("""
                UPDATE county_sources
                SET meta_data = COALESCE(meta_data, '{}'::jsonb) || jsonb_build_object(:k, :v)
                WHERE county_id = :cid AND signal_type = 'voter_registry'
            """),
            {"k": _STATE_KEY, "v": folder_key, "cid": _COUNTY_ID},
        )


def run_voter_registry_refresh(force: bool = False) -> dict:
    """
    Check for a new monthly voter file and load it if available.

    Args:
        force: Re-process even if the folder key matches the last-processed key.

    Returns:
        dict with keys: skipped, inserted, updated, quarantined, folder_key.
    """
    logger.info("[VoterRefresh] Checking MediaFire folder key=%s", _ROOT_FOLDER_KEY)

    subfolders = _get_subfolders(_ROOT_FOLDER_KEY)
    if not subfolders:
        logger.info("[VoterRefresh] No subfolders found — nothing to process")
        return {"skipped": True, "reason": "no_subfolders"}

    # Newest subfolder first (already ordered desc by created)
    newest = subfolders[0]
    newest_key = newest.get("folderkey") or newest.get("key")
    newest_name = newest.get("name", "")

    if not newest_key:
        logger.error("[VoterRefresh] Could not extract folder key from %s", newest)
        return {"skipped": True, "reason": "no_folder_key"}

    last_key = _get_last_processed_key()
    if newest_key == last_key and not force:
        logger.info(
            "[VoterRefresh] Folder %s (%s) already processed — skipping",
            newest_name, newest_key,
        )
        return {"skipped": True, "reason": "already_processed", "folder_key": newest_key}

    logger.info("[VoterRefresh] New folder found: %s (%s)", newest_name, newest_key)

    files = _get_files_in_folder(newest_key)
    page_url = _find_voter_zip_link(files)
    if not page_url:
        logger.error("[VoterRefresh] No eligible voter zip found in folder %s", newest_key)
        return {"skipped": True, "reason": "no_voter_zip", "folder_key": newest_key}

    direct_url = _resolve_direct_download(page_url)
    if not direct_url:
        logger.error("[VoterRefresh] Could not resolve direct download link from %s", page_url)
        return {"skipped": True, "reason": "no_direct_link", "folder_key": newest_key}

    logger.info("[VoterRefresh] Downloading voter zip from %s", direct_url)
    try:
        resp = requests_get_with_retry(direct_url, timeout=600, stream=True)
        resp.raise_for_status()
        zip_bytes = resp.content
    except Exception as e:
        logger.error("[VoterRefresh] Download failed: %s", e)
        raise

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
            if not txt_names:
                raise ValueError("Zip contains no .txt voter file")
            txt_bytes = zf.read(txt_names[0])
    except Exception as e:
        logger.error("[VoterRefresh] Zip extraction failed: %s", e)
        raise

    import pandas as pd
    from src.loaders.voter_registry import VoterRegistryLoader

    content = txt_bytes.decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(content), dtype=str, sep="\t", header=None)
    df = VoterRegistryLoader.inject_fl_dos_header(df)

    logger.info("[VoterRefresh] Loaded %d voter rows from %s", len(df), newest_name)

    with get_db_context() as session:
        loader = VoterRegistryLoader(session, county_id=_COUNTY_ID)
        inserted, updated, quarantined = loader.load_from_dataframe(df)

    _save_last_processed_key(newest_key)

    logger.info(
        "[VoterRefresh] Done — inserted=%d updated=%d quarantined=%d folder=%s",
        inserted, updated, quarantined, newest_name,
    )
    return {
        "skipped": False,
        "inserted": inserted,
        "updated": updated,
        "quarantined": quarantined,
        "folder_key": newest_key,
        "folder_name": newest_name,
    }


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    result = run_voter_registry_refresh(force=force)
    print(result)
