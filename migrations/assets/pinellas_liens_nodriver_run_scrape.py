async def run_scrape(page, download_dir, start_date, end_date, url, county_id):
    await page.set_download_path(str(download_dir))
    await page.sleep(3)

    challenge_markers = ("just a moment", "checking your browser", "verify you are human")
    doc_type_tile = None
    for attempt in range(5):
        try:
            title = await page.evaluate("document.title")
            body = await page.evaluate("document.body ? document.body.innerText.slice(0,500) : ''")
        except Exception:
            title, body = "", ""
        text = (str(title) + " " + str(body)).lower()
        if not any(m in text for m in challenge_markers):
            doc_type_tile = await page.select('img[alt="Doc Type"]', timeout=5)
            if doc_type_tile is not None:
                break
        try:
            await page.verify_cf()
        except Exception:
            pass
        await page.sleep(6)

    if doc_type_tile is None:
        doc_type_tile = await page.select('img[alt="Doc Type"]', timeout=15)
    if doc_type_tile is None:
        raise RuntimeError("CF_CHALLENGE_NOT_CLEARED - Turnstile checkbox never resolved")

    await doc_type_tile.click()
    await page.sleep(2)

    accept_btn = await page.select("#btnButton", timeout=3)
    if accept_btn is not None:
        await accept_btn.click()
        await page.sleep(1)

    date_from = await page.select("#RecordDateFrom", timeout=15)
    if date_from is None:
        raise RuntimeError("PORTAL_LAYOUT_CHANGED - #RecordDateFrom not found")
    await date_from.clear_input()
    await date_from.send_keys(start_date)

    date_to = await page.select("#RecordDateTo", timeout=15)
    if date_to is None:
        raise RuntimeError("PORTAL_LAYOUT_CHANGED - #RecordDateTo not found")
    await date_to.clear_input()
    await date_to.send_keys(end_date)

    search_btn = await page.select("#btnSearch", timeout=10)
    if search_btn is None:
        raise RuntimeError("PORTAL_LAYOUT_CHANGED - #btnSearch not found")
    await search_btn.click()
    await page.sleep(5)

    csv_btn = await page.select("#btnCsvButton", timeout=30)
    if csv_btn is None:
        return pd.DataFrame()

    existing = set(download_dir.glob("*.csv"))
    await csv_btn.click()
    await page.sleep(8)

    candidates = [f for f in download_dir.glob("*.csv") if f not in existing]
    if not candidates:
        candidates = list(download_dir.glob("*.csv"))
    if not candidates:
        raise RuntimeError("DOWNLOAD_FAILED - no CSV file appeared after export click")

    dest = max(candidates, key=lambda p: p.stat().st_mtime)
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            df = pd.read_csv(dest, encoding=enc)
            df["county_id"] = county_id
            return df
        except Exception:
            continue
    return pd.DataFrame()
