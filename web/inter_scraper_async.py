from __future__ import annotations
import asyncio
import logging
from contextlib import suppress
from typing import Iterable, List, Tuple

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


class AsyncInterScraper:
    """Async Playwright scraper for Interrapidísimo with concurrency control.

    - Strictly reads the status from the detail card:
      div.content p.title-current-state + p.font-weight-600
    - Follows the new tab created after entering the tracking number.
    - Exposes get_status_many to process multiple guides concurrently.
    """

    def __init__(self, headless: bool = True, max_concurrency: int = 3, slow_mo: int = 0,
                 retries: int = 2, timeout_ms: int = 30000, block_resources: bool = True,
                 debug: bool = False):
        # Public/backwards-compatible attributes
        self.headless = headless
        self.max_concurrency = max(1, int(max_concurrency))
        # When headed, enforce at least 100ms to reduce race conditions with UI animations
        # When headless, allow caller-provided slow_mo (e.g., 50–150) as anti-bot softening
        self.slow_mo = slow_mo if headless else max(slow_mo, 100)
        self.retries = max(0, int(retries))
        self._retries = self.retries  # maintain compatibility with existing references
        self.timeout = int(timeout_ms)
        self._timeout = self.timeout  # maintain compatibility with existing references
        self.block_resources = block_resources
        self.debug = debug
        self._pw = None
        self.browser = None
        self._sem = asyncio.Semaphore(self.max_concurrency)

    async def start(self):
        logging.info("[PW] Starting async_playwright...")
        self._pw = await async_playwright().start()
        logging.info("[PW] Launching Chromium. headless=%s", self.headless)
        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1280,800",
        ]
        if not self.headless:
            launch_args.append("--start-maximized")
        self.browser = await self._pw.chromium.launch(headless=self.headless, slow_mo=self.slow_mo, args=launch_args)
        logging.info("[PW] Chromium launched. slow_mo=%s", self.slow_mo)

    async def close(self):
        with suppress(Exception):
            if self.browser:
                logging.info("[PW] Closing browser...")
                await self.browser.close()
        with suppress(Exception):
            if self._pw:
                logging.info("[PW] Stopping async_playwright...")
                await self._pw.stop()

    async def _extract_status_from_page(self, page) -> str:
        # Wait basic load
        with suppress(PlaywrightTimeoutError):
            logging.debug("[PW] Waiting for DOMContentLoaded (timeout=%sms)", self.timeout)
            await page.wait_for_load_state("domcontentloaded", timeout=self.timeout)
        # Anchor to the title and read the following bold text
        try:
            title = page.locator("css=div.content p.title-current-state").first
            logging.debug("[PW] Waiting title-current-state visible")
            await title.wait_for(state="visible", timeout=self.timeout)
            # Prefer CSS sibling combinator first, then XPath fallback
            value = title.locator("css=~ p.font-weight-600").first
            if not await value.is_visible(timeout=1000):
                value = title.locator("xpath=following-sibling::p[contains(@class,'font-weight-600')][1]")
            logging.debug("[PW] Waiting value (font-weight-600) visible")
            await value.wait_for(state="visible", timeout=self.timeout)
            txt = (await value.inner_text()).strip()
            if txt:
                logging.debug("[PW] Extracted status via primary locator: %s", txt)
                return txt
        except PlaywrightTimeoutError:
            pass
        # Alternative anchor: by text content of the title (class may vary)
        try:
            title_by_text = page.locator(
                "xpath=(//*[self::p or self::h1 or self::h2 or self::div][contains(normalize-space(.), 'Estado actual de tu envío')])[1]"
            )
            logging.debug("[PW] Waiting alternative title text visible")
            await title_by_text.wait_for(state="visible", timeout=min(6000, self.timeout))
            value = title_by_text.locator("css=~ p.font-weight-600").first
            if not await value.is_visible(timeout=1000):
                value = title_by_text.locator("xpath=following::p[contains(@class,'font-weight-600')][1]")
            logging.debug("[PW] Waiting value (alt) visible")
            await value.wait_for(state="visible", timeout=min(6000, self.timeout))
            txt = (await value.inner_text()).strip()
            if txt:
                logging.debug("[PW] Extracted status via alt locator: %s", txt)
                return txt
        except PlaywrightTimeoutError:
            pass
        # Direct CSS fallback within the same content card
        with suppress(Exception):
            value2 = page.locator("css=div.content p.font-weight-600").first
            logging.debug("[PW] Waiting fallback value visible")
            await value2.wait_for(state="visible", timeout=min(5000, self.timeout))
            txt2 = (await value2.inner_text()).strip()
            if txt2:
                logging.debug("[PW] Extracted status via fallback: %s", txt2)
                return txt2
        # Last resort: novelty pill
        with suppress(Exception):
            novelty = page.locator("css=p.guide-WhitOut-Novelty").first
            await novelty.wait_for(state="visible", timeout=min(3000, self.timeout))
            txt3 = (await novelty.inner_text()).strip()
            if txt3:
                return txt3
        return ""

    async def _extract_status_from_frame(self, frame) -> str:
        """Try to extract status text from an iframe frame.
        Attempts similar heuristics as page extraction but scoped to the frame.
        Prioritizes: title-current-state ~ p.font-weight-600, then p.font-weight-600, then p.guide-WhitOut-Novelty.
        """
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        with suppress(PlaywrightTimeoutError):
            await frame.wait_for_load_state("domcontentloaded", timeout=min(8000, self._timeout))
        # Primary: same pattern as page
        try:
            title = frame.locator("css=div.content p.title-current-state").first
            await title.wait_for(state="visible", timeout=min(6000, self._timeout))
            value = title.locator("css=~ p.font-weight-600").first
            if not await value.is_visible(timeout=1000):
                value = title.locator("xpath=following-sibling::p[contains(@class,'font-weight-600')][1]")
            await value.wait_for(state="visible", timeout=min(6000, self._timeout))
            txt = (await value.inner_text()).strip()
            if txt:
                return txt
        except PlaywrightTimeoutError:
            pass
        # Alt by text
        try:
            title_by_text = frame.locator(
                "xpath=(//*[self::p or self::h1 or self::h2 or self::div][contains(normalize-space(.), 'Estado actual')])[1]"
            )
            await title_by_text.wait_for(state="visible", timeout=min(6000, self._timeout))
            value = title_by_text.locator("css=~ p.font-weight-600").first
            if not await value.is_visible(timeout=1000):
                value = title_by_text.locator("xpath=following::p[contains(@class,'font-weight-600')][1]")
            await value.wait_for(state="visible", timeout=min(6000, self._timeout))
            txt = (await value.inner_text()).strip()
            if txt:
                return txt
        except PlaywrightTimeoutError:
            pass
        # Generic strong/bold text in the card
        with suppress(Exception):
            el = frame.locator("css=p.font-weight-600, strong, b").first
            await el.wait_for(state="visible", timeout=min(5000, self._timeout))
            txt = (await el.inner_text()).strip()
            if txt:
                return txt
        # Novelty pill
        with suppress(Exception):
            novelty = frame.locator("css=p.guide-WhitOut-Novelty").first
            await novelty.wait_for(state="visible", timeout=min(3000, self._timeout))
            txt3 = (await novelty.inner_text()).strip()
            if txt3:
                return txt3
        return ""

    async def get_status(self, tracking_number: str) -> str:
        context = None
        page = None
        popup = None
        try:
            # New context per guide
            ua = (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
            ctx_opts = {
                "user_agent": ua,
                "locale": "es-ES",
                "timezone_id": "America/Bogota",
                "extra_http_headers": {"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
            }
            if self.headless:
                logging.debug("[PW] Creating new context (headless) for %s", tracking_number)
                ctx_opts["viewport"] = {"width": 1280, "height": 800}
            else:
                logging.debug("[PW] Creating new context (headed) for %s", tracking_number)
                ctx_opts["viewport"] = None
            context = await self.browser.new_context(**ctx_opts)

            # Hide webdriver property to reduce bot detection
            with suppress(Exception):
                await context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )

            # Debug hooks: capture console and network events when enabled
            console_events: list[dict] = []
            network_events: list[dict] = []
            if self.debug:
                try:
                    from datetime import datetime
                    def _ts():
                        return datetime.utcnow().isoformat() + "Z"

                    def _on_console(msg):
                        try:
                            console_events.append({
                                "t": _ts(),
                                "type": msg.type,
                                "text": msg.text,
                                "page": getattr(msg.page, 'url', lambda: None)() if getattr(msg, 'page', None) else None,
                            })
                        except Exception:
                            pass

                    async def _on_request_finished(req):
                        try:
                            rtype = req.resource_type
                            if rtype in {"xhr", "fetch"}:
                                status = None
                                with suppress(Exception):
                                    resp = await req.response()
                                    status = resp.status if resp else None
                                network_events.append({
                                    "t": _ts(),
                                    "url": req.url,
                                    "status": status,
                                    "rtype": rtype,
                                })
                        except Exception:
                            pass

                    context.on("console", _on_console)
                    context.on("requestfinished", lambda req: asyncio.create_task(_on_request_finished(req)))
                except Exception:
                    pass

            # Block heavy resources to speed up
            if self.block_resources:
                async def _route_handler(route):
                    try:
                        if route.request.resource_type in {"image", "media", "font", "stylesheet"}:
                            await route.abort()
                        else:
                            await route.continue_()
                    except Exception:
                        with suppress(Exception):
                            await route.continue_()
                logging.debug("[PW] Installing route handler (resource blocking)")
                await context.route("**/*", _route_handler)

            logging.info("[PW] [%-14s] New page", tracking_number)
            page = await context.new_page()
            logging.debug("[PW] [%s] Navigating to tracking page", tracking_number)
            url = "https://interrapidisimo.com/sigue-tu-envio/"
            # Robust navigation with small retries to mitigate net::ERR_ABORTED/anti-bot redirects
            nav_ok = False
            for attempt_nav in range(3):
                try:
                    await page.goto(url, timeout=max(45000, self._timeout), wait_until="domcontentloaded")
                    nav_ok = True
                    break
                except Exception as nav_err:
                    logging.warning("[PW] [%s] nav attempt %d failed: %s", tracking_number, attempt_nav + 1, nav_err)
                    if attempt_nav < 2:
                        await asyncio.sleep(1.5 * (attempt_nav + 1))
                        continue
                    else:
                        raise
            if not nav_ok:
                raise RuntimeError("navigation_failed")
            with suppress(Exception):
                logging.debug("[PW] [%s] Landed URL: %s", tracking_number, page.url)

            # Try to accept cookie banners quickly
            with suppress(Exception):
                btn = page.get_by_role("button", name=lambda n: n and ("acept" in n.lower() or "de acuerdo" in n.lower() or "entendido" in n.lower()))
                await btn.click(timeout=2000)
                logging.debug("[PW] [%s] Cookie banner clicked", tracking_number)

            # Find the visible input (desktop/mobile)
            input_css = "#inputGuide:visible, #inputGuideMovil:visible, input.buscarGuiaInput:visible"
            loc = page.locator(input_css).first
            logging.debug("[PW] [%s] Waiting for input visible", tracking_number)
            await loc.wait_for(state="visible", timeout=self._timeout)
            await loc.scroll_into_view_if_needed()
            with suppress(Exception):
                await loc.fill("")
            await loc.fill(tracking_number)
            logging.debug("[PW] [%s] Tracking typed", tracking_number)

            # Trigger search: press Enter and also try clicking known buttons (site updated behavior)
            popup = None
            triggered = False
            try:
                logging.debug("[PW] [%s] Expecting popup on Enter", tracking_number)
                async with context.expect_page(timeout=2000) as new_page_info:
                    await loc.press("Enter")
                popup = await new_page_info.value
                with suppress(Exception):
                    await popup.bring_to_front()
                triggered = True
                logging.debug("[PW] [%s] Popup opened (Enter)", tracking_number)
            except PlaywrightTimeoutError:
                popup = None
            # If no popup, try clicking visible buttons by id/class used on site
            if not triggered:
                buttons_sel = "#BtnGuide:visible, #BtnR:visible, #BtnMovilGuide:visible, #BtnRMovil:visible, #buscarGuia:visible, button.buscarGuia:visible"
                btn = page.locator(buttons_sel).first
                with suppress(Exception):
                    await btn.wait_for(state="visible", timeout=3000)
                    await btn.scroll_into_view_if_needed()
                    await btn.click()
                    triggered = True

            # After triggering, either a popup opens or an iframe is injected in the same page
            target = popup if popup is not None else page

            # Preferred wait: iframe LOCATOR with content already mounted, then inner status locator
            result = ""
            try:
                iframe_fl = target.frame_locator(
                    "iframe.iframe-sigue-tu-envio, iframe[src*='SiguetuEnvio'], iframe[src*='Shipment']"
                ).first
                status_inside = iframe_fl.locator(
                    "css=div.content p.title-current-state ~ p.font-weight-600, "
                    "p.font-weight-600, p.guide-WhitOut-Novelty"
                ).first
                logging.debug("[PW] [%s] Waiting status inside iframe (preferred)", tracking_number)
                await status_inside.wait_for(state="visible", timeout=self._timeout)
                txt = (await status_inside.inner_text()).strip()
                if txt:
                    result = txt
            except Exception:
                result = ""

            # Fallbacks: try classic frame handle extraction or on-page extraction
            if not result:
                iframe = None
                with suppress(Exception):
                    # First try by class
                    el = await target.locator("iframe.iframe-sigue-tu-envio").first.element_handle(timeout=4000)
                    if el:
                        iframe = await el.content_frame()
                if iframe is None:
                    with suppress(Exception):
                        el = await target.locator("iframe[src*='SiguetuEnvio'], iframe[src*='Shipment']").first.element_handle(timeout=4000)
                        if el:
                            iframe = await el.content_frame()
                if iframe is not None:
                    with suppress(Exception):
                        result = await self._extract_status_from_frame(iframe)
                if not result:
                    result = await self._extract_status_from_page(target)
            logging.info("[PW] [%-14s] Status: %s", tracking_number, result or "<empty>")
            if not result:
                # Capture page state if extraction yielded empty
                with suppress(Exception):
                    await self._dump_debug(target, tracking_number, reason="empty")
                with suppress(Exception):
                    if iframe is not None:
                        # Dump iframe HTML too for diagnosis
                        from datetime import datetime
                        import os
                        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                        logs_dir = os.path.join(os.getcwd(), "logs")
                        os.makedirs(logs_dir, exist_ok=True)
                        base = os.path.join(logs_dir, f"debug_{tracking_number}_{ts}_iframe")
                        html = await iframe.content()
                        with open(base + ".html", "w", encoding="utf-8") as f:
                            f.write(html)
                # Dump console/network journals when debug enabled
                if self.debug:
                    with suppress(Exception):
                        from datetime import datetime
                        import json, os
                        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                        logs_dir = os.path.join(os.getcwd(), "logs")
                        os.makedirs(logs_dir, exist_ok=True)
                        base = os.path.join(logs_dir, f"debug_{tracking_number}_{ts}")
                        with open(base + "_console.ndjson", "w", encoding="utf-8") as f:
                            for ev in console_events:
                                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                        with open(base + "_network.ndjson", "w", encoding="utf-8") as f:
                            for ev in network_events:
                                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            return result
        except Exception as e:
            logging.error("[PW] Error for %s: %s", tracking_number, e)
            # Dump debug artifacts if possible
            with suppress(Exception):
                target = popup if popup is not None else (page if page is not None else None)
                if target is not None:
                    await self._dump_debug(target, tracking_number, reason="exception")
            return ""
        finally:
            with suppress(Exception):
                if popup:
                    await popup.close()
            with suppress(Exception):
                if page:
                    await page.close()
            with suppress(Exception):
                if context:
                    await context.close()

    async def get_status_many(self, tracking_numbers: Iterable[str], rps: float | None = None) -> List[Tuple[str, str]]:
        results: List[Tuple[str, str]] = []

        async def worker(tn: str):
            async with self._sem:
                # Retries with backoff
                delay = 0.75
                for attempt in range(self._retries + 1):
                    logging.info("[PW] [%-14s] Attempt %d", tn, attempt + 1)
                    status = await self.get_status(tn)
                    if status:
                        results.append((tn, status))
                        logging.info("[PW] [%-14s] Done in %d attempts", tn, attempt + 1)
                        break
                    if attempt < self._retries:
                        logging.debug("[PW] [%-14s] Empty, retrying after %.2fs", tn, delay)
                        await asyncio.sleep(delay)
                        delay *= 2
                else:
                    # After retries, record empty string to keep row mapping intact
                    results.append((tn, ""))
                    logging.info("[PW] [%-14s] Empty after retries", tn)
        tasks = []
        # Snapshot first to avoid consuming generators twice
        tn_list = list(tracking_numbers)
        if rps and rps > 0:
            interval = 1.0 / float(rps)
            start = asyncio.get_event_loop().time()
            logging.info("[PW] Scheduling %d tasks with RPS=%.2f (interval=%.3fs)", len(tn_list), rps, interval)
            for i, tn in enumerate(tn_list):
                # Stagger task starts to respect RPS
                async def delayed_launch(tn=tn, i=i):
                    target_time = start + i * interval
                    now = asyncio.get_event_loop().time()
                    if target_time > now:
                        await asyncio.sleep(target_time - now)
                    await worker(tn)
                tasks.append(asyncio.create_task(delayed_launch()))
        else:
            logging.info("[PW] Launching %d tasks immediately (no RPS throttling)", len(tn_list))
            tasks = [asyncio.create_task(worker(tn)) for tn in tn_list]

        await asyncio.gather(*tasks)
        return results

    async def _dump_debug(self, page, tracking_number: str, reason: str = ""):
        """Dump HTML and screenshot to logs/ for troubleshooting."""
        try:
            from datetime import datetime
            import os
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            logs_dir = os.path.join(os.getcwd(), "logs")
            os.makedirs(logs_dir, exist_ok=True)
            base = os.path.join(logs_dir, f"debug_{tracking_number}_{ts}_{reason}")
            # HTML
            html_path = base + ".html"
            content = await page.content()
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(content)
            # Screenshot
            png_path = base + ".png"
            await page.screenshot(path=png_path, full_page=True)
        except Exception:
            pass
