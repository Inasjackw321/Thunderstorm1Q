// Thunderstorm1Q — 3-day T1 / W1 / H1 hazard risk map.
// Reads data/day{1,2,3}.json written by scripts/update_day*.py every 12h.
//   day 1 = 24 hourly frames (HRRR-auto)
//   day 2 = 4 × 6h-max windows (GFS, ×0.85 skill)
//   day 3 = 4 × 6h-max windows (GFS, ×0.70 skill)
// Each frame ships three hazard slots — tornado / wind / hail — and
// the user-selectable Hazard pill (T1 / W1 / H1) decides which the
// heatmap currently renders.
(function () {
  'use strict';

  // Heat color gradient — same palette for every hazard, the absolute
  // probability scale changes via HAZARD_SCALE so a 25 % wind day
  // doesn't look as red as a 25 % tornado day.
  const GRADIENT = {
    0.00: '#22d3ee', 0.22: '#a3e635', 0.40: '#facc15',
    0.56: '#fb923c', 0.72: '#ef4444', 0.88: '#db2777',
    1.00: '#4c1d95',
  };
  const HEAT_RADIUS = 38;
  const HEAT_BLUR = 30;
  const SIGNAL_MIN = 0.12;

  // Per-hazard "what % counts as the top of the color ramp". Tornado
  // is rare and saturates the color palette much earlier than wind.
  const HAZARD_SCALE = { tornado: 0.45, wind: 0.65, hail: 0.55 };
  const HAZARD_LABEL = { tornado: 'T1',  wind: 'W1',  hail: 'H1' };

  const POLL_MS = 5 * 60 * 1000;
  const STALE_MIN = 840;       // 12h cron + 2h grace period; anything older is "stale"

  const DAYS = [
    { n: 1, label: '1-24h', bars: 24, fhPrefix: 'F' },
    { n: 2, label: 'Day 2', bars:  4, fhPrefix: 'W' },
    { n: 3, label: 'Day 3', bars:  4, fhPrefix: 'W' },
  ];

  const spark     = document.getElementById('spark');
  const sparkFill = document.getElementById('spark-fill');
  const sparkTicks= document.getElementById('spark-ticks');
  const sparkHead = document.getElementById('spark-head');
  const sparkTip  = document.getElementById('spark-tip');
  const tipFh     = document.getElementById('tip-fh');
  const tipPct    = document.getElementById('tip-pct');
  const roPct     = document.getElementById('ro-pct');
  const roTime    = document.getElementById('ro-time');
  const roStatus  = document.getElementById('ro-status');
  const roStatTxt = document.getElementById('ro-status-text');
  const banner    = document.getElementById('state-banner');
  const pillsEl   = document.getElementById('day-pills');
  const pillEls   = Array.from(pillsEl.querySelectorAll('.pill'));
  const mapTip    = document.getElementById('map-tip');
  const mapTipPct = document.getElementById('map-tip-pct');
  const mapTipCrd = document.getElementById('map-tip-coord');
  const locateBtn  = document.getElementById('locate-btn');
  const searchBtn  = document.getElementById('search-btn');
  const searchPop  = document.getElementById('search-pop');
  const searchInput= document.getElementById('search-input');
  const searchGo   = document.getElementById('search-go');
  const locateErr  = document.getElementById('locate-error');
  const locateErrBody = document.getElementById('locate-error-body');
  const locateErrOk = document.getElementById('locate-error-ok');
  const hazardEls  = Array.from(document.querySelectorAll('.haz'));
  const pointPanel = document.getElementById('point-panel');
  const ppTitle    = document.getElementById('pp-title');
  const ppCoord    = document.getElementById('pp-coord');
  const ppBody     = document.getElementById('pp-body');
  const ppClose    = document.getElementById('pp-close');

  // CONUS bounds — same box the model is gridded over.
  const CONUS = { s: 24.5, w: -125.0, n: 49.5, e: -66.5 };

  // Per-day datasets / metas, keyed 1..3. `null` until the first load.
  const datasets = { 1: null, 2: null, 3: null };
  const metas    = { 1: null, 2: null, 3: null };

  let currentDay = 1;
  let currentHazard = 'tornado';   // 'tornado' | 'wind' | 'hail'
  let currentFh = 1;
  let totalFrames = DAYS[0].bars;
  let heatLayer = null;
  let tickEls   = [];   // current .tick-label children of #spark-ticks

  // Leaflet: locked map.
  const map = L.map('map', {
    center: [38.5, -97.5],
    zoom: 5,
    minZoom: 5, maxZoom: 5,
    maxBounds: [[22.5, -128.0], [51.5, -64.0]],
    maxBoundsViscosity: 1.0,
    zoomControl: false,
    dragging: false,
    scrollWheelZoom: false,
    doubleClickZoom: false,
    boxZoom: false,
    keyboard: false,
    touchZoom: false,
    tap: false,
    preferCanvas: true,
    worldCopyJump: false,
    attributionControl: true,
    zoomSnap: 0,
  });
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO &middot; T1',
    maxZoom: 18, subdomains: 'abcd',
  }).addTo(map);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png', {
    pane: 'shadowPane', maxZoom: 18, subdomains: 'abcd',
  }).addTo(map);

  // Tuck the attribution into the bottom-left corner so it isn't
  // hidden behind the centered timeline pill.
  if (map.attributionControl) map.attributionControl.setPosition('bottomleft');


  spark.addEventListener('keydown', onSparkKey);
  spark.addEventListener('pointerdown', onSparkPointerDown);
  spark.addEventListener('pointermove', onSparkHover);
  spark.addEventListener('pointerleave', hideTip);
  pillEls.forEach(el => el.addEventListener('click', () => {
    const n = Number(el.dataset.day);
    if (n && n !== currentDay) switchDay(n);
  }));
  window.addEventListener('keydown', onKey);

  map.on('mousemove', onMapMove);
  map.on('mouseout',  hideMapTip);

  hazardEls.forEach(el => el.addEventListener('click', () => {
    const haz = el.dataset.hazard;
    if (haz && haz !== currentHazard) switchHazard(haz);
  }));

  // ---------- sparkline ----------

  // Palette stops keyed to normalised probability (v / HAZARD_SCALE).
  // Matches the leaflet-heat GRADIENT.
  const STOPS = [
    [0.00, [0x22, 0xd3, 0xee]],
    [0.22, [0xa3, 0xe6, 0x35]],
    [0.40, [0xfa, 0xcc, 0x15]],
    [0.56, [0xfb, 0x92, 0x3c]],
    [0.72, [0xef, 0x44, 0x44]],
    [0.88, [0xdb, 0x27, 0x77]],
    [1.00, [0x4c, 0x1d, 0x95]],
  ];
  const DIM = [0x5b, 0x6f, 0x99];

  function lerp(a, b, t) { return a + (b - a) * t; }
  function colorAt(t) {
    if (t <= 0) return STOPS[0][1];
    if (t >= 1) return STOPS[STOPS.length - 1][1];
    for (let i = 1; i < STOPS.length; i++) {
      const [s1, c1] = STOPS[i - 1];
      const [s2, c2] = STOPS[i];
      if (t <= s2) {
        const k = (t - s1) / (s2 - s1);
        return [lerp(c1[0], c2[0], k), lerp(c1[1], c2[1], k), lerp(c1[2], c2[2], k)];
      }
    }
    return STOPS[STOPS.length - 1][1];
  }
  function rgb(c, a) {
    const r = Math.round(c[0]), g = Math.round(c[1]), b = Math.round(c[2]);
    return a == null ? `rgb(${r},${g},${b})` : `rgba(${r},${g},${b},${a})`;
  }
  function darker(c, k) { return [c[0] * k, c[1] * k, c[2] * k]; }

  // Pick which fhs to render as tick labels for a given frame count.
  // Day 1 (n=24) shows quarter-points without endpoints to avoid
  // colliding with the knob at the rail's edges; day 2 / day 3
  // (n=4) label every frame as W1..W4.
  function tickFhs(n, prefix) {
    if (n === 4)  return [1, 2, 3, 4];
    if (n === 24) return [6, 12, 18];
    // generic: every (n / 4) starting at floor(n/4), skip endpoints.
    const step = Math.max(1, Math.round(n / 4));
    const out = [];
    for (let f = step; f < n; f += step) out.push(f);
    return out;
  }

  function buildSpark(n) {
    sparkTicks.innerHTML = '';
    tickEls = [];
    const prefix = (DAYS[currentDay - 1] || DAYS[0]).fhPrefix;
    for (const fh of tickFhs(n, prefix)) {
      const b = document.createElement('button');
      b.type = 'button';
      b.className = 'tick-label';
      b.dataset.fh = String(fh);
      b.textContent = prefix + String(fh).padStart(2, '0');
      // Position label at (fh-1)/(n-1) along the rail.
      const x = n <= 1 ? 0 : (fh - 1) / (n - 1);
      b.style.left = (x * 100).toFixed(2) + '%';
      sparkTicks.appendChild(b);
      tickEls.push(b);
    }
    spark.setAttribute('aria-valuemin', '1');
    spark.setAttribute('aria-valuemax', String(n));
  }

  // Single-frame visuals: width and color of the heat-fill plus the
  // .past flag on each tick label. The knob position is owned by
  // updatePlayhead() so dragging stays smooth.
  function paintSpark() {
    const d = datasets[currentDay];
    const hours = (d && d.hours) || [];
    const rec = hours.find(h => h.fh === currentFh) || hours[currentFh - 1];
    const mx  = rec ? (tornadoRec(rec).max || 0) : 0;
    const t   = Math.max(0, Math.min(1, mx / HAZARD_SCALE[currentHazard]));

    if (sparkFill) {
      const x = totalFrames <= 1 ? 0 : (currentFh - 1) / (totalFrames - 1);
      sparkFill.style.setProperty('--x', x.toFixed(4));
      if (mx >= SIGNAL_MIN) {
        const c = colorAt(t);
        sparkFill.style.setProperty('--c1', rgb(c, 0.55));
        sparkFill.style.setProperty('--c2', rgb(c));
      } else {
        sparkFill.style.setProperty('--c1', rgb(DIM, 0.40));
        sparkFill.style.setProperty('--c2', rgb(DIM, 0.55));
      }
    }

    tickEls.forEach((el) => {
      const fh = Number(el.dataset.fh);
      el.classList.toggle('past', fh < currentFh);
    });

    spark.setAttribute('aria-valuenow', String(currentFh));
    updatePlayhead();
  }

  buildSpark(totalFrames);
  loadAll();
  setInterval(pollForUpdate, POLL_MS);

  // Click a tick label to jump straight to that frame.
  if (sparkTicks) {
    sparkTicks.addEventListener('click', (e) => {
      const el = e.target.closest('.tick-label');
      if (!el || !el.dataset.fh) return;
      e.stopPropagation();
      setFrame(Number(el.dataset.fh));
      spark.focus({ preventScroll: true });
    });
  }

  function updatePlayhead() {
    if (!sparkHead || totalFrames < 1) return;
    const x = totalFrames <= 1 ? 0 : (currentFh - 1) / (totalFrames - 1);
    sparkHead.style.setProperty('--x', x.toFixed(4));
  }

  // ---------- scrub / hover ----------

  function fhFromClientX(clientX) {
    // Padding mirrors the track padding in CSS so cursor-x maps
    // 1:1 to the visible portion of the rail. The mobile media
    // query uses 8 px instead of 10 px — close enough that the
    // mid-track frame-pick is unaffected.
    const rect = spark.getBoundingClientRect();
    const pad = 10;
    const usable = Math.max(1, rect.width - pad * 2);
    const frac = Math.max(0, Math.min(1, (clientX - rect.left - pad) / usable));
    return Math.max(1, Math.min(totalFrames, Math.round(frac * (totalFrames - 1)) + 1));
  }
  function onSparkPointerDown(e) {
    if (e.button !== undefined && e.button !== 0) return;
    spark.classList.add('dragging');
    try { spark.setPointerCapture(e.pointerId); } catch (_) {}
    setFrame(fhFromClientX(e.clientX));
    showTipFor(currentFh, e.clientX);
    const move = (ev) => {
      setFrame(fhFromClientX(ev.clientX));
      showTipFor(currentFh, ev.clientX);
    };
    const up = (ev) => {
      spark.classList.remove('dragging');
      try { spark.releasePointerCapture(e.pointerId); } catch (_) {}
      spark.removeEventListener('pointermove', move);
      spark.removeEventListener('pointerup', up);
      spark.removeEventListener('pointercancel', up);
    };
    spark.addEventListener('pointermove', move);
    spark.addEventListener('pointerup', up);
    spark.addEventListener('pointercancel', up);
    spark.focus({ preventScroll: true });
  }
  function onSparkHover(e) {
    if (spark.classList.contains('dragging')) return;
    showTipFor(fhFromClientX(e.clientX), e.clientX);
  }
  function showTipFor(fh, clientX) {
    if (!sparkTip) return;
    const d = datasets[currentDay];
    const hours = (d && d.hours) || [];
    const rec = hours.find(h => h.fh === fh) || hours[fh - 1];
    const mx = rec ? (tornadoRec(rec).max || 0) : 0;
    const prefix = DAYS[currentDay - 1].fhPrefix;
    tipFh.textContent  = prefix + String(fh).padStart(2, '0');
    tipPct.textContent = (mx * 100).toFixed(0) + '%';
    const x = totalFrames <= 1 ? 0 : (fh - 1) / (totalFrames - 1);
    sparkTip.style.setProperty('--x', x.toFixed(4));
    sparkTip.classList.add('show');
  }
  function hideTip() { if (sparkTip) sparkTip.classList.remove('show'); }

  // ---------- data ----------

  async function loadAll() {
    const loads = DAYS.map(d => Promise.all([
      fetchJSON(`data/day${d.n}.json`),
      fetchJSON(`data/day${d.n}.meta.json`),
    ]).then(([j, m]) => { datasets[d.n] = j; metas[d.n] = m; })
      .catch(() => { /* individual day failures are tolerated */ }));

    await Promise.allSettled(loads);

    if (!datasets[currentDay]) {
      setStatus('error', 'error');
      showBanner('error', 'Could not load forecast data.');
      return;
    }
    renderAll();
  }

  async function pollForUpdate() {
    const checks = DAYS.map(async (d) => {
      try {
        const m2 = await fetchJSON(`data/day${d.n}.meta.json`);
        const prev = metas[d.n];
        if (!prev || m2.generated_at !== prev.generated_at) {
          const j2 = await fetchJSON(`data/day${d.n}.json`);
          datasets[d.n] = j2;
          metas[d.n] = m2;
          return true;
        }
      } catch (_) { /* transient */ }
      return false;
    });
    const results = await Promise.all(checks);
    if (results.some(Boolean)) {
      renderAll(/*keepFrame*/ true);
    } else {
      updateBanner();
    }
  }

  function fetchJSON(url) {
    return fetch(url, { cache: 'no-store' }).then(r => {
      if (!r.ok) throw new Error(url + ' -> ' + r.status);
      return r.json();
    });
  }

  // ---------- rendering ----------

  function switchDay(n) {
    if (!DAYS.find(d => d.n === n)) return;
    currentDay = n;
    pillEls.forEach(el => {
      el.setAttribute('aria-selected',
        String(Number(el.dataset.day) === n));
    });
    renderAll(/*keepFrame*/ false);
  }

  function switchHazard(haz) {
    if (!HAZARD_SCALE[haz]) return;
    currentHazard = haz;
    hazardEls.forEach(el => {
      el.setAttribute('aria-selected',
        String(el.dataset.hazard === haz));
    });
    // Re-render the map and the inline status with the new hazard's
    // numbers. Keep the current frame so scrubbing position survives
    // the toggle.
    renderAll(/*keepFrame*/ true);
    // If the location panel is open, refresh its 12-hour table too.
    if (pointPanel && !pointPanel.hidden && lastPoint) {
      paintPointPanel();
    }
  }

  function renderAll(keepFrame) {
    const d = datasets[currentDay];
    const spec = DAYS[currentDay - 1];
    const hours = (d && d.hours) || [];
    const n = Math.max(1, hours.length || spec.bars);
    if (n !== totalFrames) {
      totalFrames = n;
      buildSpark(n);
    }
    if (!keepFrame) {
      const m = metas[currentDay];
      const peakObj = (m && m.peaks && m.peaks.tornado) ||
                      (m && m.peak) || null;
      currentFh = (peakObj && peakObj.fh) ? peakObj.fh : 1;
    }
    if (currentFh > totalFrames) currentFh = totalFrames;
    if (currentFh < 1) currentFh = 1;
    updateBanner();
    renderHour(currentFh);
  }

  // Hazard-aware accessor: returns {cells, max} for the currently
  // selected hazard. Falls back to legacy {cells, max} at the rec's
  // root if the JSON pre-dates the multi-hazard schema, or to the
  // tornado slot if a specific frame is missing the requested hazard.
  function hazardRec(rec, hazard) {
    if (!rec) return { cells: [], max: 0 };
    const want = hazard || currentHazard;
    if (rec[want]) return rec[want];
    if (rec.tornado) return rec.tornado;
    return { cells: rec.cells || [], max: rec.max || 0 };
  }
  // Legacy alias kept so any forgotten reference still works.
  const tornadoRec = (rec) => hazardRec(rec, currentHazard);

  function setFrame(fh) {
    fh = Math.max(1, Math.min(totalFrames, fh));
    if (fh === currentFh) return;
    currentFh = fh;
    renderHour(fh);
  }

  function renderHour(fh) {
    const d = datasets[currentDay];
    const hours = (d && d.hours) || [];
    const rec = hours.find(h => h.fh === fh) || hours[fh - 1];

    const h = tornadoRec(rec);
    const pts = (h.cells || []).map(c => [c[0], c[1], c[2]]);
    if (heatLayer) map.removeLayer(heatLayer);
    heatLayer = L.heatLayer(pts, {
      radius: HEAT_RADIUS, blur: HEAT_BLUR, maxZoom: 9,
      max: HAZARD_SCALE[currentHazard], gradient: GRADIENT,
    }).addTo(map);

    paintSpark();

    const mx = typeof h.max === 'number' ? h.max : 0;
    const validIso = rec && rec.valid;

    setLabel(roPct,  (mx * 100).toFixed(0) + '%');
    setLabel(roTime, fmtDate(validIso) + ' · ' + fmtHour(validIso));
    tintReadout(mx);
  }

  // ---------- map cursor probe ----------

  // Look up the per-cell tornado probability at (lat, lon) for the
  // currently displayed day + frame. Cells in data/day*.json are
  // sparse (anything below SCORE_FLOOR is dropped) so a cell-not-found
  // means "below the model's reporting threshold", not "no data".
  function valueAtLatLon(lat, lon) {
    const d = datasets[currentDay];
    if (!d) return null;
    const hours = d.hours || [];
    const rec = hours.find(h => h.fh === currentFh) || hours[currentFh - 1];
    if (!rec) return null;
    const cells = tornadoRec(rec).cells || [];
    const grid = d.grid_deg || 1.5;
    const half = grid * 0.55;
    for (let i = 0; i < cells.length; i++) {
      const c = cells[i];
      if (Math.abs(c[0] - lat) <= half && Math.abs(c[1] - lon) <= half) {
        return c[2];
      }
    }
    return 0;
  }

  function fmtLatLon(lat, lon) {
    const ns = lat >= 0 ? 'N' : 'S';
    const ew = lon >= 0 ? 'E' : 'W';
    return `${Math.abs(lat).toFixed(1)}°${ns}  ${Math.abs(lon).toFixed(1)}°${ew}`;
  }

  function onMapMove(e) {
    if (!mapTip) return;
    const v = valueAtLatLon(e.latlng.lat, e.latlng.lng);
    if (v == null) { hideMapTip(); return; }
    mapTipPct.textContent = v > 0 ? (v * 100).toFixed(0) + '%' : '<2%';
    if (mapTipCrd) mapTipCrd.textContent = fmtLatLon(e.latlng.lat, e.latlng.lng);
    const cp = e.containerPoint;
    mapTip.style.transform = `translate(${cp.x + 14}px, ${cp.y + 14}px)`;
    mapTip.classList.add('show');
  }
  function hideMapTip() {
    if (mapTip) mapTip.classList.remove('show');
  }

  // ---------- chosen point (GPS or typed location) ----------

  // The user can pick a point either via the GPS button or by typing
  // a city/zip/lat-lon into the search popover. Either way we render
  // a small home marker; clicking it opens the side panel with a
  // 12-hour T1/W1/H1 table for that grid cell.
  let locateMarker = null;
  let locateRing   = null;
  let lastPoint    = null;   // { lat, lon, label }

  function inCONUS(lat, lon) {
    return lat >= CONUS.s && lat <= CONUS.n &&
           lon >= CONUS.w && lon <= CONUS.e;
  }
  function clearLocate() {
    if (locateMarker) { map.removeLayer(locateMarker); locateMarker = null; }
    if (locateRing)   { map.removeLayer(locateRing);   locateRing = null; }
  }

  // SVG-backed home icon for the picked location. Built as a Leaflet
  // divIcon so the marker is just an absolutely-positioned DOM node
  // we can style and click — no Leaflet image hosting needed.
  function homeDivIcon() {
    const html =
      '<div class="home-marker" title="Click for forecast">' +
      '<svg viewBox="0 0 24 26" width="22" height="24" aria-hidden="true">' +
      '<path class="home-fill" d="M12 1 L1 11 L4 11 L4 24 L10 24 L10 16 L14 16 L14 24 L20 24 L20 11 L23 11 Z"/>' +
      '<rect class="home-roof" x="6" y="11" width="12" height="2"/>' +
      '</svg></div>';
    return L.divIcon({
      className: 'home-divicon',
      html, iconSize: [22, 24], iconAnchor: [11, 22],
    });
  }

  function dropLocateMarker(lat, lon, label) {
    clearLocate();
    locateRing = L.circle([lat, lon], {
      radius: 35000,
      color: 'rgba(167, 139, 250, 0.65)',
      fillColor: 'rgba(167, 139, 250, 0.10)',
      fillOpacity: 1,
      weight: 1,
    }).addTo(map);
    locateMarker = L.marker([lat, lon], {
      icon: homeDivIcon(),
      keyboard: true,
      title: label || 'Forecast for this point',
    }).addTo(map);
    locateMarker.on('click', openPointPanel);
    lastPoint = { lat, lon, label: label || null };
    openPointPanel();
  }
  function showLocateError(title, body) {
    if (!locateErr) return;
    if (locateErrBody) locateErrBody.textContent = body;
    const titleEl = document.getElementById('locate-error-title');
    if (titleEl && title) titleEl.textContent = title;
    locateErr.hidden = false;
    requestAnimationFrame(() => locateErr.classList.add('show'));
    if (locateErrOk) locateErrOk.focus();
  }
  function hideLocateError() {
    if (!locateErr) return;
    locateErr.classList.remove('show');
    setTimeout(() => { locateErr.hidden = true; }, 180);
  }
  if (locateErrOk) locateErrOk.addEventListener('click', hideLocateError);
  if (locateErr) {
    locateErr.addEventListener('click', (e) => {
      if (e.target === locateErr) hideLocateError();
    });
  }
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && locateErr && !locateErr.hidden) hideLocateError();
  });

  function locate() {
    if (!('geolocation' in navigator)) {
      showLocateError('Location unavailable',
        'Your browser does not support geolocation.');
      return;
    }
    if (locateBtn) locateBtn.classList.add('loading');
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        if (locateBtn) locateBtn.classList.remove('loading');
        const lat = pos.coords.latitude;
        const lon = pos.coords.longitude;
        if (!inCONUS(lat, lon)) {
          clearLocate();
          showLocateError('Outside CONUS',
            'Your location is outside the contiguous United States, so '
            + 'no marker was added. T1 only forecasts the CONUS domain.');
          return;
        }
        dropLocateMarker(lat, lon, 'My location');
      },
      (err) => {
        if (locateBtn) locateBtn.classList.remove('loading');
        const msg = err && err.code === err.PERMISSION_DENIED
          ? 'Location permission was denied. Allow location access in '
            + 'your browser settings to use this feature.'
          : 'Could not get your location. Try again in a moment.';
        showLocateError('Location unavailable', msg);
      },
      { enableHighAccuracy: false, timeout: 8000, maximumAge: 60000 });
  }
  if (locateBtn) locateBtn.addEventListener('click', locate);

  // ---------- search by name or coordinates ----------

  // Toggle the search popover open/closed.
  function openSearch() {
    if (!searchPop) return;
    searchPop.hidden = false;
    setTimeout(() => searchInput && searchInput.focus(), 30);
  }
  function closeSearch() {
    if (!searchPop) return;
    searchPop.hidden = true;
  }
  if (searchBtn) searchBtn.addEventListener('click', () => {
    if (searchPop.hidden) openSearch(); else closeSearch();
  });

  // Detect a "lat,lon" or "lat lon" input and short-circuit the
  // geocode call. Accepts e.g. "38.5,-97.5", "38.5 -97.5", " 38.5N , 97.5W ".
  function parseLatLon(s) {
    const cleaned = s.trim()
      .replace(/[NSEW]/gi, m => (m.toUpperCase() === 'S' || m.toUpperCase() === 'W') ? '-' : '')
      .replace(/\s+/g, ',');
    const m = cleaned.match(/^(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)$/);
    if (!m) return null;
    const lat = parseFloat(m[1]);
    const lon = parseFloat(m[2]);
    if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return null;
    return { lat, lon, label: `${lat.toFixed(2)}, ${lon.toFixed(2)}` };
  }

  // Open-Meteo's free geocoding endpoint. Returns the first US match
  // when we can; falls back to whatever's first.
  async function geocode(query) {
    const url = 'https://geocoding-api.open-meteo.com/v1/search?'
      + 'count=5&language=en&format=json&name='
      + encodeURIComponent(query);
    const r = await fetch(url, { cache: 'no-store' });
    if (!r.ok) throw new Error('geocode HTTP ' + r.status);
    const json = await r.json();
    const results = (json && json.results) || [];
    if (!results.length) return null;
    const us = results.find(x => (x.country_code || '').toUpperCase() === 'US');
    const pick = us || results[0];
    const place = [pick.name, pick.admin1, pick.country_code]
      .filter(Boolean).join(', ');
    return { lat: pick.latitude, lon: pick.longitude, label: place };
  }

  async function runSearch() {
    if (!searchInput) return;
    const raw = searchInput.value || '';
    if (!raw.trim()) return;

    // Try lat/lon first to avoid an unnecessary network call.
    const direct = parseLatLon(raw);
    let pick = direct;
    if (!pick) {
      try {
        pick = await geocode(raw);
      } catch (_) { pick = null; }
    }
    if (!pick) {
      showLocateError('Not found',
        'Could not find a place matching that name. Try a city, '
        + 'a "City, ST" pair, a ZIP, or coordinates like 38.5,-97.5.');
      return;
    }
    if (!inCONUS(pick.lat, pick.lon)) {
      showLocateError('Outside CONUS',
        `${pick.label} is outside the contiguous United States, so `
        + 'no marker was added. T1 only forecasts the CONUS domain.');
      return;
    }
    dropLocateMarker(pick.lat, pick.lon, pick.label);
    closeSearch();
  }
  if (searchGo)    searchGo.addEventListener('click', runSearch);
  if (searchInput) searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter')  { e.preventDefault(); runSearch(); }
    if (e.key === 'Escape') { e.preventDefault(); closeSearch(); }
  });

  // ---------- forecast panel for the chosen point ----------

  // 12 hours of {fh, valid, percentages-per-hazard} for the cell
  // closest to (lat, lon) on day 1's grid.
  function pointForecast(lat, lon, hours) {
    const d = datasets[1];
    if (!d) return [];
    const grid = d.grid_deg || 1.5;
    const half = grid * 0.55;
    const out = [];
    const all = d.hours || [];
    for (let i = 0; i < Math.min(hours, all.length); i++) {
      const rec = all[i];
      const row = { fh: rec.fh, valid: rec.valid };
      for (const haz of ['tornado', 'wind', 'hail']) {
        const cells = (rec[haz] && rec[haz].cells) || [];
        let v = 0;
        for (let j = 0; j < cells.length; j++) {
          const c = cells[j];
          if (Math.abs(c[0] - lat) <= half && Math.abs(c[1] - lon) <= half) {
            v = c[2]; break;
          }
        }
        row[haz] = v;
      }
      out.push(row);
    }
    return out;
  }

  function fmtHourLabel(iso) {
    try {
      const d = new Date(iso);
      return d.toLocaleString('en-GB', {
        hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
        timeZone: 'UTC',
      }) + 'Z';
    } catch { return iso || '—'; }
  }

  function paintPointPanel() {
    if (!pointPanel || !lastPoint || !ppBody) return;
    const rows = pointForecast(lastPoint.lat, lastPoint.lon, 12);
    ppTitle.textContent = lastPoint.label || 'Selected point';
    ppCoord.textContent =
      `${lastPoint.lat.toFixed(2)}°N  ${lastPoint.lon.toFixed(2)}°E`
        .replace('-', '').replace('°N', lastPoint.lat < 0 ? '°S' : '°N')
        .replace('°E', lastPoint.lon < 0 ? '°W' : '°E');
    const fmt = (v) => v >= 0.01 ? (v * 100).toFixed(0) + '%' : '—';
    ppBody.innerHTML = rows.map(r => {
      return `<tr>
        <td>${fmtHourLabel(r.valid)}</td>
        <td>${fmt(r.tornado)}</td>
        <td>${fmt(r.wind)}</td>
        <td>${fmt(r.hail)}</td>
      </tr>`;
    }).join('');
  }

  function openPointPanel() {
    if (!pointPanel || !lastPoint) return;
    paintPointPanel();
    pointPanel.hidden = false;
  }
  function closePointPanel() {
    if (pointPanel) pointPanel.hidden = true;
  }
  if (ppClose) ppClose.addEventListener('click', closePointPanel);

  function tintReadout(mx) {
    // Tint the percentage with the heat gradient when we have any
    // signal at all, but skip the box-shadow glow — the colored
    // text alone reads cleaner against the dark panel.
    if (!roPct) return;
    if (mx < SIGNAL_MIN) {
      roPct.style.color = '';
      return;
    }
    const t = Math.max(0, Math.min(1, mx / HAZARD_SCALE[currentHazard]));
    roPct.style.color = rgb(colorAt(t));
  }

  // Soft crossfade on text so scrubbing / autoplay feels polished.
  function setLabel(el, text) {
    if (!el || el.textContent === text) return;
    el.classList.add('tick');
    requestAnimationFrame(() => {
      el.textContent = text;
      requestAnimationFrame(() => el.classList.remove('tick'));
    });
  }

  function setStatus(kind, text) {
    if (!roStatus) return;
    roStatus.classList.remove('live', 'stale', 'seed', 'error');
    roStatus.classList.add(kind);
    if (roStatTxt) roStatTxt.textContent = text;
  }

  // Status reflects the currently-selected day's meta so users see
  // per-day freshness. If Day 3 is stale but Day 1 is live, switching
  // pills flips the dot accordingly.
  function updateBanner() {
    const m = metas[currentDay];
    if (!m) { setStatus('error', 'offline'); return hideBanner(); }
    if (m.seeded) {
      setStatus('seed', 'seed');
      return showBanner('seed',
        'Awaiting first T1 run — showing an empty seed forecast.');
    }
    const gen = m.generated_at ? new Date(m.generated_at).getTime() : 0;
    const ageMin = (Date.now() - gen) / 60000;
    if (gen && ageMin > STALE_MIN) {
      setStatus('stale', 'stale');
      return showBanner('stale',
        'Last update ' + Math.round(ageMin) +
        ' min ago — showing cached forecast.');
    }
    setStatus('live', 'live');
    hideBanner();
  }

  function showBanner(kind, text) {
    banner.hidden = false;
    banner.className = 'banner ' + kind;
    banner.textContent = text;
  }
  function hideBanner() { banner.hidden = true; }

  // ---------- controls ----------

  function jumpToPeak() {
    const m = metas[currentDay];
    const peakObj = (m && m.peaks && m.peaks.tornado) ||
                    (m && m.peak) || null;
    if (!peakObj || !peakObj.fh) return;
    setFrame(peakObj.fh);
  }

  function onKey(e) {
    if (e.target && /^(INPUT|TEXTAREA)$/.test(e.target.tagName)) return;
    if (e.key === 'ArrowRight')      { setFrame(currentFh + 1); }
    else if (e.key === 'ArrowLeft')  { setFrame(currentFh - 1); }
    else if (e.key === 'p' || e.key === 'P') { jumpToPeak(); }
    else if (e.key === '1') { switchDay(1); }
    else if (e.key === '2') { switchDay(2); }
    else if (e.key === '3') { switchDay(3); }
    else if (e.key === 't' || e.key === 'T') { switchHazard('tornado'); }
    else if (e.key === 'w' || e.key === 'W') { switchHazard('wind'); }
    else if (e.key === 'h' || e.key === 'H') { switchHazard('hail'); }
  }
  function onSparkKey(e) {
    if (e.key === 'ArrowRight' || e.key === 'ArrowUp') {
      e.preventDefault(); setFrame(currentFh + 1);
    } else if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') {
      e.preventDefault(); setFrame(currentFh - 1);
    } else if (e.key === 'Home') {
      e.preventDefault(); setFrame(1);
    } else if (e.key === 'End') {
      e.preventDefault(); setFrame(totalFrames);
    }
  }

  // ---------- formatting (UTC / Zulu) ----------

  function fmtHour(iso) {
    try {
      return new Date(iso).toLocaleString('en-GB', {
        hour: '2-digit', minute: '2-digit',
        hourCycle: 'h23', timeZone: 'UTC',
      }) + 'Z';
    } catch { return iso || ''; }
  }
  function fmtDate(iso) {
    try {
      return new Date(iso).toLocaleString('en-US', {
        weekday: 'short', month: 'short', day: 'numeric',
        timeZone: 'UTC',
      });
    } catch { return iso || ''; }
  }
})();
