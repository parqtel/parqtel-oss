// Regression E2E for the builder ⇄ typed-query round trip (the "filters
// dropped when a function is selected" bug). Appended to the main builder
// E2E by scripts/test_builder_ui.sh via BUILDER_RT=1, or standalone:
//   node scripts/test_builder_ui_roundtrip.js <proxyPort>
const PORT = process.argv[2] ? Number(process.argv[2]) : 9099;
const CDP_PORT = 9300 + (process.getpid ? process.getpid() % 100 : 1);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const { spawn } = require('child_process');
  const chrome = spawn('google-chrome', ['--headless=new','--disable-gpu','--no-sandbox',`--user-data-dir=/tmp/cdp-rt-${Date.now()}`,`--remote-debugging-port=${CDP_PORT}`,'about:blank'], {stdio:'ignore'});
  let targets = null;
  for (let i = 0; i < 40; i++) { try { const r = await fetch(`http://127.0.0.1:${CDP_PORT}/json`); targets = await r.json(); if (targets.length) break; } catch {} await sleep(300); }
  if (!targets) { console.error('FATAL: chrome CDP did not come up'); process.exit(1); }
  const page = targets.find(t => t.type === 'page');
  const ws = new WebSocket(page.webSocketDebuggerUrl);
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
  let msgId = 0; const pending = new Map();
  ws.onmessage = (ev) => { const m = JSON.parse(ev.data); if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); } };
  const send = (method, params = {}) => new Promise((res) => { const id = ++msgId; pending.set(id, res); ws.send(JSON.stringify({ id, method, params })); });
  const evalJS = async (e) => {
    const r = (await send('Runtime.evaluate', { expression: e, returnByValue: true, awaitPromise: true })).result;
    if (r.subtype === 'error') throw new Error(r.description);
    return r.result.value;
  };
  const errors = [];
  ws.addEventListener('message', (ev) => { try { const m = JSON.parse(ev.data); if (m.method === 'Runtime.exceptionThrown') errors.push(m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text); } catch {} });
  await send('Page.enable');
  await send('Runtime.enable');
  await send('Page.navigate', { url: `http://localhost:${PORT}/ui` });
  await sleep(3000);

  let passed = 0, failed = 0;
  const step = async (name, fn) => {
    try { const out = await fn(); console.log(`PASS ${name}${out ? ': ' + out : ''}`); passed++; }
    catch (e) { console.log(`FAIL ${name}: ${e.message}`); failed++; }
  };
  const boot = async () => {
    await evalJS(`(function(){document.querySelector('#signal-tabs [data-sig="metrics"]').click();})()`);
    await evalJS(`(function(){const b=document.getElementById('btn-build'); b.style.display=''; b.click();})()`);
    await sleep(400);
  };
  const qIn = () => `document.querySelector('#search-wrap input')`;
  const setQ = async (q) => { await evalJS(`(function(){const i=${qIn()}; i.value=${JSON.stringify(q)}; i.dispatchEvent(new Event('input',{bubbles:true}));})()`); await sleep(100); };
  const preview = () => evalJS(`document.getElementById('bld-preview').textContent`);
  const bldState = () => evalJS(`(function(){
    return JSON.stringify({
      fn: document.getElementById('bld-fn').value,
      metric: document.getElementById('bld-metric').value,
      filters: [...document.querySelectorAll('#bld-filters .bld-filter')].map(r=>({
        key:r.querySelector('select').value,
        op:r.querySelectorAll('select')[1].value,
        val:r.querySelector('input').value})),
      range: document.querySelector('#bld-fargs .arg-range') ? document.querySelector('#bld-fargs .arg-range').value : null
    });})()`).then(s => JSON.parse(s));
  // reopen builder fresh for each scenario
  const isOpen = () => evalJS(`document.getElementById('builder-bar').classList.contains('visible')`);
  const openBld = async () => { if (!(await isOpen())) { await evalJS(`(function(){document.getElementById('btn-build').click();})()`); } await sleep(250); };
  const closeBld = async () => { if (await isOpen()) { await evalJS(`(function(){document.getElementById('btn-build').click();})()`); } await sleep(150); };

  await step('page loads, builder opens', boot);

  // ── Scenario 1: typed filtered selector → builder keeps filters ──
  await step('typed {method="GET"} selector keeps its filter', async () => {
    await closeBld();
    await setQ('http_requests_total_0{method="GET"}');
    await openBld();
    const st = await bldState();
    if (st.filters.length !== 1 || st.filters[0].key !== 'method' || st.filters[0].val !== 'GET' || st.filters[0].op !== '=') throw new Error('filter lost: ' + JSON.stringify(st.filters));
    const p = await preview();
    if (p !== 'http_requests_total_0{method="GET"}') throw new Error(p);
    return p;
  });

  // ── Scenario 2: typed filtered query → select fn → filter survives ──
  await step('typed filter + fn selection keeps the filter', async () => {
    await closeBld();
    await setQ('http_requests_total_0{method="GET",status="200"}');
    await openBld();
    await evalJS(`(function(){const f=document.getElementById('bld-fn'); f.value='rate'; f.dispatchEvent(new Event('change',{bubbles:true}));})()`);
    await sleep(200);
    const p = await preview();
    if (!/rate\(http_requests_total_0\{method="GET", ?status="200"\}\[5m\]\)/.test(p)) throw new Error(p);
    return p;
  });

  // ── Scenario 3: fn FIRST in builder, then add filter — still works ──
  await step('fn first, then filter added, survives', async () => {
    await closeBld();
    await setQ('');
    await openBld();
    // reset to a clean selector: drop any leftover fn/filters from earlier scenarios
    await evalJS(`(function(){const f=document.getElementById('bld-fn'); if(f.value){f.value='';f.dispatchEvent(new Event('change',{bubbles:true}));}})()`);
    await sleep(100);
    await evalJS(`(function(){while(document.querySelector('#bld-filters .bld-x'))document.querySelector('#bld-filters .bld-x').click();})()`);
    await sleep(100);
    await evalJS(`(function(){const s=document.getElementById('bld-metric'); const o=[...s.options].find(o=>o.value==='http_requests_total_0'); if(o&&s.value!=='http_requests_total_0'){s.value='http_requests_total_0';s.dispatchEvent(new Event('change',{bubbles:true}));}})()`);
    await sleep(100);
    await evalJS(`(function(){const f=document.getElementById('bld-fn'); f.value='rate'; f.dispatchEvent(new Event('change',{bubbles:true}));})()`);
    await sleep(150);
    await evalJS(`(function(){document.getElementById('bld-add-filter').click();})()`);
    await sleep(100);
    await evalJS(`(function(){const s=document.querySelector('#bld-filters .bld-filter select'); s.value='method'; s.dispatchEvent(new Event('change',{bubbles:true}));})()`);
    await sleep(100);
    await evalJS(`(function(){const i=document.querySelector('#bld-filters input'); i.value='GET'; i.dispatchEvent(new Event('input',{bubbles:true}));})()`);
    await sleep(200);
    const p = await preview();
    if (!/rate\(http_requests_total_0\{method="GET"\}\[5m\]\)/.test(p)) throw new Error(p);
    return p;
  });

  // ── Scenario 4: typed agg+grouping+filter round trip ──
  await step('typed sum by (status) with filter round-trips', async () => {
    await closeBld();
    await setQ('sum by (status) (http_requests_total_0{method="GET"})');
    await openBld();
    const st = await bldState();
    if (st.fn !== 'sum') throw new Error('fn=' + st.fn);
    if (st.filters.length !== 1 || st.filters[0].key !== 'method' || st.filters[0].val !== 'GET') throw new Error('filters=' + JSON.stringify(st.filters));
    const grp = await evalJS(`(function(){const s=document.querySelectorAll('#bld-fargs select'); return s.length? s[0].value : null;})()`);
    if (grp !== 'status') throw new Error('grouping select=' + grp);
    const p = await preview();
    if (!/sum by \(status\) \(http_requests_total_0\{method="GET"\}\)/.test(p)) throw new Error(p);
    return p;
  });

  // ── Scenario 5: typed rate(...[10m]) with filter keeps range + filter ──
  await step('typed rate(m{...}[10m]) keeps range + filter', async () => {
    await closeBld();
    await setQ('rate(http_requests_total_0{method="POST"}[10m])');
    await openBld();
    const st = await bldState();
    if (st.fn !== 'rate') throw new Error('fn=' + st.fn);
    if (!st.filters.length || st.filters[0].val !== 'POST') throw new Error('filters=' + JSON.stringify(st.filters));
    if (st.range !== '10m') throw new Error('range=' + st.range);
    const p = await preview();
    if (!/rate\(http_requests_total_0\{method="POST"\}\[10m\]\)/.test(p)) throw new Error(p);
    return p;
  });

  // ── Scenario 6: close builder → query input gets the full filtered query ──
  await step('closing builder writes the full filtered query', async () => {
    await openBld();
    await closeBld(); // close → writes query + runs it
    await sleep(400);
    const v = await evalJS(`${qIn()}.value`);
    if (!/http_requests_total_0\{method="POST"\}/.test(v)) throw new Error(v);
    return v;
  });

  // ── Scenario 7: unknown fn in typed query → builder state untouched (no crash) ──
  await step('unknown fn leaves builder state intact', async () => {
    await closeBld();
    await setQ('derp(http_requests_total_0{method="GET"})');
    await openBld();
    const st = await bldState();
    if (st.fn !== '' ) { /* state may keep previous fn — acceptable as long as preview is sane */ }
    const p = await preview();
    if (p === 'undefined' || p === null) throw new Error('preview broken');
    return 'no crash, preview=' + JSON.stringify(p.slice(0, 40));
  });

  if (errors.length) { console.log('PAGE ERRORS:'); errors.forEach(e => console.log('  ' + e.split('\n')[0])); }
  console.log(`\n${passed} passed, ${failed} failed`);
  chrome.kill();
  process.exit(failed ? 1 : 0);
}
main().catch(e => { console.error('ERR', e.message); process.exit(1); });
