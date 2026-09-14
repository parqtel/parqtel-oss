// Deterministic headless builder E2E via raw Chrome DevTools Protocol.
// Opens the real UI (through the API-proxying server), drives the builder
// with DOM events, and waits for real conditions instead of virtual-time
// racing.
// Usage: node scripts/test_builder_ui.js [proxyPort]   (default 9099)
// Launched by scripts/test_builder_ui.sh (which starts the proxy server).
const PORT = process.argv[2] ? Number(process.argv[2]) : 9099;
const CDP_PORT = 9200 + (process.getpid ? process.getpid() % 100 : 22);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  // Launch Chrome with remote debugging.
  const { spawn } = require('child_process');
  const chrome = spawn('google-chrome', [
    '--headless=new', '--disable-gpu', '--no-sandbox',
    `--user-data-dir=/tmp/cdp-profile-${Date.now()}`,
    `--remote-debugging-port=${CDP_PORT}`,
    'about:blank',
  ], { stdio: 'ignore' });
  // Wait for the DevTools endpoint.
  let targets = null;
  for (let i = 0; i < 30; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${CDP_PORT}/json`);
      targets = await r.json();
      if (targets.length) break;
    } catch {}
    await sleep(300);
  }
  if (!targets) throw new Error('chrome never came up');
  const page = targets.find((t) => t.type === 'page');
  const ws = new WebSocket(page.webSocketDebuggerUrl);

  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
  let msgId = 0;
  const pending = new Map();
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.id && pending.has(m.id)) {
      pending.get(m.id)(m);
      pending.delete(m.id);
    }
  };
  const send = (method, params = {}) => new Promise((res) => {
    const id = ++msgId;
    pending.set(id, res);
    ws.send(JSON.stringify({ id, method, params }));
  });
  const evalJS = async (expression) => {
    const r = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
    return r.result ? r.result.result.value : undefined;
  };

  // Navigate to the proxy-served UI.
  await send('Page.enable');
  await send('Page.navigate', { url: `http://localhost:${PORT}/ui` });
  await sleep(2500); // let init + prefetch settle

  const out = [];
  const step = async (name, fn) => {
    try { const r = await fn(); out.push('PASS ' + name + (r !== undefined ? ': ' + r : '')); }
    catch (e) { out.push('FAIL ' + name + ': ' + e.message); }
  };
  const waitFor = async (condExpr, label, timeoutMs = 8000) => {
    const t0 = Date.now();
    while (Date.now() - t0 < timeoutMs) {
      if (await evalJS(condExpr)) return true;
      await sleep(200);
    }
    throw new Error('timeout waiting for ' + label);
  };

  // 0. switch to metrics signal
  await step('switch to metrics tab', async () => {
    await evalJS(`document.querySelector('#signal-tabs [data-sig="metrics"]').click()`);
    await waitFor(`document.getElementById('view-metrics').classList.contains('active')`, 'metrics pane');
  });
  // 1. open builder (re-populate via prefetch if needed)
  await step('builder toggle', async () => {
    await evalJS(`(function(){      const b = document.getElementById('btn-build');
      b.style.display = '';
      b.click();
      true
    })()`);
    await waitFor(`document.getElementById('builder-bar').classList.contains('visible')`, 'builder bar');
  });
  // 2. function catalog
  await step('fn catalog >= 80 fns', async () => {
    const n = await evalJS(`document.getElementById('bld-fn').options.length`);
    if (!n || n < 80) throw new Error('only ' + n);
    return n + ' fns';
  });
  // 3. metric options (wait for names to load — the toggle prefetch)
  await step('metric options', async () => {
    await waitFor(`document.getElementById('bld-metric').options.length > 5`, 'metric options');
    const n = await evalJS(`document.getElementById('bld-metric').options.length`);
    return n + ' metrics';
  });
  // 4. select metric → preview
  await step('metric change → preview', async () => {
    await evalJS(`(function(){      const s = document.getElementById('bld-metric');
      s.value = 'http_requests_total_0';
      s.dispatchEvent(new Event('change', {bubbles: true}));
      true
    })()`);
    const q = await evalJS(`document.getElementById('bld-preview').textContent`);
    if (!/http_requests_total_0/.test(q || '')) throw new Error(q);
    return q;
  });
  // 5. rate() + range editor
  await step('rate() + range editor', async () => {
    await evalJS(`(function(){      const f = document.getElementById('bld-fn');
      f.value = 'rate';
      f.dispatchEvent(new Event('change', {bubbles: true}));
      true
    })()`);
    const q = await evalJS(`document.getElementById('bld-preview').textContent`);
    if (!/rate\(http_requests_total_0\[\d+[mhd]\]\)/.test(q || '')) throw new Error(q);
    return q;
  });
  // 6. sum by (method)
  await step('sum by (method)', async () => {
    await evalJS(`(function(){      const f = document.getElementById('bld-fn');
      f.value = 'sum';
      f.dispatchEvent(new Event('change', {bubbles: true}));
      true
    })()`);
    await evalJS(`(function(){      const gs = document.querySelectorAll('#bld-fargs select');
      gs[0].value = 'method';
      gs[0].dispatchEvent(new Event('change', {bubbles: true}));
      true
    })()`);
    const q = await evalJS(`document.getElementById('bld-preview').textContent`);
    if (!/sum by \(method\) \(http_requests_total_0\)/.test(q || '')) throw new Error(q);
    return q;
  });
  // 7. sum(rate(m[5m]))
  await step('sum(rate(m[5m]))', async () => {
    await evalJS(`(function(){      const gs = document.querySelectorAll('#bld-fargs select');
      gs[1].value = 'rate';
      gs[1].dispatchEvent(new Event('change', {bubbles: true}));
      true
    })()`);
    const q = await evalJS(`document.getElementById('bld-preview').textContent`);
    if (!/sum by \(method\) \(rate\(http_requests_total_0\[5m\]\)\)/.test(q || '')) throw new Error(q);
    return q;
  });
  // 8. quantile_over_time(φ, m[r])
  await step('quantile_over_time(φ, m[r])', async () => {
    await evalJS(`(function(){      const f = document.getElementById('bld-fn');
      f.value = 'quantile_over_time';
      f.dispatchEvent(new Event('change', {bubbles: true}));
      true
    })()`);
    const q = await evalJS(`document.getElementById('bld-preview').textContent`);
    if (!/quantile_over_time\(0\.9, http_requests_total_0\[/.test(q || '')) throw new Error(q);
    return q;
  });
  // 9. filter row + user_id AC (bounded fetch)
  await step('filter row + user_id AC bounded ≤ 10', async () => {
    await evalJS(`
      document.getElementById('bld-add-filter').click();
      const sel = document.querySelector('#bld-filters .bld-filter select');
      sel.value = 'user_id';
      sel.dispatchEvent(new Event('change', {bubbles: true}));
      const inp = document.querySelector('#bld-filters input');
      inp.dispatchEvent(new Event('focus')); // headless: synthetic focus event
      true
    `);
    await waitFor(`
      (() => {
        const d = document.querySelector('#bld-filters .bld-ac');
        return d && d.classList.contains('open') && d.querySelectorAll('.ac-v').length > 0;
      })()
    `, 'AC dropdown open');
    const n = await evalJS(`document.querySelectorAll('#bld-filters .bld-ac .ac-v').length`);
    if (n > 10) throw new Error(n + ' items — UNBOUNDED');
    return n + ' values';
  });
  // 10. prefix narrows server-side
  await step('prefix user-04 narrows AC', async () => {
    await evalJS(`(function(){      const inp = document.querySelector('#bld-filters input');
      inp.value = 'user-04';
      inp.dispatchEvent(new Event('input', {bubbles: true}));
      true
    })()`);
    await waitFor(`
      (() => {
        const vs = [...document.querySelectorAll('#bld-filters .bld-ac .ac-v')].map(e => e.textContent);
        return vs.length > 0 && vs.every(v => v.indexOf('user-04') === 0);
      })()
    `, 'prefix-filtered AC values');
    const vis = await evalJS(`[...document.querySelectorAll('#bld-filters .bld-ac .ac-v')].map(e => e.textContent)`);
    return vis.slice(0, 3).join(', ') + '…';
  });

  console.log(out.join('\n'));
  const fails = out.filter((l) => l.startsWith('FAIL')).length;
  console.log(`\n${out.length - fails} passed, ${fails} failed`);
  ws.close();
  chrome.kill();
  process.exit(fails ? 1 : 0);
}

main().catch((e) => { console.error('HARNESS ERROR:', e.message); process.exit(2); });
