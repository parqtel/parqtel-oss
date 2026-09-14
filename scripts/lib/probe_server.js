// Probe server: serves the real UI page on :9099 and proxies /api + /v1
// requests to the real parqtel so the page's own fetches work under test.
// Usage: node .tools/probe_server.js [upstreamURL]   (default :9090)
const http = require('http');
const zlib = require('zlib');
const fs = require('fs');

const UPSTREAM = process.argv[2] || 'http://localhost:9090';
const LISTEN = process.argv[3] ? Number(process.argv[3]) : 9099;

http.get(UPSTREAM + '/ui', { headers: { 'Accept-Encoding': 'gzip' } }, (res) => {
  const chunks = [];
  res.on('data', (c) => chunks.push(c));
  res.on('end', () => {
    let buf = Buffer.concat(chunks);
    if (res.headers['content-encoding'] === 'gzip') buf = zlib.gunzipSync(buf);
    const html = buf.toString();
    // NOTE: no script injection — the CDP test (.tools/cdp_builder_test.js)
    // drives the page itself; a parallel injected probe caused races.
    fs.writeFileSync('.tools/probe_served.html', html);

    http
      .createServer((req, res2) => {
        // Page and API proxying: page paths serve the injected HTML;
        // everything else (API calls) pipes to the real parqtel.
        if (req.url === '/ui' || req.url === '/' || req.url.startsWith('/?')) {
          res2.setHeader('content-type', 'text/html');
          return res2.end(html);
        }
        const up = req.pipe(
          http.request(UPSTREAM + req.url, { method: req.method, headers: { ...req.headers, host: 'localhost:9090' } }, (ur) => {
            res2.writeHead(ur.statusCode, ur.headers);
            ur.pipe(res2);
          })
        );
        up.on('error', () => {
          res2.writeHead(502);
          res2.end('proxy error');
        });
      })
      .listen(LISTEN);
    console.log('probe server up on 9099 (API proxied)');
  });
});
setInterval(() => {}, 1 << 30);
