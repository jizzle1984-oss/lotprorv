// server.js — LotProRV v4 (serves index.html from disk — no string escaping)
import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import path from 'path'
import { fileURLToPath } from 'url'
import rateLimit from 'express-rate-limit'
import pLimit from 'p-limit'
import pRetry, { AbortError } from 'p-retry'
import * as cheerio from 'cheerio'
import { initDb, dbRun, dbGet, dbAll, dbExec, dbTransaction, lastInsertRowId } from './db.js'

const __dirname      = path.dirname(fileURLToPath(import.meta.url))
const PORT           = process.env.PORT           || 3001
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD  || 'lotprorv-admin'
const SCRAPERAPI_KEY = process.env.SCRAPERAPI_KEY  || ''
const CONCURRENCY    = parseInt(process.env.CRAWL_CONCURRENCY || '3')

// ── DB INIT ───────────────────────────────────────────────────────────────────
await initDb()

dbExec(`
  CREATE TABLE IF NOT EXISTS urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL DEFAULT 'guide',
    discovered_from TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    last_crawled_at TEXT,
    next_crawl_at TEXT,
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_urls_status   ON urls(status);
  CREATE INDEX IF NOT EXISTS idx_urls_priority ON urls(priority DESC);

  CREATE TABLE IF NOT EXISTS floorplans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT NOT NULL UNIQUE,
    year INTEGER, make TEXT, model TEXT, floorplan TEXT,
    rv_type TEXT, rv_class TEXT,
    length_ft REAL, width_ft REAL, height_ft REAL,
    dry_weight_lbs REAL, gvwr_lbs REAL, hitch_weight_lbs REAL,
    pin_weight_lbs REAL, payload_lbs REAL,
    fresh_water_gal REAL, gray_water_gal REAL, black_water_gal REAL,
    fuel_gal REAL, lp_lbs REAL,
    sleep_capacity INTEGER, slides_count INTEGER, msrp REAL,
    engine TEXT, transmission TEXT, chassis TEXT,
    horsepower INTEGER, torque_lb_ft INTEGER,
    features TEXT, raw_specs TEXT,
    parse_confidence REAL NOT NULL DEFAULT 0,
    parse_notes TEXT,
    scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    is_stale INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_fp_make    ON floorplans(make);
  CREATE INDEX IF NOT EXISTS idx_fp_year    ON floorplans(year);
  CREATE INDEX IF NOT EXISTS idx_fp_rv_type ON floorplans(rv_type);

  CREATE TABLE IF NOT EXISTS crawl_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger TEXT NOT NULL DEFAULT 'manual',
    scope TEXT, status TEXT NOT NULL DEFAULT 'running',
    urls_crawled INTEGER NOT NULL DEFAULT 0,
    urls_failed INTEGER NOT NULL DEFAULT 0,
    records_added INTEGER NOT NULL DEFAULT 0,
    records_updated INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT, error TEXT
  );

  CREATE TABLE IF NOT EXISTS crawl_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    level TEXT NOT NULL DEFAULT 'info',
    url TEXT, message TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_logs_run ON crawl_logs(run_id);
`)
console.log('✅ DB ready')

// ── DB HELPERS ────────────────────────────────────────────────────────────────
const FP_FIELDS = [
  'source_url','year','make','model','floorplan','rv_type','rv_class',
  'length_ft','width_ft','height_ft','dry_weight_lbs','gvwr_lbs',
  'hitch_weight_lbs','pin_weight_lbs','payload_lbs','fresh_water_gal',
  'gray_water_gal','black_water_gal','fuel_gal','lp_lbs',
  'sleep_capacity','slides_count','msrp','engine','transmission',
  'chassis','horsepower','torque_lb_ft','features','raw_specs',
  'parse_confidence','parse_notes'
]

function upsertFloorplan(data) {
  const row = {}
  for (const f of FP_FIELDS) {
    let v = data[f] ?? null
    if (f === 'features' && Array.isArray(v)) v = JSON.stringify(v)
    if (f === 'raw_specs' && v && typeof v === 'object') v = JSON.stringify(v)
    row[f] = v
  }
  const existing = dbGet(`SELECT id FROM floorplans WHERE source_url=?`, [row.source_url])
  if (existing) {
    const sets = FP_FIELDS.filter(f => f !== 'source_url').map(f => `${f}=?`).join(',')
    const vals = FP_FIELDS.filter(f => f !== 'source_url').map(f => row[f])
    dbRun(`UPDATE floorplans SET ${sets},updated_at=datetime('now'),is_stale=0 WHERE id=?`, [...vals, existing.id])
    return { action: 'updated', id: existing.id }
  }
  dbRun(
    `INSERT INTO floorplans (${FP_FIELDS.join(',')}) VALUES (${FP_FIELDS.map(() => '?').join(',')})`,
    FP_FIELDS.map(f => row[f])
  )
  return { action: 'inserted', id: lastInsertRowId() }
}

function searchFloorplans({ q, make, year, rv_type, min_length, max_length, sleeps, slides, limit=40, offset=0 }) {
  const conds = [], params = []
  if (q) {
    const term = `%${q}%`
    conds.push(`(make LIKE ? OR model LIKE ? OR floorplan LIKE ? OR rv_type LIKE ?)`)
    params.push(term, term, term, term)
  }
  if (make)       { conds.push('LOWER(make) LIKE LOWER(?)');    params.push(`%${make}%`) }
  if (year)       { conds.push('year=?');                        params.push(+year) }
  if (rv_type)    { conds.push('LOWER(rv_type) LIKE LOWER(?)'); params.push(`%${rv_type}%`) }
  if (min_length) { conds.push('length_ft>=?');                  params.push(+min_length) }
  if (max_length) { conds.push('length_ft<=?');                  params.push(+max_length) }
  if (sleeps)     { conds.push('sleep_capacity>=?');             params.push(+sleeps) }
  if (slides)     { conds.push('slides_count>=?');               params.push(+slides) }
  const where = conds.length ? `WHERE ${conds.join(' AND ')}` : ''
  const total = dbGet(`SELECT COUNT(*) as c FROM floorplans ${where}`, params)?.c || 0
  const rows  = dbAll(
    `SELECT id,year,make,model,floorplan,rv_type,rv_class,length_ft,dry_weight_lbs,gvwr_lbs,sleep_capacity,slides_count,msrp,parse_confidence,scraped_at FROM floorplans ${where} ORDER BY make,model,year DESC LIMIT ? OFFSET ?`,
    [...params, limit, offset]
  )
  return { rows, total }
}

function getFloorplan(id) {
  const fp = dbGet(`SELECT * FROM floorplans WHERE id=?`, [id])
  if (!fp) return null
  try { fp.features = JSON.parse(fp.features) } catch { fp.features = [] }
  try { fp.raw_specs = JSON.parse(fp.raw_specs) } catch { fp.raw_specs = {} }
  return fp
}

function bulkUpsertUrls(items) {
  dbTransaction(() => {
    for (const r of items) {
      dbRun(`INSERT INTO urls (url,type,discovered_from,priority) VALUES (?,?,?,?) ON CONFLICT(url) DO NOTHING`,
        [r.url, r.type, r.discoveredFrom || null, r.priority || 0])
    }
  })
}

function claimNextUrl() {
  const row = dbGet(`SELECT * FROM urls WHERE status='pending' AND (next_crawl_at IS NULL OR next_crawl_at<=datetime('now')) ORDER BY priority DESC, id ASC LIMIT 1`)
  if (!row) return null
  dbRun(`UPDATE urls SET status='in_progress', updated_at=datetime('now') WHERE id=?`, [row.id])
  return row
}

function markUrlDone(id, s) {
  dbRun(`UPDATE urls SET status='done',http_status=?,last_crawled_at=datetime('now'),next_crawl_at=datetime('now','+7 days'),error=NULL,updated_at=datetime('now') WHERE id=?`, [s, id])
}

function markUrlError(id, s, e) {
  const row = dbGet(`SELECT retry_count FROM urls WHERE id=?`, [id])
  const retries = (row?.retry_count || 0) + 1
  dbRun(`UPDATE urls SET http_status=?,error=?,retry_count=?,status=?,next_crawl_at=datetime('now','+5 minutes'),updated_at=datetime('now') WHERE id=?`,
    [s, String(e).slice(0,500), retries, retries >= 5 ? 'error' : 'pending', id])
}

function startCrawlRun(trigger, scope) {
  dbRun(`INSERT INTO crawl_runs (trigger,scope) VALUES (?,?)`, [trigger, scope || null])
  return lastInsertRowId()
}

function finishCrawlRun(id, stats) {
  dbRun(`UPDATE crawl_runs SET status='done',urls_crawled=?,urls_failed=?,records_added=?,records_updated=?,finished_at=datetime('now') WHERE id=?`,
    [stats.crawled, stats.failed, stats.added, stats.updated, id])
}

function addLog(runId, level, message, url = null) {
  try { dbRun(`INSERT INTO crawl_logs (run_id,level,message,url) VALUES (?,?,?,?)`, [runId, level, String(message).slice(0,2000), url]) } catch {}
}

// ── PARSERS ───────────────────────────────────────────────────────────────────
const RVUSA_BASE = 'https://www.rvusa.com'
const abs = href => !href ? null : href.startsWith('http') ? href : RVUSA_BASE + (href.startsWith('/') ? href : '/' + href)

function parseNum(str) {
  if (!str) return null
  const n = parseFloat(String(str).replace(/[^0-9.]/g, ''))
  return isNaN(n) ? null : n
}

function parseFeetInches(str) {
  if (!str) return null
  const s = String(str).trim()
  const fi = s.match(/^(\d+)['′](?:\s*(\d+)["″])?/)
  if (fi) return parseFloat(fi[1]) + (fi[2] ? parseFloat(fi[2]) / 12 : 0)
  const ft = s.match(/^([\d.]+)\s*(?:ft|feet)/)
  if (ft) return parseFloat(ft[1])
  return parseNum(s)
}

const FIELD_MAP = [
  { key:'dry_weight_lbs',   labels:['dry weight','unloaded vehicle weight','uvw'] },
  { key:'gvwr_lbs',         labels:['gvwr','gross vehicle weight','gross weight'] },
  { key:'hitch_weight_lbs', labels:['hitch weight','tongue weight'] },
  { key:'pin_weight_lbs',   labels:['pin weight','kingpin'] },
  { key:'payload_lbs',      labels:['payload','cargo capacity','net carrying'] },
  { key:'fresh_water_gal',  labels:['fresh water','freshwater'] },
  { key:'gray_water_gal',   labels:['gray water','grey water'] },
  { key:'black_water_gal',  labels:['black water','black tank'] },
  { key:'fuel_gal',         labels:['fuel capacity','fuel tank'] },
  { key:'lp_lbs',           labels:['lp capacity','lp gas','propane'] },
  { key:'length_ft',        labels:['overall length','exterior length','length'] },
  { key:'width_ft',         labels:['overall width','exterior width','width'] },
  { key:'height_ft',        labels:['overall height','exterior height','height'] },
  { key:'sleep_capacity',   labels:['sleeping capacity','sleeps'] },
  { key:'slides_count',     labels:['number of slides','slides','slide-outs'] },
  { key:'msrp',             labels:['msrp','suggested retail','price'] },
  { key:'engine',           labels:['engine'] },
  { key:'transmission',     labels:['transmission'] },
  { key:'chassis',          labels:['chassis'] },
  { key:'horsepower',       labels:['horsepower','hp'] },
]

const STRING_KEYS = new Set(['engine','transmission','chassis'])
const DIM_KEYS    = new Set(['length_ft','width_ft','height_ft'])

function labelToKey(label) {
  const lc = label.toLowerCase().trim()
  for (const { key, labels } of FIELD_MAP) if (labels.some(l => lc.includes(l))) return key
  return null
}

function coerceValue(key, raw) {
  if (raw == null) return null
  if (STRING_KEYS.has(key)) return String(raw).trim()
  if (DIM_KEYS.has(key))    return parseFeetInches(raw)
  return parseNum(raw)
}

function mapRawSpecs(rawMap) {
  const out = {}
  for (const [label, value] of Object.entries(rawMap)) {
    const key = labelToKey(label)
    if (key && out[key] == null) out[key] = coerceValue(key, value)
  }
  return out
}

const RV_TYPES = [
  ['fifth wheel','Fifth Wheel'],['5th wheel','Fifth Wheel'],
  ['toy hauler','Toy Hauler'],['class a','Class A Motorhome'],
  ['class b+','Class B+ Motorhome'],['class b','Class B Motorhome'],
  ['class c','Class C Motorhome'],['pop-up','Pop-Up Camper'],
  ['truck camper','Truck Camper'],['park model','Park Model'],
  ['travel trailer','Travel Trailer'],['trailer','Travel Trailer'],
]

function normalizeRvType(str) {
  if (!str) return null
  const lc = str.toLowerCase()
  for (const [p, n] of RV_TYPES) if (lc.includes(p)) return n
  return null
}

function titleCase(s) { return s ? s.replace(/\b\w/g, c => c.toUpperCase()) : null }

function detectPageType(url) {
  const parts = url.replace(/^https?:\/\/[^/]+/, '').split('/').filter(Boolean)
  const idx   = parts.findIndex(p => ['specs','rv-specs','specs-guide'].includes(p))
  if (idx < 0) return 'guide'
  const depth = parts.length - idx
  if (depth <= 1) return 'guide'
  if (depth === 2) return 'make'
  if (depth === 3) return 'model'
  return 'floorplan'
}

function infoFromUrl(url) {
  const parts = url.replace(/^https?:\/\/[^/]+/, '').split('/').filter(Boolean)
  const idx   = parts.findIndex(p => ['specs','rv-specs'].includes(p))
  if (idx < 0) return {}
  const p    = parts.slice(idx + 1)
  const slug = s => s?.replace(/-/g, ' ') ?? null
  let make = slug(p[0]), model = slug(p[1]), year = null, floorplan = null
  if (p[2] && /^\d{4}$/.test(p[2])) { year = parseInt(p[2]); floorplan = slug(p[3]) }
  else floorplan = slug(p[2])
  return { make, model, year, floorplan }
}

function parsePage(html, url) {
  const $    = cheerio.load(html)
  const type = detectPageType(url)
  const links = new Set()

  $('a[href]').each((_, el) => {
    const full = abs($(el).attr('href'))
    if (full && full.includes('rvusa.com') && full !== url &&
        /\/rv-guide\/(specs|rv-specs)/.test(full) && !full.includes('?'))
      links.add(full)
  })

  if (type !== 'floorplan') return { pageType: type, links: [...links] }

  const rawSpecs = {}
  for (const sel of ['table.specs-table','table[class*="spec"]','.rv-specs table','table']) {
    $(sel).each((_, tbl) => {
      $('tr', tbl).each((_, row) => {
        const cells = $('td,th', row).toArray()
        if (cells.length >= 2) {
          const k = $(cells[0]).text().trim(), v = $(cells[1]).text().trim()
          if (k && v) rawSpecs[k] = v
        }
      })
    })
    if (Object.keys(rawSpecs).length > 4) break
  }
  if (Object.keys(rawSpecs).length < 4) {
    $('dl').each((_, dl) => {
      const dts = $('dt', dl).toArray(), dds = $('dd', dl).toArray()
      dts.forEach((dt, i) => {
        const k = $(dt).text().trim(), v = dds[i] ? $(dds[i]).text().trim() : null
        if (k && v) rawSpecs[k] = v
      })
    })
  }

  const { make, model, year, floorplan } = infoFromUrl(url)
  const h1       = $('h1').first().text().trim()
  const metaDesc = $('meta[name="description"]').attr('content') || ''
  let rv_type = null, rv_class = null

  for (const src of [$('[class*="category"]').first().text(), h1, metaDesc]) {
    const t = normalizeRvType(src); if (t) { rv_type = t; break }
  }
  const classM = (h1 + ' ' + metaDesc).match(/class\s+([abc])\b/i)
  if (classM) rv_class = 'Class ' + classM[1].toUpperCase()

  const mapped = mapRawSpecs(rawSpecs)
  const required = [make, model, year, mapped.dry_weight_lbs ?? mapped.gvwr_lbs, mapped.length_ft]
  const parse_confidence = +(required.filter(Boolean).length / required.length).toFixed(2)
  const notes = []
  if (!make)  notes.push('make not found')
  if (!model) notes.push('model not found')
  if (!year)  notes.push('year not found')
  if (!Object.keys(rawSpecs).length) notes.push('NO SPEC TABLE — may need JS rendering')

  return {
    pageType: 'floorplan', links: [...links],
    source_url: url, year,
    make: titleCase(make), model: titleCase(model), floorplan: titleCase(floorplan),
    rv_type, rv_class, ...mapped,
    raw_specs: Object.keys(rawSpecs).length ? rawSpecs : null,
    parse_confidence,
    parse_notes: notes.length ? notes.join('; ') : null,
  }
}

// ── FETCHER ───────────────────────────────────────────────────────────────────
async function fetchPage(url, scraperOpts = {}) {
  if (!SCRAPERAPI_KEY) throw new Error('SCRAPERAPI_KEY not set')
  const scraperUrl = `https://api.scraperapi.com/?${new URLSearchParams({ api_key: SCRAPERAPI_KEY, url, ...scraperOpts })}`
  return pRetry(async attempt => {
    const ctrl  = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), 35_000)
    let res
    try   { res = await fetch(scraperUrl, { signal: ctrl.signal }) }
    catch (e) { clearTimeout(timer); if (e.name === 'AbortError') throw new Error(`Timeout: ${url}`); throw e }
    clearTimeout(timer)
    if ([403,404,410].includes(res.status)) throw new AbortError(`HTTP ${res.status}`)
    if (res.status === 429 || res.status >= 500) { const e = new Error(`HTTP ${res.status}`); e.status = res.status; throw e }
    return { html: await res.text(), status: res.status }
  }, { retries:4, minTimeout:1500, maxTimeout:32_000, factor:2,
       onFailedAttempt(e) { console.warn(`  ↻ [${e.attemptNumber}/5] ${url} — ${e.message}`) } })
}

// ── CRAWLER ───────────────────────────────────────────────────────────────────
const activeCrawls = new Map()
const sleep = ms => new Promise(r => setTimeout(r, ms))

async function runCrawl({ trigger = 'manual', scope = null, seedUrls = null } = {}) {
  const runId = startCrawlRun(trigger, scope)
  const stats = { crawled:0, failed:0, added:0, updated:0 }
  activeCrawls.set(runId, { stats, logs:[], status:'running' })

  const log = (level, msg, url = null) => {
    console.log(`[${level.toUpperCase()}] ${msg}`)
    addLog(runId, level, msg, url)
    const c = activeCrawls.get(runId)
    if (c) { c.logs.push({ level, message:msg, url, created_at:new Date().toISOString() }); if (c.logs.length > 500) c.logs.shift() }
  }

  const seeds = seedUrls || ['https://www.rvusa.com/rv-guide/specs-guide']
  bulkUpsertUrls(seeds.map(url => ({ url, type:'guide', discoveredFrom:null, priority:100 })))
  log('info', `Crawl #${runId} started — ${trigger} scope=${scope || 'all'}`)

  const limiter = pLimit(CONCURRENCY)
  let idle = 0
  const pending = []

  while (idle < 3) {
    const row = claimNextUrl()
    if (!row) { await Promise.all(pending.splice(0)); idle++; await sleep(1500); continue }
    idle = 0

    const task = limiter(async () => {
      try {
        let { html, status } = await fetchPage(row.url)
        let result = parsePage(html, row.url)
        if (result.pageType === 'floorplan' && !result.raw_specs) {
          log('warn', 'No specs — retrying with JS render', row.url)
          const r2 = await fetchPage(row.url, { render_js:'true', wait:2000 })
          result = parsePage(r2.html, row.url)
        }
        markUrlDone(row.id, status)
        stats.crawled++
        const newLinks = (result.links || []).map(link => ({
          url: link, type: detectPageType(link), discoveredFrom: row.url,
          priority: result.pageType==='guide' ? 10 : result.pageType==='make' ? 5 : 1,
        }))
        if (newLinks.length) { bulkUpsertUrls(newLinks); log('info', `Enqueued ${newLinks.length} URLs`, row.url) }
        if (result.pageType === 'floorplan') {
          const { action } = upsertFloorplan(result)
          log('info', `Floorplan ${action}: ${result.make||'?'} ${result.model||'?'} ${result.year||''} (conf=${result.parse_confidence})`, row.url)
          if (action === 'inserted') stats.added++; else stats.updated++
        }
      } catch (e) {
        markUrlError(row.id, e.status || 0, e.message)
        log('error', `Failed: ${e.message}`, row.url)
        stats.failed++
      }
      const c = activeCrawls.get(runId); if (c) Object.assign(c.stats, stats)
    })

    pending.push(task)
    task.finally(() => { const i = pending.indexOf(task); if (i >= 0) pending.splice(i, 1) })
    await sleep(350 + Math.random() * 300)
  }

  await Promise.all(pending)
  finishCrawlRun(runId, stats)
  const c = activeCrawls.get(runId); if (c) c.status = 'done'
  log('info', `Crawl #${runId} done — crawled=${stats.crawled} added=${stats.added} failed=${stats.failed}`)
  return { runId, ...stats }
}

// ── EXPRESS ───────────────────────────────────────────────────────────────────
const app = express()
app.use(express.json())
app.use(cors())
app.use('/api', rateLimit({ windowMs:60_000, max:200, standardHeaders:true, legacyHeaders:false }))

function requireAdmin(req, res, next) {
  const token = req.headers['x-admin-password'] || req.query.admin_password
  if (token !== ADMIN_PASSWORD) return res.status(401).json({ error:'Unauthorized' })
  next()
}

app.get('/api/floorplans', (req, res) => {
  try {
    res.json(searchFloorplans({
      q:req.query.q, make:req.query.make, year:req.query.year,
      rv_type:req.query.rv_type, min_length:req.query.min_length,
      max_length:req.query.max_length, sleeps:req.query.sleeps, slides:req.query.slides,
      limit:Math.min(+(req.query.limit||40),100), offset:+(req.query.offset||0),
    }))
  } catch(e) { res.status(500).json({ error:e.message }) }
})

app.get('/api/floorplans/:id', (req, res) => {
  const fp = getFloorplan(+req.params.id)
  if (!fp) return res.status(404).json({ error:'Not found' })
  res.json(fp)
})

app.get('/api/meta', (req, res) => {
  res.json({
    makes:    dbAll(`SELECT DISTINCT make FROM floorplans WHERE make IS NOT NULL ORDER BY make`).map(r => r.make),
    rv_types: dbAll(`SELECT DISTINCT rv_type FROM floorplans WHERE rv_type IS NOT NULL ORDER BY rv_type`).map(r => r.rv_type),
  })
})

app.get('/api/stats', (req, res) => {
  res.json({
    floorplans: {
      total:   dbGet(`SELECT COUNT(*) as c FROM floorplans`)?.c || 0,
      byType:  dbAll(`SELECT rv_type,COUNT(*) as c FROM floorplans GROUP BY rv_type ORDER BY c DESC`),
      byMake:  dbAll(`SELECT make,COUNT(*) as c FROM floorplans GROUP BY make ORDER BY c DESC LIMIT 20`),
      lowConf: dbGet(`SELECT COUNT(*) as c FROM floorplans WHERE parse_confidence<0.5`)?.c || 0,
    },
    urls: dbAll(`SELECT type,status,COUNT(*) as count FROM urls GROUP BY type,status ORDER BY type,status`),
  })
})

app.post('/api/admin/crawl', requireAdmin, (req, res) => {
  const { make, url, scope } = req.body || {}
  setImmediate(async () => {
    try {
      if (url) {
        bulkUpsertUrls([{ url, type:detectPageType(url), discoveredFrom:null, priority:999 }])
        await runCrawl({ trigger:'admin-url', scope:url, seedUrls:[url] })
      } else if (make) {
        const u = `https://www.rvusa.com/rv-guide/rv-specs/${make.toLowerCase().replace(/\s+/g,'-')}`
        await runCrawl({ trigger:'admin-make', scope:make, seedUrls:[u] })
      } else {
        await runCrawl({ trigger:'admin-full', scope })
      }
    } catch(e) { console.error('Crawl error:', e) }
  })
  res.json({ status:'started' })
})

app.get('/api/admin/crawl/stream', requireAdmin, (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.flushHeaders()
  const send = data => res.write(`data: ${JSON.stringify(data)}\n\n`)
  const iv = setInterval(() => {
    let latest = null
    for (const [runId, crawl] of activeCrawls)
      if (!latest || runId > latest.runId) latest = { runId, ...crawl }
    if (latest) send({ runId:latest.runId, stats:latest.stats, status:latest.status, recentLogs:latest.logs.slice(-20) })
    else send({ idle:true })
  }, 1500)
  req.on('close', () => clearInterval(iv))
})

app.get('/api/admin/errors',          requireAdmin, (req,res) => res.json(dbAll(`SELECT * FROM urls WHERE status='error' ORDER BY updated_at DESC LIMIT ?`, [+(req.query.limit||100)])))
app.post('/api/admin/requeue-errors', requireAdmin, (req,res) => { dbRun(`UPDATE urls SET status='pending',retry_count=0,error=NULL,updated_at=datetime('now') WHERE status='error'`); res.json({ok:true}) })
app.get('/api/admin/logs',            requireAdmin, (req,res) => res.json(dbAll(`SELECT l.*,r.trigger,r.scope FROM crawl_logs l LEFT JOIN crawl_runs r ON l.run_id=r.id ORDER BY l.id DESC LIMIT ?`, [+(req.query.limit||300)])))
app.get('/api/admin/runs',            requireAdmin, (req,res) => res.json(dbAll(`SELECT * FROM crawl_runs ORDER BY id DESC LIMIT 25`)))
app.get('/api/admin/auth-check',      requireAdmin, (req,res) => res.json({ok:true}))

// Serve frontend
app.get(/^(?!\/api).*/, (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'))
})

app.listen(PORT, () => {
  console.log(`\n🚐 LotProRV running at http://localhost:${PORT}`)
  console.log(`   Admin password: ${ADMIN_PASSWORD}\n`)
})
