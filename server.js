// server.js — LotProRV (sql.js edition — no native deps)
import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import rateLimit from 'express-rate-limit'
import pLimit from 'p-limit'
import pRetry, { AbortError } from 'p-retry'
import * as cheerio from 'cheerio'
import { initDb, dbRun, dbGet, dbAll, dbExec, dbTransaction, lastInsertRowId } from './db.js'

const PORT           = process.env.PORT           || 3001
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD  || 'lotprorv-admin'
const SCRAPERAPI_KEY = process.env.SCRAPERAPI_KEY  || ''
const CONCURRENCY    = parseInt(process.env.CRAWL_CONCURRENCY || '3')

// ── INIT DB ───────────────────────────────────────────────────────────────────
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

console.log('✅ Schema ready')

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
    `INSERT INTO floorplans (${FP_FIELDS.join(',')}) VALUES (${FP_FIELDS.map(()=>'?').join(',')})`,
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
    `SELECT id,year,make,model,floorplan,rv_type,rv_class,length_ft,dry_weight_lbs,gvwr_lbs,sleep_capacity,slides_count,msrp,parse_confidence,scraped_at
     FROM floorplans ${where} ORDER BY make,model,year DESC LIMIT ? OFFSET ?`,
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
      dbRun(
        `INSERT INTO urls (url,type,discovered_from,priority) VALUES (?,?,?,?) ON CONFLICT(url) DO NOTHING`,
        [r.url, r.type, r.discoveredFrom || null, r.priority || 0]
      )
    }
  })
}

function claimNextUrl() {
  const row = dbGet(
    `SELECT * FROM urls WHERE status='pending' AND (next_crawl_at IS NULL OR next_crawl_at<=datetime('now')) ORDER BY priority DESC, id ASC LIMIT 1`
  )
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
  const status = retries >= 5 ? 'error' : 'pending'
  dbRun(`UPDATE urls SET http_status=?,error=?,retry_count=?,status=?,next_crawl_at=datetime('now','+5 minutes'),updated_at=datetime('now') WHERE id=?`,
    [s, String(e).slice(0,500), retries, status, id])
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

  // Extract spec tables
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
  // Definition lists fallback
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
  const h1      = $('h1').first().text().trim()
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
  log('info', `Crawl #${runId} started — ${trigger} scope=${scope||'all'}`)

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

// Public API
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
    makes:    dbAll(`SELECT DISTINCT make FROM floorplans WHERE make IS NOT NULL ORDER BY make`).map(r=>r.make),
    rv_types: dbAll(`SELECT DISTINCT rv_type FROM floorplans WHERE rv_type IS NOT NULL ORDER BY rv_type`).map(r=>r.rv_type),
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

// Admin API
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

app.get('/api/admin/errors',          requireAdmin, (req,res) => res.json(dbAll(`SELECT * FROM urls WHERE status='error' ORDER BY updated_at DESC LIMIT ?`,[+(req.query.limit||100)])))
app.post('/api/admin/requeue-errors', requireAdmin, (req,res) => { dbRun(`UPDATE urls SET status='pending',retry_count=0,error=NULL,updated_at=datetime('now') WHERE status='error'`); res.json({ok:true}) })
app.get('/api/admin/logs',            requireAdmin, (req,res) => res.json(dbAll(`SELECT l.*,r.trigger,r.scope FROM crawl_logs l LEFT JOIN crawl_runs r ON l.run_id=r.id ORDER BY l.id DESC LIMIT ?`,[+(req.query.limit||300)])))
app.get('/api/admin/runs',            requireAdmin, (req,res) => res.json(dbAll(`SELECT * FROM crawl_runs ORDER BY id DESC LIMIT 25`)))
app.get('/api/admin/auth-check',      requireAdmin, (req,res) => res.json({ok:true}))

// ── FRONTEND ──────────────────────────────────────────────────────────────────

const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LotProRV — RV Specs Database</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{--bg:#0b0d0e;--surf:#131618;--surf2:#1a1d21;--surf3:#22262b;--border:rgba(255,255,255,.07);--border2:rgba(255,255,255,.12);--accent:#f59e0b;--accent-lo:rgba(245,158,11,.1);--text:#e8eaed;--text2:#9ca3af;--text3:#6b7280;--ok:#10b981;--err:#ef4444;--warn:#f59e0b;--info:#3b82f6;--r:8px;--r-sm:5px;--mono:'DM Mono',monospace;--sans:'DM Sans',sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;line-height:1.55;min-height:100vh;display:flex;flex-direction:column}
button{font-family:var(--sans);cursor:pointer}
input,select{font-family:var(--sans);background:var(--surf2);border:1px solid var(--border);color:var(--text);border-radius:var(--r-sm);outline:none;transition:border-color .15s}
input:focus,select:focus{border-color:var(--accent)}
input::placeholder{color:var(--text3)}
select option{background:var(--surf2)}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:9px}
@keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
@keyframes shimmer{to{background-position:-200% 0}}
@keyframes spin{to{transform:rotate(360deg)}}
.fade{animation:fadeUp .3s ease both}
.sk{background:linear-gradient(90deg,var(--surf) 25%,var(--surf2) 50%,var(--surf) 75%);background-size:200% 100%;animation:shimmer 1.4s infinite;border-radius:var(--r-sm)}
</style>
</head>
<body>
<div id="app" style="display:flex;flex-direction:column;min-height:100vh">
<header style="background:var(--surf);border-bottom:1px solid var(--border);padding:0 24px;height:54px;display:flex;align-items:center;gap:24px;position:sticky;top:0;z-index:100;flex-shrink:0">
  <div onclick="showPage('browse')" style="font-family:'Bebas Neue',sans-serif;font-size:28px;letter-spacing:2px;color:var(--accent);cursor:pointer;line-height:1">LotPro<span style="color:var(--text3);font-size:20px">RV</span></div>
  <nav style="display:flex;gap:4px;margin-left:auto" id="nav"></nav>
</header>
<main id="main" style="flex:1;max-width:1400px;width:100%;margin:0 auto;padding:0 24px 64px"></main>
<div id="toasts" style="position:fixed;bottom:20px;right:20px;z-index:999;display:flex;flex-direction:column;gap:8px"></div>
</div>
<script>
const API='/api';
let page='browse',selId=null,adminPw='',filters={q:'',make:'',year:'',rv_type:'',min_length:'',max_length:'',sleeps:'',slides:''},bPage=0,metaCache={makes:[],rv_types:[]};
const PAGE_SZ=40;

async function apiFetch(path,opts={}){const r=await fetch(API+path,opts);if(!r.ok){const e=await r.json().catch(()=>({error:r.statusText}));throw new Error(e.error||'API error')}return r.json()}
function adminHeaders(){return{'Content-Type':'application/json','X-Admin-Password':adminPw}}
function num(n){return n==null?'—':Number(n).toLocaleString()}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

function toast(msg,type='info'){
  const w=document.getElementById('toasts'),d=document.createElement('div');
  const col=type==='success'?'var(--ok)':type==='error'?'var(--err)':type==='warn'?'var(--warn)':'var(--info)';
  d.style.cssText='background:var(--surf2);border-left:3px solid '+col+';border-radius:var(--r-sm);padding:10px 16px;font-size:13px;max-width:340px;box-shadow:0 4px 16px rgba(0,0,0,.4);animation:fadeUp .25s ease';
  d.textContent=msg;w.appendChild(d);setTimeout(()=>{d.style.opacity='0';d.style.transition='opacity .3s';setTimeout(()=>d.remove(),300)},3500)
}

function renderNav(){
  const isAdmin=page==='admin'||page==='admin-auth';
  document.getElementById('nav').innerHTML=navBtn('browse','Browse',!isAdmin)+navBtn('admin','Admin',isAdmin)
}
function navBtn(id,label,active){return '<button onclick="showPage(\''+id+'\')" style="padding:5px 14px;border-radius:var(--r-sm);border:none;font-size:12px;font-weight:500;letter-spacing:.5px;text-transform:uppercase;color:'+(active?'var(--accent)':'var(--text3)')+';background:'+(active?'var(--surf2)':'none')+'">'+label+'</button>'}

async function showPage(p,id){
  page=p;if(id!==undefined)selId=id;
  renderNav();document.getElementById('main').innerHTML='';
  if(p==='browse')renderBrowse();
  else if(p==='detail')renderDetail(selId);
  else if(p==='admin')adminPw?renderAdmin():renderAdminAuth();
}

async function renderBrowse(){
  const m=document.getElementById('main');
  if(!metaCache.makes.length){try{metaCache=await apiFetch('/meta')}catch{}}
  m.innerHTML='<div style="padding:36px 0 24px;border-bottom:1px solid var(--border);margin-bottom:24px"><h1 style="font-family:\'Bebas Neue\',sans-serif;font-size:52px;letter-spacing:3px;line-height:1;margin-bottom:4px">FIND YOUR <span style="color:var(--accent)">RIG</span></h1><p style="color:var(--text3);font-size:13px;margin-bottom:22px">Structured specs database for RV sales professionals</p><div style="display:flex;gap:10px;max-width:560px"><input id="b-q" value="'+esc(filters.q)+'" placeholder="Search make, model, floorplan\u2026" style="flex:1;padding:10px 14px;font-size:14px" oninput="filters.q=this.value;debSearch()" onkeydown="if(event.key===\'Enter\')doSearch()"><button onclick="doSearch()" style="background:var(--accent);color:#000;border:none;border-radius:var(--r-sm);padding:10px 22px;font-weight:600;font-size:14px">Search</button></div></div>'+
  '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px;align-items:flex-end">'+
  fgrp('Make','<select onchange="filters.make=this.value;doSearch()" style="padding:7px 10px;font-size:13px"><option value="">All Makes</option>'+metaCache.makes.map(m=>'<option value="'+esc(m)+'"'+(filters.make===m?' selected':'')+'>'+esc(m)+'</option>').join('')+'</select>')+
  fgrp('Year','<select onchange="filters.year=this.value;doSearch()" style="padding:7px 10px;font-size:13px"><option value="">All Years</option>'+Array.from({length:35},(_,i)=>new Date().getFullYear()+1-i).map(y=>'<option value="'+y+'"'+(filters.year==y?' selected':'')+'>'+y+'</option>').join('')+'</select>')+
  fgrp('RV Type','<select onchange="filters.rv_type=this.value;doSearch()" style="padding:7px 10px;font-size:13px"><option value="">All Types</option>'+metaCache.rv_types.map(t=>'<option value="'+esc(t)+'"'+(filters.rv_type===t?' selected':'')+'>'+esc(t)+'</option>').join('')+'</select>')+
  fgrp('Min Len','<input type="number" value="'+filters.min_length+'" placeholder="ft" style="width:80px;padding:7px 10px;font-size:13px" oninput="filters.min_length=this.value;debSearch()">')+
  fgrp('Max Len','<input type="number" value="'+filters.max_length+'" placeholder="ft" style="width:80px;padding:7px 10px;font-size:13px" oninput="filters.max_length=this.value;debSearch()">')+
  fgrp('Min Sleeps','<input type="number" value="'+filters.sleeps+'" placeholder="e.g. 4" style="width:80px;padding:7px 10px;font-size:13px" oninput="filters.sleeps=this.value;debSearch()">')+
  fgrp('Min Slides','<input type="number" value="'+filters.slides+'" placeholder="e.g. 2" style="width:80px;padding:7px 10px;font-size:13px" oninput="filters.slides=this.value;debSearch()">')+
  '<button onclick="clearFilters()" style="background:none;border:1px solid var(--border);color:var(--text3);border-radius:var(--r-sm);padding:7px 12px;font-size:12px">Clear</button></div>'+
  '<div id="results-area"></div>';
  doSearch();
}

function fgrp(label,inner){return '<div style="display:flex;flex-direction:column;gap:4px"><label style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text3);font-weight:600">'+label+'</label>'+inner+'</div>'}
let debTimer=null;
function debSearch(){clearTimeout(debTimer);debTimer=setTimeout(doSearch,350)}
function clearFilters(){filters={q:'',make:'',year:'',rv_type:'',min_length:'',max_length:'',sleeps:'',slides:''};bPage=0;renderBrowse()}

async function doSearch(offset){
  if(offset!==undefined)bPage=Math.floor(offset/PAGE_SZ);
  const el=document.getElementById('results-area');if(!el)return;
  el.innerHTML='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:12px">'+Array(6).fill(0).map(()=>'<div class="sk" style="height:200px"></div>').join('')+'</div>';
  try{
    const p=new URLSearchParams(Object.entries({...filters,limit:PAGE_SZ,offset:bPage*PAGE_SZ}).filter(([,v])=>v!=null&&v!==''));
    const data=await apiFetch('/floorplans?'+p);
    renderResults(data);
  }catch(e){el.innerHTML='<div style="padding:40px;text-align:center;color:var(--err)">Error: '+esc(e.message)+'</div>'}
}

function renderResults({rows,total}){
  const el=document.getElementById('results-area');if(!el)return;
  const pages=Math.ceil(total/PAGE_SZ);
  let html='<div style="font-size:13px;color:var(--text3);margin-bottom:14px"><strong style="color:var(--text)">'+total.toLocaleString()+'</strong> floorplans'+(total===0?' — run a crawl from Admin to populate data':'')+'</div>';
  if(!rows.length){
    html+='<div style="text-align:center;padding:80px 0;color:var(--text3)"><div style="font-size:48px;margin-bottom:12px">\uD83D\uDE90</div><div style="font-size:20px;color:var(--text);margin-bottom:8px">No results</div><p style="font-size:13px">Go to Admin panel and trigger a crawl to start importing RV specs</p></div>';
  } else {
    html+='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:12px">';
    rows.forEach((fp,i)=>{
      const conf=Math.round((fp.parse_confidence||0)*100);
      const cCol=conf>=80?'var(--ok)':conf>=50?'var(--warn)':'var(--err)';
      html+='<div onclick="showPage(\'detail\','+fp.id+')" class="fade" style="background:var(--surf);border:1px solid var(--border);border-radius:var(--r);padding:16px;cursor:pointer;transition:all .15s;animation-delay:'+(i*20)+'ms;position:relative;overflow:hidden" onmouseenter="this.style.borderColor=\'var(--accent)\';this.style.transform=\'translateY(-2px)\'" onmouseleave="this.style.borderColor=\'var(--border)\';this.style.transform=\'none\'">'+'<div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,'+cCol+' '+conf+'%,var(--border) 0)"></div>'+'<div style="font-size:10px;text-transform:uppercase;letter-spacing:2px;color:var(--accent);font-weight:600;margin-bottom:3px">'+(fp.year||'—')+'</div>'+'<div style="font-size:16px;font-weight:600;margin-bottom:2px">'+esc(fp.make)+' '+esc(fp.model)+'</div>'+'<div style="font-size:12px;color:var(--text3);margin-bottom:10px">'+esc(fp.floorplan||'Base Floorplan')+'</div>'+(fp.rv_type?'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:var(--surf2);color:var(--text3);margin-bottom:10px">'+esc(fp.rv_type)+'</span>':'')+'<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;border-top:1px solid var(--border);padding-top:10px">'+sm('Length',fp.length_ft?fp.length_ft+"'":null)+sm('Dry Wt',fp.dry_weight_lbs?num(fp.dry_weight_lbs)+' lbs':null)+sm('Sleeps',fp.sleep_capacity)+sm('MSRP',fp.msrp?'$'+num(fp.msrp):null)+'</div><div style="margin-top:10px;display:flex;align-items:center;gap:8px"><div style="flex:1;height:3px;background:var(--surf3);border-radius:99px;overflow:hidden"><div style="width:'+conf+'%;height:100%;background:'+cCol+';border-radius:99px"></div></div><span style="font-size:10px;font-family:var(--mono);color:var(--text3)">'+conf+'%</span></div></div>';
    });
    html+='</div>';
  }
  if(pages>1){
    html+='<div style="display:flex;justify-content:center;gap:6px;margin-top:28px">';
    if(bPage>0)html+=pBtn('&larr; Prev',(bPage-1)*PAGE_SZ);
    for(let i=Math.max(0,bPage-2);i<=Math.min(pages-1,bPage+2);i++)html+='<button onclick="doSearch('+i*PAGE_SZ+')" style="padding:5px 12px;border-radius:var(--r-sm);border:none;background:'+(i===bPage?'var(--accent)':'var(--surf2)')+';color:'+(i===bPage?'#000':'var(--text)')+';font-size:12px">'+(i+1)+'</button>';
    if(bPage<pages-1)html+=pBtn('Next &rarr;',(bPage+1)*PAGE_SZ);
    html+='</div>';
  }
  el.innerHTML=html;
}
function sm(l,v){return '<div><div style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text3)">'+l+'</div><div style="font-size:13px;font-family:var(--mono);font-weight:500;color:'+(v!=null?'var(--text)':'var(--text3)')+'\">'+(v!=null?v:'—')+'</div></div>'}
function pBtn(l,o){return '<button onclick="doSearch('+o+')" style="padding:5px 12px;border-radius:var(--r-sm);border:1px solid var(--border);background:var(--surf);color:var(--text);font-size:12px">'+l+'</button>'}

async function renderDetail(id){
  const m=document.getElementById('main');
  m.innerHTML='<div style="padding:32px 0;text-align:center"><div style="display:inline-block;width:32px;height:32px;border:2px solid var(--border2);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite"></div></div>';
  try{
    const fp=await apiFetch('/floorplans/'+id);
    const conf=Math.round((fp.parse_confidence||0)*100);
    const cCol=conf>=80?'var(--ok)':conf>=50?'var(--warn)':'var(--err)';
    const features=Array.isArray(fp.features)?fp.features:[];
    m.innerHTML='<div class="fade"><button onclick="showPage(\'browse\')" style="display:flex;align-items:center;gap:6px;color:var(--text3);background:none;border:none;font-size:13px;margin:20px 0 18px;padding:0" onmouseenter="this.style.color=\'var(--accent)\'" onmouseleave="this.style.color=\'var(--text3)\'">← Back to results</button>'+
    '<div style="margin-bottom:28px;padding-bottom:24px;border-bottom:1px solid var(--border)">'+
    '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px">'+(fp.rv_type?badge(fp.rv_type,'var(--accent-lo)','var(--accent)'):'')+badge(fp.year||'—','var(--surf2)','var(--text3)')+'</div>'+
    '<h1 style="font-family:\'Bebas Neue\',sans-serif;font-size:46px;letter-spacing:2px;line-height:1.05;margin-bottom:6px">'+esc(fp.make)+' '+esc(fp.model)+'</h1>'+
    '<div style="color:var(--text3);font-size:15px;margin-bottom:12px">'+esc(fp.floorplan||'Base Floorplan')+'</div>'+
    (fp.msrp?'<div style="font-size:28px;font-weight:700;color:var(--accent);font-family:var(--mono)">$'+num(fp.msrp)+' MSRP</div>':'')+
    '<div style="margin-top:14px;max-width:280px"><div style="font-size:11px;color:var(--text3);margin-bottom:4px">Parse confidence</div><div style="display:flex;align-items:center;gap:8px"><div style="flex:1;height:5px;background:var(--surf3);border-radius:99px;overflow:hidden"><div style="width:'+conf+'%;height:100%;background:'+cCol+';border-radius:99px"></div></div><span style="font-size:12px;font-family:var(--mono);color:var(--text3)">'+conf+'%</span></div></div></div>'+
    '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px">'+
    sCard('📐 Dimensions',[['Overall Length',fp.length_ft,v=>v+"'"],[' Overall Width',fp.width_ft,v=>v+"'"],['Overall Height',fp.height_ft,v=>v+"'"]])+
    sCard('⚖️ Weights',[['Dry Weight (UVW)',fp.dry_weight_lbs,v=>num(v)+' lbs'],['GVWR',fp.gvwr_lbs,v=>num(v)+' lbs'],['Hitch Weight',fp.hitch_weight_lbs,v=>num(v)+' lbs'],['Pin Weight',fp.pin_weight_lbs,v=>num(v)+' lbs'],['Payload',fp.payload_lbs,v=>num(v)+' lbs']])+
    sCard('💧 Tanks',[['Fresh Water',fp.fresh_water_gal,v=>v+' gal'],['Gray Water',fp.gray_water_gal,v=>v+' gal'],['Black Water',fp.black_water_gal,v=>v+' gal'],['Fuel',fp.fuel_gal,v=>v+' gal'],['LP Propane',fp.lp_lbs,v=>v+' lbs']])+
    sCard('🏕 Living',[['Sleeping Capacity',fp.sleep_capacity,v=>String(v)],['Slide-Outs',fp.slides_count,v=>String(v)]])+
    (fp.engine?sCard('🔧 Powertrain',[['Engine',fp.engine,v=>v],['Horsepower',fp.horsepower,v=>v+' hp'],['Transmission',fp.transmission,v=>v],['Chassis',fp.chassis,v=>v]]):'')+(features.length?'<div style="background:var(--surf);border:1px solid var(--border);border-radius:var(--r);overflow:hidden"><div style="padding:10px 16px;background:var(--surf2);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px;color:var(--text3);border-bottom:1px solid var(--border)">✨ Features</div><div style="padding:14px 16px;display:flex;flex-direction:column;gap:6px">'+features.map(f=>'<div style="display:flex;gap:8px;font-size:13px"><span style="color:var(--accent);font-weight:700;flex-shrink:0">›</span><span>'+esc(f)+'</span></div>').join('')+'</div></div>':'')+
    (fp.parse_notes?'<div style="grid-column:1/-1;background:rgba(245,158,11,.07);border:1px solid rgba(245,158,11,.2);border-radius:var(--r);padding:12px 16px;font-size:13px;color:var(--warn)"><strong style="display:block;margin-bottom:4px">⚠ Parse notes</strong>'+esc(fp.parse_notes)+'</div>':'')+
    '<div style="grid-column:1/-1;font-size:11px;color:var(--text3);font-family:var(--mono)">Source: <a href="'+fp.source_url+'" target="_blank" style="color:var(--accent)">'+fp.source_url+'</a> · Scraped '+(fp.scraped_at||'').slice(0,10)+'</div></div></div>';
  }catch(e){document.getElementById('main').innerHTML='<div style="padding:40px;text-align:center;color:var(--err)">Error: '+esc(e.message)+'</div>'}
}
function badge(txt,bg,col){return '<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600;background:'+bg+';color:'+col+'">'+esc(String(txt))+'</span>'}
function sCard(title,rows){
  const vis=rows.filter(([,v])=>v!=null);if(!vis.length)return '';
  return '<div style="background:var(--surf);border:1px solid var(--border);border-radius:var(--r);overflow:hidden"><div style="padding:10px 16px;background:var(--surf2);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px;color:var(--text3);border-bottom:1px solid var(--border)">'+title+'</div>'+rows.map(([l,v,f])=>'<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 16px;border-bottom:1px solid var(--border);gap:16px"><span style="font-size:12px;color:var(--text3)">'+l+'</span><span style="font-family:var(--mono);font-size:12px;font-weight:500;color:'+(v!=null?'var(--text)':'var(--text3)')+';font-style:'+(v==null?'italic':'normal')+'">'+(v!=null?(f?f(v):v):'N/A')+'</span></div>').join('')+'</div>'
}

function renderAdminAuth(){
  document.getElementById('main').innerHTML='<div class="fade" style="max-width:360px;margin:70px auto;background:var(--surf);border:1px solid var(--border);border-radius:var(--r);padding:32px;text-align:center"><div style="font-family:\'Bebas Neue\',sans-serif;font-size:36px;letter-spacing:2px;margin-bottom:6px">ADMIN</div><p style="color:var(--text3);font-size:13px;margin-bottom:24px">Enter your admin password</p><div style="display:flex;flex-direction:column;gap:10px"><input type="password" id="pw-in" placeholder="Admin password" style="padding:10px 14px;font-size:14px;width:100%" onkeydown="if(event.key===\'Enter\')tryLogin()"><button onclick="tryLogin()" style="background:var(--accent);color:#000;border:none;border-radius:var(--r-sm);padding:10px;font-weight:600;font-size:14px">Enter</button><div id="auth-err" style="color:var(--err);font-size:12px;display:none">Incorrect password</div></div></div>';
}

async function tryLogin(){
  const v=document.getElementById('pw-in')?.value||'';
  try{await apiFetch('/admin/auth-check',{headers:{'X-Admin-Password':v}});adminPw=v;renderAdmin()}
  catch{document.getElementById('auth-err').style.display='block'}
}

async function renderAdmin(){
  page='admin';renderNav();
  const m=document.getElementById('main');
  let stats={floorplans:{total:0,byType:[],byMake:[],lowConf:0},urls:[]},runs=[],errors=[],logs=[];
  try{[stats,runs,errors,logs]=await Promise.all([apiFetch('/stats'),apiFetch('/admin/runs',{headers:adminHeaders()}),apiFetch('/admin/errors',{headers:adminHeaders()}),apiFetch('/admin/logs',{headers:adminHeaders()})])}catch{}
  const uc={};stats.urls.forEach(u=>{uc[u.status]=(uc[u.status]||0)+u.count});
  const maxT=stats.floorplans.byType[0]?.c||1,maxM=stats.floorplans.byMake[0]?.c||1;
  m.innerHTML='<div class="fade"><div style="display:flex;align-items:flex-end;justify-content:space-between;padding-top:24px;margin-bottom:24px;padding-bottom:20px;border-bottom:1px solid var(--border)"><h1 style="font-family:\'Bebas Neue\',sans-serif;font-size:40px;letter-spacing:2px;line-height:1">ADMIN <span style="color:var(--accent)">PANEL</span></h1><button onclick="renderAdmin()" style="background:none;border:1px solid var(--border);color:var(--text3);border-radius:var(--r-sm);padding:6px 14px;font-size:12px">↺ Refresh</button></div>'+
  '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px;margin-bottom:28px">'+tile('Floorplans',stats.floorplans.total,'var(--accent)')+tile('Low Conf',stats.floorplans.lowConf,'var(--warn)')+tile('Makes',stats.floorplans.byMake.length,'var(--info)')+tile('URL Done',uc.done||0,'var(--ok)')+tile('Pending',uc.pending||0,'var(--text3)')+tile('Errors',uc.error||0,'var(--err)')+'</div>'+
  '<div style="display:flex;gap:0;border-bottom:1px solid var(--border);margin-bottom:20px" id="adm-tabs">'+aTab('ov','Overview',true)+aTab('cr','Crawl',false)+aTab('er','Errors ('+errors.length+')',false)+aTab('lg','Logs',false)+'</div><div id="adm-body"></div></div>';
  window._ad={stats,runs,errors,logs,maxT,maxM};showATab('ov');startSSE();
}

function tile(label,val,color){return '<div style="background:var(--surf);border:1px solid var(--border);border-radius:var(--r);padding:14px;text-align:center"><div style="font-family:var(--mono);font-size:24px;font-weight:500;color:'+color+'">'+num(val)+'</div><div style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text3);margin-top:4px">'+label+'</div></div>'}
function aTab(id,label,active){return '<button onclick="showATab(\''+id+'\')" id="at-'+id+'" style="padding:8px 18px;font-size:13px;border:none;cursor:pointer;background:none;font-family:var(--sans);font-weight:500;color:'+(active?'var(--accent)':'var(--text3)')+';border-bottom:2px solid '+(active?'var(--accent)':'transparent')+';margin-bottom:-1px">'+label+'</button>'}

function showATab(id){
  document.querySelectorAll('[id^="at-"]').forEach(b=>{const a=b.id==='at-'+id;b.style.color=a?'var(--accent)':'var(--text3)';b.style.borderBottomColor=a?'var(--accent)':'transparent'});
  const {stats,runs,errors,logs,maxT,maxM}=window._ad||{};
  const body=document.getElementById('adm-body');if(!body)return;
  if(id==='ov'){
    body.innerHTML='<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">'+surf('By RV Type',(stats?.floorplans?.byType||[]).map(r=>bRow(r.rv_type||'Unknown',r.c,maxT)).join(''))+surf('Top Makes',(stats?.floorplans?.byMake||[]).map(r=>bRow(r.make||'Unknown',r.c,maxM)).join(''))+'</div>';
  }else if(id==='cr'){
    body.innerHTML='<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">'+
    surf('Trigger Crawl','<div style="display:flex;flex-direction:column;gap:12px"><button onclick="triggerCrawl(\'full\')" style="background:var(--accent);color:#000;border:none;border-radius:var(--r-sm);padding:10px;font-weight:600;font-size:13px">▶ Full Crawl — All Makes</button><hr style="border:none;border-top:1px solid var(--border)"><label style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text3)">Make Name</label><input id="cr-make" placeholder="e.g. Forest River" style="padding:8px 12px;font-size:13px"><button onclick="triggerCrawl(\'make\')" style="background:none;border:1px solid var(--border);color:var(--text);border-radius:var(--r-sm);padding:8px;font-size:13px">▶ Crawl by Make</button><hr style="border:none;border-top:1px solid var(--border)"><label style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text3)">Specific URL</label><input id="cr-url" placeholder="https://www.rvusa.com/rv-guide/\u2026" style="padding:8px 12px;font-size:13px"><button onclick="triggerCrawl(\'url\')" style="background:none;border:1px solid var(--border);color:var(--text);border-radius:var(--r-sm);padding:8px;font-size:13px">▶ Crawl URL</button><hr style="border:none;border-top:1px solid var(--border)"><button onclick="requeueErrors()" style="background:var(--err);color:#fff;border:none;border-radius:var(--r-sm);padding:7px;font-size:12px">↺ Requeue All Errors</button></div>')+
    surf('Live Feed','<div id="live-feed" style="max-height:340px;overflow-y:auto;font-size:11px;font-family:var(--mono)"><span style="color:var(--text3);display:block;padding:20px 0;text-align:center">Start a crawl to see live logs</span></div>')+
    surf('Run History',runs.map(r=>'<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--surf2);border-radius:var(--r-sm);margin-bottom:6px;flex-wrap:wrap"><span style="font-family:var(--mono);font-size:11px;color:var(--text3);width:28px">#'+r.id+'</span><span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:'+(r.status==='done'?'rgba(16,185,129,.1)':'rgba(245,158,11,.1)')+';color:'+(r.status==='done'?'var(--ok)':'var(--warn)')+'">'+r.status+'</span><span style="font-size:12px;flex:1">'+r.trigger+(r.scope?' — '+r.scope:'')+'</span><span style="font-size:11px;color:var(--text3)">+'+r.records_added+' added · '+r.urls_crawled+' crawled</span></div>').join('')||'<div style="color:var(--text3);font-size:13px;text-align:center;padding:20px">No runs yet</div>',null,'1/-1')+'</div>';
  }else if(id==='er'){
    body.innerHTML=surf('Error URLs',errors.length?'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12px"><thead><tr>'+['URL','Type','HTTP','Retries','Error'].map(h=>'<th style="padding:8px 12px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text3);background:var(--surf2);border-bottom:1px solid var(--border)">'+h+'</th>').join('')+'</tr></thead><tbody>'+errors.map(r=>'<tr style="border-bottom:1px solid var(--border)"><td style="padding:8px 12px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><a href="'+r.url+'" target="_blank" style="color:var(--accent)">'+r.url+'</a></td><td style="padding:8px 12px">'+r.type+'</td><td style="padding:8px 12px">'+(r.http_status||'—')+'</td><td style="padding:8px 12px">'+r.retry_count+'</td><td style="padding:8px 12px;color:var(--err);max-width:200px">'+esc(r.error||'')+'</td></tr>').join('')+'</tbody></table></div>':'<div style="text-align:center;padding:32px;color:var(--text3)">\uD83C\uDF89 No errors</div>');
  }else if(id==='lg'){
    body.innerHTML=surf('Recent Logs',logs.length?logs.map(l=>{const col={info:'var(--text2)',warn:'var(--warn)',error:'var(--err)',debug:'var(--text3)'}[l.level];return'<div style="display:grid;grid-template-columns:52px 44px 1fr;gap:8px;padding:5px 12px;border-bottom:1px solid var(--border);font-size:11px;font-family:var(--mono)"><span style="color:var(--text3)">'+(l.created_at||'').slice(11,16)+'</span><span style="color:'+col+'">'+l.level.toUpperCase()+'</span><span style="word-break:break-all">'+esc(l.message)+(l.url?'<span style="display:block;color:var(--text3);font-size:10px">'+esc(l.url)+'</span>':'')+'</span></div>'}).join(''):'<div style="color:var(--text3);font-size:13px;text-align:center;padding:20px">No logs yet</div>');
  }
}

function surf(title,inner,action,col){return '<div style="background:var(--surf);border:1px solid var(--border);border-radius:var(--r);overflow:hidden'+(col?';grid-column:'+col:'')+'"><div style="padding:10px 16px;background:var(--surf2);font-size:12px;font-weight:600;border-bottom:1px solid var(--border)">'+title+'</div><div style="padding:14px 16px">'+inner+'</div></div>'}
function bRow(label,val,max){const p=Math.round(val/max*100);return '<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px"><span style="color:var(--text2)">'+esc(label)+'</span><span style="font-family:var(--mono);font-size:11px">'+val+'</span></div><div style="height:4px;background:var(--surf3);border-radius:99px;overflow:hidden"><div style="width:'+p+'%;height:100%;background:var(--accent);border-radius:99px"></div></div></div>'}

async function triggerCrawl(type){
  let body={};
  if(type==='make'){const v=document.getElementById('cr-make')?.value?.trim();if(!v){toast('Enter a make name','warn');return;}body={make:v}}
  if(type==='url'){const v=document.getElementById('cr-url')?.value?.trim();if(!v){toast('Enter a URL','warn');return;}body={url:v}}
  try{await apiFetch('/admin/crawl',{method:'POST',headers:adminHeaders(),body:JSON.stringify(body)});toast('Crawl started — watch the live feed','success')}
  catch(e){toast('Failed: '+e.message,'error')}
}

async function requeueErrors(){
  try{await apiFetch('/admin/requeue-errors',{method:'POST',headers:adminHeaders()});toast('Errors requeued','success');renderAdmin()}
  catch(e){toast('Failed: '+e.message,'error')}
}

let sseSource=null;
function startSSE(){
  if(sseSource)sseSource.close();
  sseSource=new EventSource('/api/admin/crawl/stream?admin_password='+encodeURIComponent(adminPw));
  sseSource.onmessage=e=>{
    const d=JSON.parse(e.data);if(d.idle)return;
    const feed=document.getElementById('live-feed');if(!feed)return;
    if(d.recentLogs?.length){
      const col={info:'var(--text2)',warn:'var(--warn)',error:'var(--err)',debug:'var(--text3)'};
      feed.innerHTML=d.recentLogs.map(l=>'<div style="display:grid;grid-template-columns:52px 44px 1fr;gap:8px;padding:5px 12px;border-bottom:1px solid var(--border)"><span style="color:var(--text3)">'+(l.created_at||'').slice(11,16)+'</span><span style="color:'+(col[l.level]||'var(--text)')+'">'+l.level.toUpperCase()+'</span><span style="word-break:break-all">'+esc(l.message)+(l.url?'<span style="display:block;color:var(--text3);font-size:10px">'+esc(l.url)+'</span>':'')+'</span></div>').join('');
      feed.scrollTop=feed.scrollHeight;
    }
  };
}

showPage('browse');
</script>
</body>
</html>`

app.get(/^(?!\/api).*/, (req, res) => res.send(HTML))

app.listen(PORT, () => {
  console.log(`\n🚐 LotProRV running at http://localhost:${PORT}`)
  console.log(`   Admin password: ${ADMIN_PASSWORD}\n`)
})
