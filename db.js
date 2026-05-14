// db.js — sql.js wrapper with disk persistence
// sql.js is pure JS (no native compilation needed)
// We load/save the DB file manually since sql.js works in-memory

import initSqlJs from 'sql.js'
import fs from 'fs'
import path from 'path'

const DB_PATH = process.env.DB_PATH || './lotprorv.db'

let _db = null
let _saveTimer = null

function scheduleSave() {
  clearTimeout(_saveTimer)
  _saveTimer = setTimeout(saveToDisk, 2000)
}

function saveToDisk() {
  if (!_db) return
  try {
    const data = _db.export()
    fs.writeFileSync(DB_PATH, Buffer.from(data))
  } catch (e) {
    console.error('DB save error:', e.message)
  }
}

export async function initDb() {
  if (_db) return _db
  const SQL = await initSqlJs()

  if (fs.existsSync(DB_PATH)) {
    const fileBuffer = fs.readFileSync(DB_PATH)
    _db = new SQL.Database(fileBuffer)
    console.log('✅ Loaded existing DB from', DB_PATH)
  } else {
    _db = new SQL.Database()
    console.log('✅ Created new DB at', DB_PATH)
  }

  // Save on process exit
  process.on('exit', saveToDisk)
  process.on('SIGINT', () => { saveToDisk(); process.exit() })
  process.on('SIGTERM', () => { saveToDisk(); process.exit() })

  return _db
}

export function getDb() {
  if (!_db) throw new Error('DB not initialized — call initDb() first')
  return _db
}

// Helpers that mimic better-sqlite3's synchronous API
export function dbRun(sql, params = []) {
  const db = getDb()
  db.run(sql, params)
  scheduleSave()
  return db
}

export function dbGet(sql, params = []) {
  const db = getDb()
  const stmt = db.prepare(sql)
  stmt.bind(params)
  if (stmt.step()) {
    const row = stmt.getAsObject()
    stmt.free()
    return row
  }
  stmt.free()
  return null
}

export function dbAll(sql, params = []) {
  const db = getDb()
  const stmt = db.prepare(sql)
  stmt.bind(params)
  const rows = []
  while (stmt.step()) rows.push(stmt.getAsObject())
  stmt.free()
  return rows
}

export function dbExec(sql) {
  getDb().run(sql)
  scheduleSave()
}

export function dbTransaction(fn) {
  const db = getDb()
  db.run('BEGIN')
  try {
    fn()
    db.run('COMMIT')
    scheduleSave()
  } catch (e) {
    db.run('ROLLBACK')
    throw e
  }
}

export function lastInsertRowId() {
  return dbGet('SELECT last_insert_rowid() as id')?.id
}
