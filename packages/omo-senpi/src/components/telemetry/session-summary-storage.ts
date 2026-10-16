import { linkSync, unlinkSync, writeFileSync, mkdirSync } from "node:fs"
import { join } from "node:path"
import type { EventTelemetryProperties } from "@oh-my-opencode/telemetry-core"

import { CORRUPT, listNames, readJsonOrCorrupt, removeFile, renameIfPresent, writeJsonAtomic } from "./session-summary-files"
import { createOwnerRegistry, parseOwner, processIsAlive, type ProcessOwner } from "./session-summary-owner"
import { parseTelemetrySessionRole, type TelemetrySessionRole } from "./session-role"
import { SUMMARY_EPOCH_SHAPE, isSummaryWindow, type SummaryWindow } from "./session-summary-window"

export const SUMMARY_TTL_MS = 14 * 24 * 60 * 60_000
export const OWNER_STALE_MS = 5 * 60_000
const OWNER_ENTRY_REFRESH_MS = 60_000

/**
 * The consent of the session a queued record came from. Only `enabled` records are ever sent; anything else,
 * including a record written before this mark existed, is deleted unsent (fail closed).
 */
export type RecordConsent = "enabled" | "unconsented"

/** Where the writing session read its consent: before a record is sent or resumed, consent is re-read for it. */
export type ConsentScope = { readonly cwd?: string }

/**
 * The single send/resume gate for a persisted record: it must carry the `enabled` mark AND its originating
 * scope must still be opted in now. A record whose scope cannot be read back (older file) or names no cwd
 * fails closed: consent read without a cwd would be the reading process's default, not the writer's.
 */
export function isRecordSendable(
  record: { readonly consent: RecordConsent; readonly scope: ConsentScope | undefined },
  isScopeConsented: (scope: ConsentScope) => boolean,
): boolean {
  const cwd = record.scope?.cwd
  return record.consent === "enabled" && cwd !== undefined && cwd.length > 0 && isScopeConsented({ cwd })
}

export type WindowRecord = {
  readonly consent: RecordConsent
  /** Absent on records older than the scope; such a record is never sent or resumed. */
  readonly scope: ConsentScope | undefined
  readonly sessionHash: string
  readonly sessionRole: TelemetrySessionRole
  readonly owner: ProcessOwner
  readonly heartbeatAt: number
  /** The run `seq` counts in; absent on records older than the epoch (a continuer starts a new one). */
  readonly epoch: string | undefined
  /** The `summary_seq` the open window will carry, or the next window when none is open. */
  readonly seq: number
  readonly sampleRate: number
  readonly windowId: string | null
  readonly window: SummaryWindow | null
}

export type PendingRecord = {
  readonly consent: RecordConsent
  readonly scope: ConsentScope | undefined
  readonly uuid: string
  readonly sessionHash: string
  readonly owner: ProcessOwner
  readonly createdAt: number
  readonly properties: EventTelemetryProperties
}

export type Claimed<T> = { readonly record: T; readonly claimPath: string }
export type ReadResult<T> = T | typeof CORRUPT | undefined

export type SummaryStorage = {
  readonly owner: ProcessOwner
  readonly now: () => number
  readonly isOwnerAlive: (owner: ProcessOwner) => boolean
  readonly windowKey: (sessionHash: string, ownCopy?: boolean) => string
  /** A key one process wrote beside another process's window; nobody continues it once its writer is gone. */
  readonly isOwnCopyKey: (key: string) => boolean
  readonly readWindow: (key: string) => ReadResult<WindowRecord>
  readonly writeWindow: (key: string, record: WindowRecord) => void
  /** Exclusive create: never replaces a window another process wrote in the meantime. */
  readonly createWindow: (key: string, record: WindowRecord) => boolean
  readonly removeWindow: (key: string) => void
  readonly windowKeys: () => readonly string[]
  readonly claimWindow: (key: string) => Claimed<ReadResult<WindowRecord>> | undefined
  readonly writePending: (record: PendingRecord) => void
  readonly readPending: (uuid: string) => ReadResult<PendingRecord>
  readonly removePending: (uuid: string) => void
  readonly hasPending: (uuid: string) => boolean
  readonly pendingUuids: () => readonly string[]
  readonly claimPending: (uuid: string) => Claimed<ReadResult<PendingRecord>> | undefined
  readonly leftovers: () => readonly Leftover[]
}

export type Leftover = { readonly path: string; readonly owner: ProcessOwner; readonly kind: "temp" | "window-claim" | "pending-claim" }

const SAFE_NAME = /^[A-Za-z0-9:_-]{1,128}$/
const SAFE_UUID = /^[0-9a-f-]{36}$/
const TEMP_FILE = /^.+\.(\d+)-([A-Za-z0-9-]{1,64})\.tmp$/
const CLAIM_FILE = /^.+\.claimed-(\d+)-([A-Za-z0-9-]{1,64})$/

export function summaryDir(stateDir: string): string {
  return join(stateDir, "session-summary")
}

export function createSummaryStorage(input: {
  readonly stateDir: string
  readonly owner: ProcessOwner
  readonly isPidAlive?: (pid: number) => boolean
  /** Start-time probe the owner registry tells a reused pid apart with; defaults to the real one. */
  readonly processStartTime?: (pid: number) => string | null
  readonly now?: () => number
}): SummaryStorage {
  const dir = summaryDir(input.stateDir)
  const pendingDir = join(dir, "pending")
  const owner = input.owner
  const now = input.now ?? Date.now
  const registry = createOwnerRegistry({
    dir,
    owner,
    isPidAlive: input.isPidAlive ?? processIsAlive,
    ...(input.processStartTime === undefined ? {} : { processStartTime: input.processStartTime }),
    now,
  })
  const claimSuffix = `.claimed-${owner.pid}-${owner.bootId}`
  const windowPath = (key: string) => join(dir, `${key}.json`)
  const pendingPath = (uuid: string) => join(pendingDir, `${uuid}.json`)
  // The registry entry's heartbeat is what keeps an owner alive where its start time cannot be compared.
  let registeredAt: number | undefined
  const ensureRegistered = () => {
    if (registeredAt !== undefined && now() - registeredAt < OWNER_ENTRY_REFRESH_MS) return
    registeredAt = now()
    registry.register()
  }

  const claim = <T>(path: string, parse: (value: unknown) => T | undefined): Claimed<ReadResult<T>> | undefined => {
    const claimPath = `${path}${claimSuffix}`
    if (!renameIfPresent(path, claimPath)) return undefined
    return { claimPath, record: parseRead(readJsonOrCorrupt(claimPath), parse) }
  }

  return {
    owner,
    now,
    isOwnerAlive: registry.isAlive,
    windowKey: (sessionHash, ownCopy = false) => `${fileName(sessionHash)}${ownCopy ? `@${owner.bootId}` : ""}`,
    isOwnCopyKey: (key) => key.includes("@"),
    readWindow: (key) => parseRead(readJsonOrCorrupt(windowPath(key)), parseWindowRecord),
    writeWindow(key, record) {
      ensureRegistered()
      writeJsonAtomic(windowPath(key), { v: 2, ...record }, owner)
    },
    createWindow(key, record) {
      ensureRegistered()
      mkdirSync(dir, { recursive: true, mode: 0o700 })
      const temp = `${windowPath(key)}.${owner.pid}-${owner.bootId}.tmp`
      writeFileSync(temp, JSON.stringify({ v: 2, ...record }), { mode: 0o600 })
      try {
        linkSync(temp, windowPath(key))
        return true
      } catch {
        return false
      } finally {
        unlinkSync(temp)
      }
    },
    removeWindow: (key) => removeFile(windowPath(key)),
    windowKeys: () => listNames(dir).filter((name) => name.endsWith(".json")).map((name) => name.slice(0, -".json".length)),
    claimWindow: (key) => claim(windowPath(key), parseWindowRecord),
    writePending(record) {
      ensureRegistered()
      writeJsonAtomic(pendingPath(record.uuid), { v: 2, ...record }, owner)
    },
    readPending: (uuid) => parseRead(readJsonOrCorrupt(pendingPath(uuid)), parsePendingRecord),
    removePending: (uuid) => removeFile(pendingPath(uuid)),
    hasPending: (uuid) => listNames(pendingDir).some((name) => name === `${uuid}.json` || name.startsWith(`${uuid}.json.claimed-`)),
    pendingUuids: () => listNames(pendingDir).filter((name) => name.endsWith(".json")).map((name) => name.slice(0, -".json".length)),
    claimPending: (uuid) => claim(pendingPath(uuid), parsePendingRecord),
    leftovers: () => [
      ...collectLeftovers(dir, "window-claim"),
      ...collectLeftovers(pendingDir, "pending-claim"),
    ],
  }
}

function collectLeftovers(dir: string, claimKind: "window-claim" | "pending-claim"): Leftover[] {
  return listNames(dir).flatMap((name): Leftover[] => {
    const claimMatch = CLAIM_FILE.exec(name)
    const match = claimMatch ?? TEMP_FILE.exec(name)
    const owner = match === null ? undefined : parseOwner({ pid: Number(match[1]), bootId: match[2] })
    if (owner === undefined) return []
    return [{ path: join(dir, name), owner, kind: claimMatch === null ? "temp" : claimKind }]
  })
}

function parseRead<T>(value: unknown, parse: (value: unknown) => T | undefined): ReadResult<T> {
  if (value === undefined) return undefined
  if (value === CORRUPT) return CORRUPT
  return parse(value) ?? CORRUPT
}

export function parseWindowRecord(value: unknown): WindowRecord | undefined {
  if (typeof value !== "object" || value === null || Reflect.get(value, "v") !== 2) return undefined
  const owner = parseOwner(Reflect.get(value, "owner"))
  const sessionHash: unknown = Reflect.get(value, "sessionHash")
  const heartbeatAt: unknown = Reflect.get(value, "heartbeatAt")
  const seq: unknown = Reflect.get(value, "seq")
  const epoch: unknown = Reflect.get(value, "epoch")
  const sampleRate: unknown = Reflect.get(value, "sampleRate")
  const windowId: unknown = Reflect.get(value, "windowId")
  const window: unknown = Reflect.get(value, "window")
  if (owner === undefined || typeof sessionHash !== "string" || typeof heartbeatAt !== "number") return undefined
  if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0 || typeof sampleRate !== "number") return undefined
  const windowIsOpen = isSummaryWindow(window) && typeof windowId === "string" && SAFE_UUID.test(windowId)
  if (!windowIsOpen && window !== null) return undefined
  return {
    consent: parseConsent(Reflect.get(value, "consent")),
    scope: parseScope(Reflect.get(value, "scope")),
    sessionHash,
    sessionRole: parseTelemetrySessionRole(Reflect.get(value, "sessionRole")),
    owner,
    heartbeatAt,
    epoch: typeof epoch === "string" && SUMMARY_EPOCH_SHAPE.test(epoch) ? epoch : undefined,
    seq,
    sampleRate,
    windowId: windowIsOpen ? windowId : null,
    window: windowIsOpen ? window : null,
  }
}

export function parsePendingRecord(value: unknown): PendingRecord | undefined {
  if (typeof value !== "object" || value === null || Reflect.get(value, "v") !== 2) return undefined
  const owner = parseOwner(Reflect.get(value, "owner"))
  const uuid: unknown = Reflect.get(value, "uuid")
  const sessionHash: unknown = Reflect.get(value, "sessionHash")
  const createdAt: unknown = Reflect.get(value, "createdAt")
  const properties: unknown = Reflect.get(value, "properties")
  if (owner === undefined || typeof uuid !== "string" || !SAFE_UUID.test(uuid)) return undefined
  if (typeof sessionHash !== "string" || typeof createdAt !== "number") return undefined
  if (typeof properties !== "object" || properties === null || Array.isArray(properties)) return undefined
  return {
    consent: parseConsent(Reflect.get(value, "consent")),
    scope: parseScope(Reflect.get(value, "scope")),
    uuid,
    sessionHash,
    owner,
    createdAt,
    properties: { ...properties },
  }
}

function parseConsent(value: unknown): RecordConsent {
  return value === "enabled" ? "enabled" : "unconsented"
}

function parseScope(value: unknown): ConsentScope | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined
  const cwd: unknown = Reflect.get(value, "cwd")
  if (cwd === undefined) return {}
  return typeof cwd === "string" ? { cwd } : undefined
}

function fileName(sessionHash: string): string {
  return SAFE_NAME.test(sessionHash) ? sessionHash.replaceAll(":", "_") : Buffer.from(sessionHash).toString("hex").slice(0, 128)
}
