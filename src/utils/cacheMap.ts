import {EntityMetadata} from 'typeorm'
import {EntityLiteral} from './misc'
import {Logger} from '@subsquid/logger'

export function captureColumnSnapshot(metadata: EntityMetadata, entity: EntityLiteral): unknown[] {
    return metadata.nonVirtualColumns.map((col) => col.getEntityValue(entity, true))
}

function valuesEqual(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) return true
    if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime()
    return false
}

export function isSnapshotDirty(metadata: EntityMetadata, entity: EntityLiteral, baseline: unknown[]): boolean {
    const cols = metadata.nonVirtualColumns
    for (let i = 0; i < baseline.length; i++) {
        if (!valuesEqual(baseline[i], cols[i].getEntityValue(entity, true))) return true
    }
    return false
}

export class CachedEntity<E extends EntityLiteral = EntityLiteral> {
    value: E | null = null
    loadedFromDb = false
    baseline: unknown[] | null = null
}

export class CacheMap {
    private map: Map<EntityMetadata, Map<string, CachedEntity>> = new Map()
    private logger?: Logger

    constructor(logger?: Logger) {
        this.logger = logger?.child('cache')
    }

    get(metadata: EntityMetadata, id: string): CachedEntity | undefined {
        return this.getEntityCache(metadata).get(id)
    }

    has(metadata: EntityMetadata, id: string): boolean {
        return !!this.getEntityCache(metadata).get(id)?.value
    }

    settle(metadata: EntityMetadata, id: string): void {
        const cacheMap = this.getEntityCache(metadata)
        if (cacheMap.has(id)) return

        cacheMap.set(id, new CachedEntity())
        this.logger?.debug(`added empty entity ${metadata.name} ${id}`)
    }

    delete(metadata: EntityMetadata, id: string): void {
        this.getEntityCache(metadata).set(id, new CachedEntity())
        this.logger?.debug(`deleted entity ${metadata.name} ${id}`)
    }

    clear(): void {
        this.logger?.debug(`cleared`)
        this.map.clear()
    }

    /**
     * After a successful write, align baseline with the canonical entity so the next
     * flush does not treat unchanged rows as dirty.
     */
    syncBaselineAfterWrite(metadata: EntityMetadata, entity: EntityLiteral): void {
        const cached = this.get(metadata, entity.id)
        if (cached?.value == null) return
        cached.loadedFromDb = true
        cached.baseline = captureColumnSnapshot(metadata, cached.value)
    }

    /**
     * Store `entity` as the canonical instance for its id.
     *
     * `fromQuery` — the entity came from a TypeORM query. If no instance is cached yet,
     * stores it and captures a baseline snapshot for dirty detection. If a different
     * instance is already cached, the cached one is kept (it may be a reference already
     * handed back to user code in a concurrent read); the baseline is only refreshed
     * when that cached instance is still clean (no in-memory mutations vs. its baseline).
     * This avoids silently dropping mutations made through one reference when a parallel
     * `find()` re-loads the same row through a JOIN and traverses it through `persist`.
     *
     * `overwrite` — the caller explicitly requested upsert semantics (`replace: true`);
     * replaces any existing instance without touching `loadedFromDb` / `baseline`.
     *
     * Without either flag, a *different* object for an already-cached id throws.
     */
    add<E extends EntityLiteral>(metadata: EntityMetadata, entity: E, opts?: {fromQuery?: boolean; overwrite?: boolean}): void {
        const cacheMap = this.getEntityCache(metadata)

        let cached = cacheMap.get(entity.id)
        if (cached == null) {
            cached = new CachedEntity()
            cacheMap.set(entity.id, cached)
        }

        if (cached.value == null) {
            cached.value = entity
            if (opts?.fromQuery) {
                cached.loadedFromDb = true
                cached.baseline = captureColumnSnapshot(metadata, entity)
            }
            this.logger?.debug(`added entity ${metadata.name} ${entity.id}`)
            return
        }

        if (cached.value === entity) return

        if (opts?.fromQuery) {
            // Preserve the canonical cached instance: a concurrent reader may already
            // hold a reference to it and be about to mutate it. Replacing it here would
            // silently drop those mutations and reset the baseline to the freshly-loaded
            // (untouched) row, so dirty-detection at sync time would miss the change.
            const cachedValue = cached.value
            const cachedClean =
                cached.loadedFromDb &&
                cached.baseline != null &&
                !isSnapshotDirty(metadata, cachedValue, cached.baseline)
            if (cachedClean || !cached.loadedFromDb) {
                // Cached instance has no pending in-memory mutations (or has never been
                // associated with a DB baseline at all). Safe to align baseline to the
                // latest DB read so future dirty detection works against fresh data.
                cached.baseline = captureColumnSnapshot(metadata, cachedValue)
            }
            cached.loadedFromDb = true
            this.logger?.debug(`refreshed entity from query ${metadata.name} ${entity.id}`)
            return
        }

        if (opts?.overwrite) {
            cached.value = entity
            this.logger?.debug(`replaced entity (overwrite) ${metadata.name} ${entity.id}`)
            return
        }

        throw new Error(
            `Entity ${metadata.name} ${entity.id} is already in the store cache with a different object instance. ` +
                `Use getOrCreate() to obtain or create the canonical instance, or fetch it with get()/find() and mutate it in place. ` +
                `To intentionally replace a cached instance, pass { replace: true } to track().`
        )
    }

    private getEntityCache<E extends EntityLiteral>(metadata: EntityMetadata): Map<string, CachedEntity<E>> {
        let map = this.map.get(metadata)
        if (map == null) {
            map = new Map()
            this.map.set(metadata, map)
        }
        return map as Map<string, CachedEntity<E>>
    }
}
