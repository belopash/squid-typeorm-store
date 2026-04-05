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
     * `fromQuery` — the entity came from a TypeORM query; replaces any existing
     * instance and captures a baseline snapshot for dirty detection.
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
            cached.value = entity
            cached.loadedFromDb = true
            cached.baseline = captureColumnSnapshot(metadata, entity)
            this.logger?.debug(`replaced entity from query ${metadata.name} ${entity.id}`)
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
