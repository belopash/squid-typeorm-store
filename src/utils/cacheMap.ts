import {EntityMetadata} from 'typeorm'
import {EntityLiteral} from './misc'
import {Logger} from '@subsquid/logger'
import clone from 'fast-copy'

export class CachedEntity<E extends EntityLiteral = EntityLiteral> {
    constructor(public value: E | null = null) {}
}

export class CacheMap {
    private map: Map<EntityMetadata, Map<string, CachedEntity>> = new Map()
    private logger?: Logger

    constructor(logger?: Logger) {
        this.logger = logger?.child('cache')
    }

    get(metadata: EntityMetadata, id: string) {
        return this.getEntityCache(metadata)?.get(id)
    }

    has(metadata: EntityMetadata, id: string): boolean {
        const cacheMap = this.getEntityCache(metadata)
        const cachedEntity = cacheMap.get(id)
        return !!cachedEntity?.value
    }

    settle(metadata: EntityMetadata, id: string): void {
        const cacheMap = this.getEntityCache(metadata)

        if (cacheMap.has(id)) return

        cacheMap.set(id, new CachedEntity())
        this.logger?.debug(`added empty entity ${metadata.name} ${id}`)
    }

    delete(metadata: EntityMetadata, id: string): void {
        const cacheMap = this.getEntityCache(metadata)
        cacheMap.set(id, new CachedEntity())
        this.logger?.debug(`deleted entity ${metadata.name} ${id}`)
    }

    clear(): void {
        this.logger?.debug(`cleared`)
        this.map.clear()
    }

    add<E extends EntityLiteral>(metadata: EntityMetadata, entity: E, opts?: {nullify?: boolean, override?: boolean}): void {
        const cacheMap = this.getEntityCache(metadata)

        let cached = cacheMap.get(entity.id)
        if (cached == null) {
            cached = new CachedEntity()
            cacheMap.set(entity.id, cached)
        }

        if (cached.value == null) {
            cached.value = metadata.create() as E
            cached.value.id = entity.id
            this.logger?.debug(`added entity ${metadata.name} ${entity.id}`)
        }

        const cachedEntity = cached.value

        for (const column of metadata.nonVirtualColumns) {
            const objectColumnValue = column.getEntityValue(entity)
            const cachedColumnValue = column.getEntityValue(cachedEntity)
            if (!opts?.override && cachedColumnValue !== undefined) continue
            if (!opts?.nullify && objectColumnValue === undefined) continue
            if (objectColumnValue === cachedColumnValue && objectColumnValue !== undefined) continue
            column.setEntityValue(cachedEntity, clone(objectColumnValue ?? null))
        }

        for (const relation of metadata.relations) {
            if (!relation.isOwning) continue

            const inverseEntity = relation.getEntityValue(entity)
            const cachedInverseEntity = relation.getEntityValue(cachedEntity)

            if (!opts?.override && cachedInverseEntity !== undefined) continue
            if (!opts?.nullify && inverseEntity === undefined) continue
            if (inverseEntity?.id === cachedInverseEntity?.id && inverseEntity != null) continue

            const inverseMetadata = relation.inverseEntityMetadata
            const mockEntity = inverseEntity == null ? null : inverseMetadata.create()
            if (mockEntity != null) {
                mockEntity.id = inverseEntity.id
            } 
            relation.setEntityValue(cachedEntity, mockEntity)
        }
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
