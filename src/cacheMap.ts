import {Entity} from '@subsquid/typeorm-store'
import {EntityManager, EntityMetadata, EntityTarget} from 'typeorm'
import {copy} from './utils'
import {Logger} from '@subsquid/logger'

export class CachedEntity<E extends Entity> {
    value: E | null

    constructor() {
        this.value = null
    }
}

export class CacheMap {
    private map: Map<EntityMetadata, Map<string, CachedEntity<any>>> = new Map()
    private logger: Logger

    constructor(private em: () => EntityManager, private opts: {logger: Logger}) {
        this.logger = this.opts.logger.child('cache')
    }

    exist<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const metadata = this.getEntityMetadata(entityClass)
        const cacheMap = this.getEntityCache(metadata)
        const cachedEntity = cacheMap.get(id)
        return !!cachedEntity?.value
    }

    get<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const metadata = this.getEntityMetadata(entityClass)
        const cacheMap = this.getEntityCache(metadata)
        return cacheMap.get(id) as CachedEntity<E> | undefined
    }

    ensure<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const metadata = this.getEntityMetadata(entityClass)
        const cacheMap = this.getEntityCache(metadata)

        if (cacheMap.has(id)) return

        cacheMap.set(id, new CachedEntity())
        this.logger.debug(`added empty entity ${metadata.name} ${id}`)
    }

    delete<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const metadata = this.getEntityMetadata(entityClass)
        const cacheMap = this.getEntityCache(metadata)
        cacheMap.set(id, new CachedEntity())
        this.logger.debug(`deleted entity ${metadata.name} ${id}`)
    }

    clear() {
        for (const [name, item] of this.map) {
            if (item.size > 0) {
                item.clear()
                this.logger.debug(`cleared cache for ${name} (${item.size})`)
            }
        }
        this.map.clear()
    }

    add<E extends Entity>(e: E | E[], isNew?: boolean) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length === 0) return

        for (const entity of entities) {
            this.cacheEntity(entity, isNew)
        }
    }

    private cacheEntity(entity: Entity, isNew = false) {
        const metadata = this.getEntityMetadata(entity.constructor)
        const cacheMap = this.getEntityCache(metadata)

        let cached = cacheMap.get(entity.id)
        if (cached == null) {
            cached = new CachedEntity()
            cacheMap.set(entity.id, cached)
        }

        let cachedEntity = cached.value
        if (cachedEntity == null) {
            cachedEntity = cached.value = metadata.create()
            cachedEntity.id = entity.id
            this.logger.debug(`added entity ${metadata.name} ${entity.id}`)
        }

        for (const column of metadata.nonVirtualColumns) {
            const objectColumnValue = column.getEntityValue(entity)
            if (objectColumnValue !== undefined) {
                column.setEntityValue(cachedEntity, copy(objectColumnValue))
            }
        }

        for (const relation of metadata.relations) {
            const inverseEntity = relation.getEntityValue(entity)
            const inverseMetadata = relation.inverseEntityMetadata

            if (relation.isOwning) {
                if (inverseEntity == null) {
                    if (inverseEntity === null || isNew) {
                        relation.setEntityValue(cachedEntity, null)
                    }
                } else {
                    const relationCacheMap = this.getEntityCache(inverseMetadata)
                    const cachedRelation = relationCacheMap.get(inverseEntity.id)?.value

                    if (cachedRelation == null) {
                        throw new Error(`Missing entity ${inverseMetadata.name} with id ${inverseEntity.id}`)
                    }

                    relation.setEntityValue(cachedEntity, cachedRelation)
                }
            }
        }
    }

    private getEntityCache(metadata: EntityMetadata) {
        let map = this.map.get(metadata)
        if (map == null) {
            map = new Map()
            this.map.set(metadata, map)
        }

        return map
    }

    private getEntityMetadata(entityClass: EntityTarget<any>) {
        const em = this.em()
        return em.connection.getMetadata(entityClass)
    }
}
