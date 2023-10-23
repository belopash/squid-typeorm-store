import {Entity} from '@subsquid/typeorm-store'
import {EntityManager, EntityTarget, FindOptionsRelations} from 'typeorm'
import {ColumnMetadata} from 'typeorm/metadata/ColumnMetadata'
import {RelationMetadata} from 'typeorm/metadata/RelationMetadata'
import {copy} from './utils'
import {Logger} from '@subsquid/logger'
import {def} from '@subsquid/util-internal'

export class CachedEntity<E extends Entity> {
    value: E | null

    constructor() {
        this.value = null
    }
}

export class CacheMap {
    private map: Map<string, Map<string, CachedEntity<any>>> = new Map()

    constructor(private em: () => EntityManager, private opts: {logger: Logger}) {}

    exist<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const cacheMap = this.getEntityCache(entityClass)
        const cachedEntity = cacheMap.get(id)
        return cachedEntity?.value != null
    }

    get<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const cacheMap = this.getEntityCache(entityClass)
        return cacheMap.get(id) as CachedEntity<E> | undefined
    }

    ensure<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const cacheMap = this.getEntityCache(entityClass)

        if (!cacheMap.has(id)) {
            cacheMap.set(id, new CachedEntity())

            const name = this.getEntityName(entityClass)
            this.getLogger().debug(`added empty entity ${name} ${id}`)
        }
    }

    delete<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const cacheMap = this.getEntityCache(entityClass)
        cacheMap.set(id, new CachedEntity())

        const name = this.getEntityName(entityClass)
        this.getLogger().debug(`deleted entity ${name} ${id}`)
    }

    clear() {
        const log = this.getLogger()

        for (const [name, item] of this.map) {
            log.debug(`cleared cache for ${name} (${item.size})`)
            item.clear()
        }
        this.map.clear()
    }

    add<E extends Entity>(entity: E, mask?: FindOptionsRelations<E>): void
    add<E extends Entity>(entities: E[], mask?: FindOptionsRelations<E>): void
    add<E extends Entity>(e: E | E[], mask: FindOptionsRelations<E> = {}) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length === 0) return

        for (const entity of entities) {
            this.cacheEntity(entity, mask)
        }
    }

    private cacheEntity(entity: Entity, mask: FindOptionsRelations<any>) {
        const em = this.em()

        const entityClass = entity.constructor
        const metadata = em.connection.getMetadata(entityClass)
        const cacheMap = this.getEntityCache(entityClass)

        const entityId = entity.id
        let cachedEntity = cacheMap.get(entityId)

        if (cachedEntity == null) {
            cachedEntity = new CachedEntity()
            cacheMap.set(entityId, cachedEntity)
        }

        if (cachedEntity.value == null) {
            cachedEntity.value = em.create(metadata.target)
            this.getLogger().debug(`added entity ${metadata.name} ${entity.id}`)
        }

        this.cacheColumns(metadata.nonVirtualColumns, entity, cachedEntity.value)
        this.cacheRelatedEntities(metadata.relations, entity, cachedEntity.value, mask)
    }

    private cacheColumns(columns: ColumnMetadata[], sourceEntity: Entity, cachedEntity: Entity) {
        for (const column of columns) {
            const objectColumnValue = column.getEntityValue(sourceEntity)
            if (objectColumnValue !== undefined) {
                column.setEntityValue(cachedEntity, copy(objectColumnValue))
            }
        }
    }

    private cacheRelatedEntities(
        relations: RelationMetadata[],
        sourceEntity: Entity,
        cachedEntity: Entity,
        mask: FindOptionsRelations<any>
    ) {
        for (const relation of relations) {
            const invMetadata = relation.inverseEntityMetadata
            const invEntity = relation.getEntityValue(sourceEntity)
            const invMask = mask[relation.propertyName]
            if (invEntity === undefined) continue

            if (invMask) {
                if (relation.isOneToMany || relation.isManyToMany) {
                    if (!Array.isArray(invEntity)) continue

                    for (const entity of invEntity) {
                        this.cacheEntity(entity, typeof invMask === 'boolean' ? {} : invMask)
                    }
                } else if (invEntity != null) {
                    this.cacheEntity(invEntity, typeof invMask === 'boolean' ? {} : invMask)
                }
            }

            if (relation.isOwning) {
                if (invEntity === null) {
                    relation.setEntityValue(cachedEntity, null)
                } else {
                    const relationCacheMap = this.getEntityCache(invMetadata.target)
                    const cachedRelation = relationCacheMap.get(invEntity.id)

                    if (cachedRelation == null) {
                        throw new Error(`Missing entity ${invMetadata.name} with id ${invEntity.id}`)
                    }

                    relation.setEntityValue(cachedEntity, cachedRelation.value)
                }
            }
        }
    }

    private getEntityCache(entityClass: EntityTarget<any>) {
        const name = this.getEntityName(entityClass)

        let map = this.map.get(name)
        if (map == null) {
            map = new Map()
            this.map.set(name, map)
        }

        return map
    }

    private getEntityName(entityClass: EntityTarget<any>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)
        return metadata.name
    }

    @def
    private getLogger(): Logger {
        return this.opts.logger.child('cache')
    }
}
