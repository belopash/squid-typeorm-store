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

    add<E extends Entity>(e: E | E[], opts: {mask?: FindOptionsRelations<E>; isNew?: boolean} = {}) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length === 0) return

        for (const entity of entities) {
            this.cacheEntity(entity, opts)
        }
    }

    private cacheEntity(
        entity: Entity,
        {
            mask = {},
            isNew = false,
        }: {
            mask?: FindOptionsRelations<any>
            isNew?: boolean
        }
    ) {
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

        this.cacheColumns({columns: metadata.nonVirtualColumns, sourceEntity: entity, cachedEntity: cachedEntity.value})
        this.cacheRelatedEntities({
            relations: metadata.relations,
            sourceEntity: entity,
            cachedEntity: cachedEntity.value,
            mask,
            isNew,
        })
    }

    private cacheColumns({
        columns,
        sourceEntity,
        cachedEntity,
    }: {
        columns: ColumnMetadata[]
        sourceEntity: Entity
        cachedEntity: Entity
    }) {
        for (const column of columns) {
            const objectColumnValue = column.getEntityValue(sourceEntity)
            if (objectColumnValue !== undefined) {
                column.setEntityValue(cachedEntity, copy(objectColumnValue))
            }
        }
    }

    private cacheRelatedEntities({
        relations,
        sourceEntity,
        cachedEntity,
        mask,
        isNew,
    }: {
        relations: RelationMetadata[]
        sourceEntity: Entity
        cachedEntity: Entity
        mask: FindOptionsRelations<any>
        isNew: boolean
    }) {
        for (const relation of relations) {
            const invMetadata = relation.inverseEntityMetadata
            const invEntity = relation.getEntityValue(sourceEntity)
            const invMask = mask[relation.propertyName]

            if (invMask) {
                if (relation.isOneToMany || relation.isManyToMany) {
                    if (!Array.isArray(invEntity)) continue

                    for (const entity of invEntity) {
                        this.cacheEntity(entity, {mask: typeof invMask === 'boolean' ? {} : invMask})
                    }
                } else if (invEntity != null) {
                    this.cacheEntity(invEntity, {mask: typeof invMask === 'boolean' ? {} : invMask})
                }
            }

            if (relation.isOwning) {
                if (invEntity == null) {
                    if (invEntity === null || isNew) {
                        relation.setEntityValue(cachedEntity, null)
                    }
                } else {
                    const relationCacheMap = this.getEntityCache(invMetadata.target)
                    const cachedRelation = relationCacheMap.get(invEntity.id)?.value

                    if (cachedRelation == null) {
                        throw new Error(`Missing entity ${invMetadata.name} with id ${invEntity.id}`)
                    }

                    relation.setEntityValue(cachedEntity, cachedRelation)
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
