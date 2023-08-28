import {Entity} from '@subsquid/typeorm-store'
import assert from 'assert'
import {EntityManager, EntityTarget, FindOptionsRelations} from 'typeorm'
import {copy} from './utils'

export class CachedEntity<E extends Entity> {
    value: E | null
    relations: {[key: string]: boolean}

    constructor() {
        this.value = null
        this.relations = {}
    }
}

export class CacheMap {
    private map: Map<string, Map<string, CachedEntity<any>>> = new Map()

    constructor(private em: () => EntityManager) {}

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

        let cachedEntity = cacheMap.get(id)
        if (cachedEntity == null) {
            cachedEntity = new CachedEntity()
            cacheMap.set(id, cachedEntity)
        }
    }

    delete<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const cacheMap = this.getEntityCache(entityClass)

        const cachedEntity = new CachedEntity()
        cacheMap.set(id, cachedEntity)
    }

    add<E extends Entity>(entity: E, mask?: FindOptionsRelations<any>): void
    add<E extends Entity>(entities: E[], mask?: FindOptionsRelations<any>): void
    add<E extends Entity>(e: E | E[], mask: FindOptionsRelations<any> = {}) {
        const em = this.em()

        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = em.connection.getMetadata(entities[0].constructor)

        const cacheMap = this.getEntityCache(metadata.target)

        for (const entity of entities) {
            let cachedEntity = cacheMap.get(entity.id)
            if (cachedEntity == null) {
                cachedEntity = new CachedEntity()
                cacheMap.set(entity.id, cachedEntity)
            }

            if (cachedEntity.value == null) {
                cachedEntity.value = em.create(entityClass)
            }

            for (const column of metadata.nonVirtualColumns) {
                const objectColumnValue = column.getEntityValue(entity)
                if (objectColumnValue !== undefined) {
                    column.setEntityValue(cachedEntity.value, copy(objectColumnValue))
                }
            }

            for (const relation of metadata.relations) {
                const relatedMetadata = relation.inverseEntityMetadata
                const relatedEntity = relation.getEntityValue(entity) as Entity | null | undefined

                const relatedMask = mask[relation.propertyName]
                if (relatedMask) {
                    if (relation.isOneToMany || relation.isManyToMany) {
                        if (Array.isArray(relatedEntity)) {
                            for (const r of relatedEntity) {
                                this.add(r, typeof relatedMask === 'boolean' ? {} : relatedMask)
                            }
                        }
                    } else if (relatedEntity != null) {
                        this.add(relatedEntity, typeof relatedMask === 'boolean' ? {} : relatedMask)
                    }
                }

                if (relation.isOwning && relatedMask) {
                    if (relatedEntity == null) {
                        relation.setEntityValue(cachedEntity.value, null)
                    } else {
                        const _relationCacheMap = this.getEntityCache(relatedMetadata.target)
                        const cachedRelation = _relationCacheMap.get(relatedEntity.id)
                        assert(
                            cachedRelation != null,
                            `missing entity ${relatedMetadata.name} with id ${relatedEntity.id}`
                        )

                        const relatedEntityIdOnly = em.create(relatedMetadata.target, {id: relatedEntity.id})
                        relation.setEntityValue(cachedEntity.value, relatedEntityIdOnly)
                    }
                }
            }
        }
    }

    clear() {
        for (const item of this.map.values()) {
            item.clear()
        }
        this.map.clear()
    }

    private getEntityCache(entityClass: EntityTarget<any>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)

        let map = this.map.get(metadata.name)
        if (map == null) {
            map = new Map()
            this.map.set(metadata.name, map)
        }

        return map
    }
}
