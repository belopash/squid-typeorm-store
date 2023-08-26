import {Entity as _Entity, Entity, EntityClass, FindManyOptions, FindOneOptions, Store} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {def} from '@subsquid/util-internal'
import assert from 'assert'
import {Graph} from 'graph-data-structure'
import {EntityManager, EntityTarget, FindOptionsRelations, FindOptionsWhere, In} from 'typeorm'
import {copy, splitIntoBatches} from './utils'

export {EntityClass, FindManyOptions, FindOneOptions, Entity}

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
        const _cacheMap = this.getEntityCache(entityClass)
        const cachedEntity = _cacheMap.get(id)
        return cachedEntity?.value != null
    }

    get<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const _cacheMap = this.getEntityCache(entityClass)
        return _cacheMap.get(id) as CachedEntity<E>
    }

    ensure<E extends Entity>(entityClass: EntityTarget<E>, id: string) {
        const _cacheMap = this.getEntityCache(entityClass)

        let cachedEntity = _cacheMap.get(id)
        if (cachedEntity == null) {
            cachedEntity = new CachedEntity()
            _cacheMap.set(id, cachedEntity)
        }
    }

    add<E extends Entity>(entity: E, mask?: FindOptionsRelations<any>): void
    add<E extends Entity>(entities: E[], mask?: FindOptionsRelations<any>): void
    add<E extends Entity>(e: E | E[], mask: FindOptionsRelations<any> = {}) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = this.em().connection.getMetadata(entities[0].constructor)

        const _cacheMap = this.getEntityCache(metadata.target)

        for (const entity of entities) {
            let cachedEntity = _cacheMap.get(entity.id)
            if (cachedEntity == null) {
                cachedEntity = new CachedEntity()
                _cacheMap.set(entity.id, cachedEntity)
            }

            if (cachedEntity.value == null) {
                cachedEntity.value = this.em().create(entityClass)
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
                        assert(Array.isArray(relation))
                        for (const r of relation) {
                            this.add(r, typeof relatedMask === 'boolean' ? {} : relatedMask)
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
                            `missing entity ${relation.inverseEntityMetadata.name} with id ${relatedEntity.id}`
                        )

                        const relatedEntityIdOnly = this.em().create(relatedMetadata.target, {id: relatedEntity.id})
                        relation.setEntityValue(cachedEntity.value, relatedEntityIdOnly)
                    }
                }
            }
        }
    }

    private getEntityCache(entityClass: EntityTarget<any>) {
        const metadata = this.em().connection.getMetadata(entityClass)

        let map = this.map.get(metadata.name)
        if (map == null) {
            map = new Map()
            this.map.set(metadata.name, map)
        }

        return map
    }
}

export type DeferMap = Map<string, {ids: Set<string>; relations: FindOptionsRelations<any>}>
export type ChangeMap = Map<string, Set<string>>

// @ts-ignore
export class StoreWithCache extends Store {
    private deferMap: DeferMap = new Map()
    private insertMap: ChangeMap = new Map()
    private upsertMap: ChangeMap = new Map()

    private cache: CacheMap

    private constructor(private em: () => EntityManager, changes?: ChangeTracker) {
        super(em, changes)
        this.cache = new CacheMap(em)
    }

    async insert<E extends _Entity>(entity: E): Promise<void>
    async insert<E extends _Entity>(entities: E[]): Promise<void>
    async insert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = this.em().connection.getMetadata(entityClass)

        const relationMask: FindOptionsRelations<any> = {}
        for (const relation of metadata.relations) {
            if (relation.isOwning) {
                relationMask[relation.propertyName] = true
            }
        }

        const _insertList = this.getInsertList(entityClass)
        const _upsertList = this.getUpsertList(entityClass)

        for (const entity of entities) {
            assert(!_insertList.has(entity.id))
            assert(!_upsertList.has(entity.id))
            assert(!this.cache.exist(metadata.target, entity.id))

            this.cache.add(entity, relationMask)
            _insertList.add(entity.id)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const EntityTarget = entities[0].constructor
        const metadata = this.em().connection.getMetadata(EntityTarget)
        const _insertList = this.getInsertList(EntityTarget.name)
        const _upsertList = this.getUpsertList(EntityTarget.name)
        for (const entity of entities) {
            const relationMask: FindOptionsRelations<any> = {}
            for (const relation of metadata.relations) {
                if (relation.isOwning && entity[relation.propertyName as keyof E] !== undefined) {
                    relationMask[relation.propertyName] = true
                }
            }

            this.cache.add(entity, relationMask)
            if (!_insertList.has(entity.id)) {
                _upsertList.add(entity.id)
            }
        }
    }

    async save<E extends _Entity>(entity: E): Promise<void>
    async save<E extends _Entity>(entities: E[]): Promise<void>
    async save<E extends _Entity>(e: E | E[]): Promise<void> {
        return await this.upsert(e as any)
    }

    async remove<E extends Entity>(entity: E): Promise<void>
    async remove<E extends Entity>(entities: E[]): Promise<void>
    async remove<E extends Entity>(entityClass: EntityTarget<E>, id: string | string[]): Promise<void>
    async remove(entityClass: any, id?: any): Promise<void> {
        throw new Error('not implemented')
    }

    async count<E extends Entity>(entityClass: EntityTarget<E>, options?: FindManyOptions<E>): Promise<number> {
        await this.flush()
        return await super.count(entityClass as EntityClass<E>, options)
    }

    async countBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<number> {
        await this.flush()
        return await super.countBy(entityClass as EntityClass<E>, where)
    }

    async find<E extends Entity>(entityClass: EntityTarget<E>, options: FindManyOptions<E>): Promise<E[]> {
        await this.flush()
        const res = await super.find(entityClass as EntityClass<E>, options)
        if (res != null) this.cache.add(res, options.relations)
        return res
    }

    async findBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        await this.flush()
        const res = await super.findBy(entityClass as EntityClass<E>, where)
        if (res != null) this.cache.add(res)
        return res
    }

    async findOne<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E | undefined> {
        await this.flush()
        const res = await super.findOne(entityClass as EntityClass<E>, options)
        if (res != null) this.cache.add(res, options.relations)
        return res
    }

    async findOneOrFail<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E> {
        await this.flush()
        const res = await super.findOneOrFail(entityClass as EntityClass<E>, options)
        if (res != null) this.cache.add(res, options.relations)
        return res
    }

    async findOneBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        await this.flush()
        const res = await super.findOneBy(entityClass as EntityClass<E>, where)
        if (res != null) this.cache.add(res)
        return res
    }

    async findOneByOrFail<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        await this.flush()
        const res = await super.findOneByOrFail(entityClass as EntityClass<E>, where)
        if (res != null) this.cache.add(res)
        return res
    }

    async get<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        relations?: FindOptionsRelations<E>
    ): Promise<E | undefined> {
        await this.load()

        const entity = this.getCached(entityClass, id, relations)

        if (entity !== undefined) {
            return entity == null ? undefined : entity
        } else {
            return await this.findOne(entityClass, {where: {id} as any, relations})
        }
    }

    async getOrFail<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        relations?: FindOptionsRelations<E>
    ): Promise<E> {
        let e = await this.get(entityClass, id, relations)

        if (e == null) {
            const metadata = this.em().connection.getMetadata(entityClass)
            throw new Error(`Missing entity ${metadata.name} with id "${id}"`)
        }

        return e
    }

    private getCached<E extends Entity>(entityClass: EntityTarget<E>, id: string, mask: FindOptionsRelations<E> = {}) {
        const metadata = this.em().connection.getMetadata(entityClass)

        const cachedEntity = this.cache.get(entityClass, id)

        if (cachedEntity == null) {
            return undefined
        } else if (cachedEntity.value == null) {
            return null
        } else {
            const clonedEntity = this.em().create(entityClass)

            for (const column of metadata.nonVirtualColumns) {
                const objectColumnValue = column.getEntityValue(cachedEntity.value)
                if (objectColumnValue !== undefined) {
                    column.setEntityValue(clonedEntity, copy(objectColumnValue))
                }
            }

            for (const relation of metadata.relations) {
                let relatedMask = mask[relation.propertyName as keyof E]
                if (!relatedMask) continue

                const relatedEntity = relation.getEntityValue(cachedEntity.value)

                if (relatedEntity === undefined) {
                    return undefined // relation is missing, but required
                } else if (relatedEntity == null) {
                    relation.setEntityValue(clonedEntity, null)
                } else {
                    const cachedRelatedEntity = this.getCached(
                        relation.inverseEntityMetadata.target,
                        relatedEntity.id,
                        typeof relatedMask === 'boolean' ? {} : relatedMask
                    )
                    assert(cachedRelatedEntity != null)

                    relation.setEntityValue(clonedEntity, cachedRelatedEntity)
                }
            }

            return clonedEntity
        }
    }

    defer<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        relations?: FindOptionsRelations<E>
    ): DeferredEntity<E> {
        const _deferredList = this.getDeferData(entityClass)

        _deferredList.ids.add(id)

        if (relations != null) {
            _deferredList.relations = mergeRelataions(_deferredList.relations, relations)
        }

        return new DeferredEntity({
            get: async () => this.get(entityClass, id, relations),
            getOrFail: async () => this.getOrFail(entityClass, id, relations),
        })
    }

    async flush(): Promise<void> {
        const entityOrder = await this.getTopologicalOrder()

        for (const name of entityOrder) {
            const changes = this.computeChanges(name)

            await super.upsert(changes.upserts)
            await super.insert(changes.inserts)
            await super.upsert(changes.delayedUpserts)
        }

        this.clearChanges()
    }

    private computeChanges<E extends Entity>(entityClass: EntityTarget<E>) {
        const metadata = this.em().connection.getMetadata(entityClass)
        const selfRelations = metadata.manyToOneRelations.filter((r) => r.inverseEntityMetadata.name === metadata.name)

        const insertList = this.getInsertList(entityClass)
        const inserts: E[] = []
        for (const id of insertList) {
            const cached = this.cache.get<E>(entityClass, id)
            assert(cached != null && cached.value != null)
            inserts.push(cached.value)
        }

        const upsertList = this.getUpsertList(entityClass)
        const upserts: E[] = []
        const delayedUpserts: E[] = []
        for (const id of upsertList) {
            const cached = this.cache.get<E>(entityClass, id)
            assert(cached != null && cached.value != null)
            let isDelayed = false
            for (const relation of selfRelations) {
                const related = relation.getEntityValue(cached)
                if (related != null && insertList.has(related.id)) {
                    isDelayed = true
                    break
                }
            }

            if (isDelayed) {
                delayedUpserts.push(cached.value)
            } else {
                upserts.push(cached.value)
            }
        }

        return {
            inserts,
            upserts,
            delayedUpserts,
        }
    }

    private clearChanges() {
        this.insertMap.clear()
        this.upsertMap.clear()
    }

    private async load(): Promise<void> {
        for (const [entityName, _deferData] of this.deferMap) {
            if (_deferData.ids.size === 0) return

            const metadata = this.em().connection.getMetadata(entityName)

            for (const id of _deferData.ids) {
                this.cache.ensure(metadata.target, id)
            }

            for (let batch of splitIntoBatches([..._deferData.ids], 30000)) {
                await this.find<any>(metadata.target, {where: {id: In(batch)}, relations: _deferData.relations})
            }
        }

        this.deferMap.clear()
    }

    @def
    private async getTopologicalOrder() {
        const graph = Graph()
        for (const metadata of this.em().connection.entityMetadatas) {
            graph.addNode(metadata.name)
            for (const foreignKey of metadata.foreignKeys) {
                if (foreignKey.referencedEntityMetadata === metadata) continue // don't add self-refs

                graph.addEdge(metadata.name, foreignKey.referencedEntityMetadata.name)
            }
        }

        return graph.topologicalSort().reverse()
    }

    private getDeferData(entityClass: EntityTarget<any>) {
        const metadata = this.em().connection.getMetadata(entityClass)

        let list = this.deferMap.get(metadata.name)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.deferMap.set(metadata.name, list)
        }

        return list
    }

    private getInsertList(entityClass: EntityTarget<any>) {
        const metadata = this.em().connection.getMetadata(entityClass)

        let list = this.insertMap.get(metadata.name)
        if (list == null) {
            list = new Set()
            this.insertMap.set(metadata.name, list)
        }

        return list
    }

    private getUpsertList(entityClass: EntityTarget<any>) {
        const metadata = this.em().connection.getMetadata(entityClass)

        let list = this.upsertMap.get(metadata.name)
        if (list == null) {
            list = new Set()
            this.upsertMap.set(metadata.name, list)
        }

        return list
    }
}

function mergeRelataions<E extends Entity>(
    a: FindOptionsRelations<E>,
    b: FindOptionsRelations<E>
): FindOptionsRelations<E> {
    const mergedObject: FindOptionsRelations<E> = {}

    for (const key in a) {
        mergedObject[key] = a[key]
    }

    for (const key in b) {
        const bValue = b[key]
        const value = mergedObject[key]
        if (typeof bValue === 'object') {
            mergedObject[key] = (typeof value === 'object' ? mergeRelataions(value, bValue) : bValue) as any
        } else {
            mergedObject[key] = value || bValue
        }
    }

    return mergedObject
}

export class DeferredEntity<E extends Entity> {
    constructor(private opts: {get: () => Promise<E | undefined>; getOrFail: () => Promise<E>}) {}

    async get(): Promise<E | undefined> {
        return await this.opts.get()
    }

    async getOrFail(): Promise<E> {
        return await this.opts.getOrFail()
    }
}
