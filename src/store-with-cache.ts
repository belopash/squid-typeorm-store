import {Entity as _Entity, EntityClass, FindManyOptions, FindOneOptions, Store} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {def} from '@subsquid/util-internal'
import assert from 'assert'
import {Graph} from 'graph-data-structure'
import {EntityManager, EntityTarget, FindOptionsRelations, FindOptionsWhere, In} from 'typeorm'

export interface Entity extends _Entity {
    [k: string]: any
}

export type DeferMap = Map<string, {ids: Set<string>; relations: FindOptionsRelations<any>}>
export type CacheMap = Map<string, Map<string, Entity | null>>
export type ChangeMap = Map<string, Map<string, Entity>>

export class StoreWithCache extends Store {
    static create(store: Store) {
        return new StoreWithCache(store['em'], store['changes'])
    }

    private get _em(): EntityManager {
        return this['em']()
    }

    private deferMap: DeferMap = new Map()

    private cacheMap: CacheMap = new Map()

    private insertMap: ChangeMap = new Map()
    private upsertMap: ChangeMap = new Map()

    private constructor(em: () => EntityManager, changes?: ChangeTracker) {
        super(em, changes)
    }

    async insert<E extends _Entity>(entity: E): Promise<void>
    async insert<E extends _Entity>(entities: E[]): Promise<void>
    async insert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = this._em.connection.getMetadata(entityClass)
        const fullRelationMask = metadata.relations.reduce((mask, relation) => {
            if (relation.isOwning) {
                mask[relation.propertyName] = true
            }

            return mask
        }, {} as FindOptionsRelations<any>)

        const _insertList = this.getInsertList(entityClass)
        const _upsertList = this.getUpsertList(entityClass)
        const _cacheMap = this.getCacheMap(entityClass)

        for (const entity of entities) {
            assert(!_insertList.has(entity.id))
            assert(!_upsertList.has(entity.id))

            let cached = _cacheMap.get(entity.id)
            assert(cached == null)
            cached = this.cache(entity, fullRelationMask)

            _insertList.set(entity.id, cached)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const EntityTarget = entities[0].constructor
        const metadata = this._em.connection.getMetadata(EntityTarget)
        const _insertList = this.getInsertList(EntityTarget.name)
        const _upsertList = this.getUpsertList(EntityTarget.name)
        for (const entity of entities) {
            const relationMask = metadata.relations.reduce((mask, relation) => {
                if (relation.isOwning && entity[relation.propertyName as keyof E] !== undefined) {
                    mask[relation.propertyName] = true
                }

                return mask
            }, {} as FindOptionsRelations<any>)

            const cached = this.cache(entity, relationMask)
            if (!_insertList.has(entity.id)) {
                _upsertList.set(entity.id, cached)
            }
        }
    }

    async save<E extends _Entity>(entity: E): Promise<void>
    async save<E extends _Entity>(entities: E[]): Promise<void>
    async save<E extends _Entity>(e: E | E[]): Promise<void> {
        return await this.upsert(e as any)
    }

    remove<E extends Entity>(entity: E): Promise<void>
    remove<E extends Entity>(entities: E[]): Promise<void>
    remove<E extends Entity>(entityClass: EntityTarget<E>, id: string | string[]): Promise<void>
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
        if (res != null) this.cache(res, options.relations)
        return res
    }

    async findBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        await this.flush()
        const res = await super.findBy(entityClass as EntityClass<E>, where)
        if (res != null) this.cache(res)
        return res
    }

    async findOne<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E | undefined> {
        await this.flush()
        const res = await super.findOne(entityClass as EntityClass<E>, options)
        if (res != null) this.cache(res, options.relations)
        return res
    }

    async findOneOrFail<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E> {
        await this.flush()
        const res = await super.findOneOrFail(entityClass as EntityClass<E>, options)
        if (res != null) this.cache(res, options.relations)
        return res
    }

    async findOneBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        await this.flush()
        const res = await super.findOneBy(entityClass as EntityClass<E>, where)
        if (res != null) this.cache(res)
        return res
    }

    async findOneByOrFail<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        await this.flush()
        const res = await super.findOneByOrFail(entityClass as EntityClass<E>, where)
        if (res != null) this.cache(res)
        return res
    }

    async get<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        relations?: FindOptionsRelations<E>
    ): Promise<E | undefined> {
        await this.load()

        const entity = this.getCachedEntity(entityClass, id, relations ?? {})

        if (entity !== undefined) {
            return entity == null ? undefined : entity
        } else {
            return await this.findOne(entityClass, {where: {id} as any, relations})
        }
    }

    private getCachedEntity<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        mask: FindOptionsRelations<any>
    ) {
        const metadata = this._em.connection.getMetadata(entityClass)

        const _cacheMap = this.getCacheMap(metadata.target)
        const entity = _cacheMap.get(id)

        if (entity == null) {
            return null
        } else {
            const clonedEntity = this._em.create(entityClass)

            for (const column of metadata.nonVirtualColumns) {
                const objectColumnValue = column.getEntityValue(entity)
                if (objectColumnValue !== undefined) {
                    column.setEntityValue(clonedEntity, copy(objectColumnValue))
                }
            }

            for (const relation of metadata.relations) {
                let relatedMask = mask[relation.propertyName]
                if (!relatedMask) continue
                relatedMask = typeof relatedMask === 'boolean' ? {} : relatedMask

                const relatedEntity = relation.getEntityValue(entity) as Entity | null | undefined

                if (relatedEntity === undefined) {
                    return undefined // relation is missing
                } else if (relatedEntity == null) {
                    relation.setEntityValue(clonedEntity, null)
                } else {
                    const clonedRelation = this.getCachedEntity(
                        relation.inverseEntityMetadata.target,
                        relatedEntity.id,
                        relatedMask
                    )

                    if (clonedRelation === undefined) {
                        return undefined // some relation in relation entity
                    } else {
                        relation.setEntityValue(clonedEntity, clonedRelation)
                    }
                }
            }

            return clonedEntity
        }
    }

    async getOrFail<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        relations?: FindOptionsRelations<E>
    ): Promise<E> {
        let e = await this.get(entityClass, id, relations)

        if (e == null) {
            const metadata = this._em.connection.getMetadata(entityClass)
            throw new Error(`Missing entity ${metadata.name} with id "${id}"`)
        }

        return e
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
            const inserts = this.getInsertList(name)
            const upserts = this.getUpsertList(name)
            const delayedUpserts = new Map<string, Entity>()

            const metadata = this._em.connection.getMetadata(name)
            const selfRelations = metadata.manyToOneRelations.filter(
                (r) => r.inverseEntityMetadata.name === metadata.name
            )
            if (selfRelations.length > 0) {
                for (const [id, entity] of upserts) {
                    for (const relation of selfRelations) {
                        const value = relation.getEntityValue(entity)
                        if (value != null && inserts.has(value.id)) {
                            delayedUpserts.set(id, entity)
                            upserts.delete(id)
                            break
                        }
                    }
                }
            }

            if (upserts.size > 0) {
                metadata.create
                const entities = upserts.values()
                await super.upsert([...entities])
                upserts.clear()
            }

            if (inserts.size > 0) {
                const entities = inserts.values()
                await super.insert([...entities])
                inserts.clear()
            }

            if (delayedUpserts.size > 0) {
                const entities = delayedUpserts.values()
                await super.upsert([...entities])
                delayedUpserts.clear()
            }
        }
    }

    private async load(): Promise<void> {
        for (const [entityName, _deferData] of this.deferMap) {
            if (_deferData.ids.size === 0) return

            const _cacheMap = this.getCacheMap(entityName)
            for (const id of _deferData.ids) {
                if (_cacheMap.has(id)) continue
                _cacheMap.set(id, null)
            }

            const metadata = this._em.connection.getMetadata(entityName)

            for (let batch of splitIntoBatches([..._deferData.ids], 30000)) {
                await this.find<any>(metadata.target, {where: {id: In(batch)}, relations: _deferData.relations})
            }
        }

        this.deferMap.clear()
    }

    private cache<E extends Entity>(entity: E, mask?: FindOptionsRelations<any>): Entity
    private cache<E extends Entity>(entities: E[], mask?: FindOptionsRelations<any>): Entity[]
    private cache<E extends Entity>(e: E | E[], mask: FindOptionsRelations<any> = {}) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const metadata = this._em.connection.getMetadata(entities[0].constructor)

        const _cacheMap = this.getCacheMap(metadata.target)
        const cachedEntities: Entity[] = []

        for (const entity of entities) {
            let cachedEntity = _cacheMap.get(entity.id)
            if (cachedEntity == null) {
                cachedEntity = this._em.create(metadata.target) as Entity
                _cacheMap.set(entity.id, cachedEntity!)
            }

            for (const column of metadata.nonVirtualColumns) {
                const objectColumnValue = column.getEntityValue(entity)
                if (objectColumnValue !== undefined) {
                    column.setEntityValue(cachedEntity, copy(objectColumnValue))
                }
            }

            for (const relation of metadata.relations) {
                let relatedMask = mask[relation.propertyName]
                if (!relatedMask) continue
                relatedMask = typeof relatedMask === 'boolean' ? {} : relatedMask

                const relatedEntity = relation.getEntityValue(entity) as Entity | null | undefined

                if (relation.isOwning) {
                    if (relatedEntity == null) {
                        relation.setEntityValue(cachedEntity, null)
                    } else {
                        const _relationCacheMap = this.getCacheMap(relation.inverseEntityMetadata.target)
                        let cachedRelation = _relationCacheMap.get(relatedEntity.id)
                        if (cachedRelation == null) {
                            cachedRelation = this.cache(relatedEntity, relatedMask)
                        }

                        relation.setEntityValue(cachedEntity, cachedRelation)
                    }
                } else {
                    // We also cache these realations, but do not assign them to cached entity,
                    // since we can not garantee that result will be consistent.
                    if (relation.isOneToMany) {
                        assert(Array.isArray(relation))
                        for (const r of relation) {
                            this.cache(r, relatedMask)
                        }
                    } else if (relation.isOneToOne && relatedEntity != null) {
                        this.cache(relatedEntity, relatedMask)
                    }
                }
            }

            cachedEntities.push(cachedEntity)
        }

        return Array.isArray(e) ? cachedEntities : cachedEntities[0]
    }

    @def
    private async getTopologicalOrder() {
        const graph = Graph()
        for (const metadata of this._em.connection.entityMetadatas) {
            graph.addNode(metadata.name)
            for (const foreignKey of metadata.foreignKeys) {
                if (foreignKey.referencedEntityMetadata === metadata) continue // don't add self-refs

                graph.addEdge(metadata.name, foreignKey.referencedEntityMetadata.name)
            }
        }
        return graph.topologicalSort().reverse()
    }

    private getDeferData(entityClass: EntityTarget<any>) {
        const metadata = this._em.connection.getMetadata(entityClass)

        let list = this.deferMap.get(metadata.name)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.deferMap.set(metadata.name, list)
        }

        return list
    }

    private getCacheMap(entityClass: EntityTarget<any>) {
        const metadata = this._em.connection.getMetadata(entityClass)

        let map = this.cacheMap.get(metadata.name)
        if (map == null) {
            map = new Map()
            this.cacheMap.set(metadata.name, map)
        }

        return map
    }

    private getInsertList(entityClass: EntityTarget<any>) {
        const metadata = this._em.connection.getMetadata(entityClass)

        let list = this.insertMap.get(metadata.name)
        if (list == null) {
            list = new Map()
            this.insertMap.set(metadata.name, list)
        }

        return list
    }

    private getUpsertList(entityClass: EntityTarget<any>) {
        const metadata = this._em.connection.getMetadata(entityClass)

        let list = this.upsertMap.get(metadata.name)
        if (list == null) {
            list = new Map()
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

function* splitIntoBatches<T>(list: T[], maxBatchSize: number): Generator<T[]> {
    if (list.length <= maxBatchSize) {
        yield list
    } else {
        let offset = 0
        while (list.length - offset > maxBatchSize) {
            yield list.slice(offset, offset + maxBatchSize)
            offset += maxBatchSize
        }
        yield list.slice(offset)
    }
}

function copy<T>(obj: T): T {
    if (typeof obj !== 'object' || obj == null) return obj
    else if (obj instanceof Date) {
        return new Date(obj) as any
    } else if (Array.isArray(obj)) {
        return copyArray(obj) as any
    } else if (obj instanceof Map) {
        return new Map(copyArray(Array.from(obj))) as any
    } else if (obj instanceof Set) {
        return new Set(copyArray(Array.from(obj))) as any
    } else if (ArrayBuffer.isView(obj)) {
        return copyBuffer(obj)
    } else {
        const clone = Object.create(Object.getPrototypeOf(obj))
        for (var k in obj) {
            clone[k] = copy(obj[k])
        }
        return clone
    }
}

function isObject(val: any): val is Object {
    return val !== null && typeof val === 'object'
}

function copyBuffer(buf: any) {
    if (buf instanceof Buffer) {
        return Buffer.from(buf)
    }

    return new buf.constructor(buf.buffer.slice(), buf.byteOffset, buf.length)
}

function copyArray(arr: any[]) {
    const clone = new Array(arr.length)
    for (let i = 0; i < arr.length; i++) {
        clone[i] = copy(clone[i])
    }
    return clone
}
