import {Entity as _Entity, Entity, EntityClass, FindManyOptions, FindOneOptions, Store} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {def} from '@subsquid/util-internal'
import assert from 'assert'
import {Graph} from 'graph-data-structure'
import {EntityManager, EntityTarget, FindOptionsRelations, FindOptionsWhere, In} from 'typeorm'
import {copy, splitIntoBatches} from './utils'
import {CacheMap} from './cacheMap'
import {UpdateMap, UpdateType} from './updateMap'
import {RelationMetadata} from 'typeorm/metadata/RelationMetadata'

export {EntityClass, FindManyOptions, FindOneOptions, Entity}

export type DeferMap = Map<string, {ids: Set<string>; relations: FindOptionsRelations<any>}>
export interface ChangeSet {
    inserts: Entity[]
    upserts: Entity[]
    delayedUpserts: Entity[]
    removes: Entity[]
}

// @ts-ignore
export class StoreWithCache extends Store {
    private deferMap: DeferMap = new Map()
    private updates: Map<string, UpdateMap> = new Map()
    private cache: CacheMap

    constructor(private em: () => EntityManager, changes?: ChangeTracker) {
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

        const updateMap = this.getUpdateMap(entityClass)
        for (const entity of entities) {
            updateMap.insert(entity.id)
            this.cache.add(entity, relationMask)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = this.em().connection.getMetadata(entityClass)

        const updateMap = this.getUpdateMap(entityClass)
        for (const entity of entities) {
            const relationMask: FindOptionsRelations<any> = {}
            for (const relation of metadata.relations) {
                const relatedEntity = relation.getEntityValue(entity) as Entity | null | undefined

                if (relation.isOwning && relatedEntity !== undefined) {
                    relationMask[relation.propertyName] = true
                }
            }

            updateMap.upsert(entity.id)
            this.cache.add(entity, relationMask)
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
    async remove<E extends Entity>(e: E | E[] | EntityTarget<E>, id?: string | string[]): Promise<void> {
        if (id == null) {
            const entities = Array.isArray(e) ? e : [e as E]
            if (entities.length == 0) return

            const entityClass = entities[0].constructor
            const updateMap = this.getUpdateMap(entityClass)

            for (const entity of entities) {
                updateMap.remove(entity.id)
            }
        } else {
            const ids = Array.isArray(id) ? id : [id]
            if (ids.length == 0) return

            const entityClass = e as EntityTarget<E>
            const updateMap = this.getUpdateMap(entityClass)

            for (const i of ids) {
                updateMap.remove(i)
            }
        }
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
        const entityOrder = this.getTopologicalOrder()
        const entityOrderReversed = [...entityOrder].reverse()

        const changeSets: Map<string, ChangeSet> = new Map()
        for (const name of entityOrder) {
            const updateMap = this.getUpdateMap(name)

            const inserts: Entity[] = []
            const upserts: Entity[] = []
            const delayedUpserts: Entity[] = []
            const removes: Entity[] = []
            for (const {id, type} of updateMap) {
                const cached = this.cache.get(name, id)

                switch (type) {
                    case UpdateType.Insert: {
                        assert(cached != null && cached.value != null)
                        inserts.push(cached.value)
                        break
                    }
                    case UpdateType.Upsert: {
                        assert(cached != null && cached.value != null)

                        let isDelayed = false
                        for (const relation of this.getSelfRelations(name)) {
                            const relatedEntity = relation.getEntityValue(cached.value)
                            const relatedUpdateType = updateMap.get(relatedEntity.id)

                            if (relatedUpdateType === UpdateType.Insert) {
                                isDelayed = true
                                break
                            }
                        }

                        if (isDelayed) {
                            delayedUpserts.push(cached.value)
                        } else {
                            upserts.push(cached.value)
                        }
                        break
                    }
                    case UpdateType.Remove: {
                        const e = this.em().create(name, {id})
                        removes.push(e)
                        break
                    }
                }
            }

            changeSets.set(name, {
                inserts,
                upserts,
                delayedUpserts,
                removes,
            })
        }

        for (const name of entityOrder) {
            const changeSet = changeSets.get(name)
            if (changeSet == null) continue

            await super.upsert(changeSet.upserts)
            await super.insert(changeSet.inserts)
            await super.upsert(changeSet.delayedUpserts)
        }

        for (const name of entityOrderReversed) {
            const changeSet = changeSets.get(name)
            if (changeSet == null) continue

            await super.remove(changeSet.removes)
        }

        this.updates.clear()
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

    private knownSelfRelations: Record<string, RelationMetadata[]> = {}
    private getSelfRelations<E extends Entity>(entityClass: EntityTarget<E>) {
        const metadata = this.em().connection.getMetadata(entityClass)

        if (this.knownSelfRelations[metadata.name] == null) {
            this.knownSelfRelations[metadata.name] = metadata.relations.filter(
                (r) => r.inverseEntityMetadata.name === metadata.name
            )
        }
        return this.knownSelfRelations[metadata.name]
    }

    @def
    private getTopologicalOrder() {
        const graph = Graph()
        for (const metadata of this.em().connection.entityMetadatas) {
            graph.addNode(metadata.name)
            for (const foreignKey of metadata.foreignKeys) {
                if (foreignKey.referencedEntityMetadata === metadata) continue // don't add self-relations

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

    private getUpdateMap(entityClass: EntityTarget<any>) {
        const metadata = this.em().connection.getMetadata(entityClass)

        let list = this.updates.get(metadata.name)
        if (list == null) {
            list = new UpdateMap()
            this.updates.set(metadata.name, list)
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
