import {Entity as _Entity, Entity, EntityClass, FindManyOptions, FindOneOptions, Store} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {def} from '@subsquid/util-internal'
import assert from 'assert'
import {EntityManager, EntityMetadata, EntityTarget, FindOptionsRelations, FindOptionsWhere, In} from 'typeorm'
import {copy, splitIntoBatches} from './utils'
import {CacheMap} from './cacheMap'
import {UpdateMap, UpdateType} from './updateMap'
import {RelationMetadata} from 'typeorm/metadata/RelationMetadata'
import {createLogger, Logger} from '@subsquid/logger'
import {ColumnMetadata} from 'typeorm/metadata/ColumnMetadata'
import {getCommitOrder} from './relationGraph'

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
        this.cache = new CacheMap(em, {logger: this.getLogger()})
    }

    async insert<E extends _Entity>(entity: E): Promise<void>
    async insert<E extends _Entity>(entities: E[]): Promise<void>
    async insert<E extends _Entity>(e: E | E[]): Promise<void> {
        const em = this.em()

        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = em.connection.getMetadata(entityClass)

        const updateMap = this.getUpdateMap(entityClass)
        for (const entity of entities) {
            updateMap.insert(entity.id)
            this.cache.add(entity)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        const em = this.em()

        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityClass = entities[0].constructor
        const metadata = em.connection.getMetadata(entityClass)

        const updateMap = this.getUpdateMap(entityClass)
        for (const entity of entities) {
            updateMap.upsert(entity.id)
            this.cache.add(entity)
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
        const em = this.em()

        if (id == null) {
            const entities = Array.isArray(e) ? e : [e as E]
            if (entities.length == 0) return

            const entityClass = entities[0].constructor
            const updateMap = this.getUpdateMap(entityClass)

            for (const entity of entities) {
                updateMap.remove(entity.id)
                this.cache.delete(entityClass, entity.id)
            }
        } else {
            const ids = Array.isArray(id) ? id : [id]
            if (ids.length == 0) return

            const entityClass = e as EntityTarget<E>
            const updateMap = this.getUpdateMap(entityClass)

            for (const i of ids) {
                updateMap.remove(i)
                this.cache.delete(entityClass, i)
            }
        }
    }

    async count<E extends Entity>(entityClass: EntityTarget<E>, options?: FindManyOptions<E>): Promise<number> {
        await this.commit()
        return await super.count(entityClass as EntityClass<E>, options)
    }

    async countBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<number> {
        await this.commit()
        return await super.countBy(entityClass as EntityClass<E>, where)
    }

    async find<E extends Entity>(entityClass: EntityTarget<E>, options: FindManyOptions<E>): Promise<E[]> {
        await this.commit()
        const res = await super.find(entityClass as EntityClass<E>, options)
        if (res != null) this.cache.add(res, options.relations)
        return res
    }

    async findBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        await this.commit()
        const res = await super.findBy(entityClass as EntityClass<E>, where)
        if (res != null) this.cache.add(res)
        return res
    }

    async findOne<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E | undefined> {
        await this.commit()
        const res = await super.findOne(entityClass as EntityClass<E>, options)
        if (res != null) this.cache.add(res, options.relations)
        return res
    }

    async findOneOrFail<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E> {
        await this.commit()
        const res = await super.findOneOrFail(entityClass as EntityClass<E>, options)
        if (res != null) this.cache.add(res, options.relations)
        return res
    }

    async findOneBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        await this.commit()
        const res = await super.findOneBy(entityClass as EntityClass<E>, where)
        if (res != null) this.cache.add(res)
        return res
    }

    async findOneByOrFail<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        await this.commit()
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
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)

        const cachedEntity = this.cache.get(entityClass, id)

        if (cachedEntity == null) {
            return undefined
        } else if (cachedEntity.value == null) {
            return null
        } else {
            const clonedEntity = em.create(entityClass)

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

    async commit(): Promise<void> {
        const log = this.getLogger()

        const entityOrder = this.getCommitOrder()
        const entityOrderReversed = [...entityOrder].reverse()

        const changeSets: Map<EntityMetadata, ChangeSet> = new Map()
        for (const metadata of entityOrder) {
            const changeSet = this.collectChangeSets(metadata.target)
            changeSets.set(metadata, changeSet)
        }

        for (const metadata of entityOrder) {
            const changeSet = changeSets.get(metadata)
            if (changeSet == null) continue

            log.debug(`commit upserts for ${metadata.name} (${changeSet.upserts.length})`)
            await super.upsert(changeSet.upserts)

            log.debug(`commit inserts for ${metadata.name} (${changeSet.inserts.length})`)
            await super.insert(changeSet.inserts)

            log.debug(`commit delayed updates for ${metadata.name} (${changeSet.delayedUpserts.length})`)
            await super.upsert(changeSet.delayedUpserts)
        }

        for (const metadata of entityOrderReversed) {
            const changeSet = changeSets.get(metadata)
            if (changeSet == null) continue

            log.debug(`commit removes for ${metadata.name} (${changeSet.removes.length})`)
            await super.remove(changeSet.removes)
        }

        this.updates.clear()
    }

    private collectChangeSets(entityClass: EntityTarget<Entity>): ChangeSet {
        const em = this.em()

        const inserts: Entity[] = []
        const upserts: Entity[] = []
        const delayedUpserts: Entity[] = []
        const removes: Entity[] = []

        const updateMap = this.getUpdateMap(entityClass)
        for (const {id, type} of updateMap) {
            const cached = this.cache.get(entityClass, id)

            switch (type) {
                case UpdateType.Insert: {
                    assert(cached?.value != null)
                    inserts.push(cached.value)
                    break
                }
                case UpdateType.Upsert: {
                    assert(cached?.value != null)

                    let isDelayed = false
                    for (const relation of this.getSelfRelations(entityClass)) {
                        const relatedEntity = relation.getEntityValue(cached.value)
                        if (relatedEntity == null) continue

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
                    const e = em.create(entityClass, {id})
                    removes.push(e)
                    break
                }
            }
        }

        return {inserts, upserts, delayedUpserts, removes}
    }

    async flush(): Promise<void> {
        await this.commit()
        this.cache.clear()
    }

    private async load(): Promise<void> {
        const em = this.em()

        for (const [name, deferData] of this.deferMap) {
            const metadata = em.connection.getMetadata(name)

            for (const id of deferData.ids) {
                this.cache.ensure(metadata.target, id)
            }

            for (let batch of splitIntoBatches([...deferData.ids], 30000)) {
                if (batch.length == 0) continue
                await this.find<any>(metadata.target, {where: {id: In(batch)}, relations: deferData.relations})
            }
        }

        this.deferMap.clear()
    }

    private knownSelfRelations: Record<string, RelationMetadata[]> = {}
    private getSelfRelations<E extends Entity>(entityClass: EntityTarget<E>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)

        if (this.knownSelfRelations[metadata.name] == null) {
            this.knownSelfRelations[metadata.name] = metadata.relations.filter(
                (r) => r.inverseEntityMetadata.name === metadata.name
            )
        }
        return this.knownSelfRelations[metadata.name]
    }

    @def
    private getCommitOrder() {
        const em = this.em()
        return getCommitOrder(em.connection.entityMetadatas)
    }

    private getDeferData(entityClass: EntityTarget<any>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)

        let list = this.deferMap.get(metadata.name)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.deferMap.set(metadata.name, list)
        }

        return list
    }

    private getUpdateMap(entityClass: EntityTarget<any>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)

        let list = this.updates.get(metadata.name)
        if (list == null) {
            list = new UpdateMap()
            this.updates.set(metadata.name, list)
        }

        return list
    }

    @def
    private getLogger(): Logger {
        return createLogger('sqd:store')
    }

    // @ts-ignore
    private async saveMany(entityClass: EntityClass<any>, entities: any[]): Promise<void> {
        assert(entities.length > 0)
        let em = this.em()
        let metadata = em.connection.getMetadata(entityClass)
        let fk = metadata.columns.filter((c) => c.relationMetadata)
        if (fk.length == 0) {
            return this.upsertMany(em, entityClass, entities)
        }
        let signatures = entities
            .map((e) => ({entity: e, value: this.getFkSignature(fk, e)}))
            .sort((a, b) => (a.value > b.value ? -1 : b.value > a.value ? 1 : 0))
        let currentSignature = signatures[0].value
        let batch = []
        for (let s of signatures) {
            if (s.value === currentSignature) {
                batch.push(s.entity)
            } else {
                await this.upsertMany(em, entityClass, batch)
                currentSignature = s.value
                batch = [s.entity]
            }
        }
        if (batch.length) {
            await this.upsertMany(em, entityClass, batch)
        }
    }

    private getFkSignature(fk: ColumnMetadata[], entity: any): bigint {
        return super['getFkSignature'](fk, entity)
    }

    private async upsertMany(em: EntityManager, entityClass: EntityClass<any>, entities: any[]): Promise<void> {
        return super['upsertMany'](em, entityClass, entities)
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
