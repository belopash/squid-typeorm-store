import {createLogger, Logger} from '@subsquid/logger'
import {Entity as _Entity, Entity, EntityClass, FindManyOptions, FindOneOptions, Store} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {def} from '@subsquid/util-internal'
import assert from 'assert'
import {EntityManager, EntityMetadata, EntityTarget, FindOptionsRelations, FindOptionsWhere, In} from 'typeorm'
import {ColumnMetadata} from 'typeorm/metadata/ColumnMetadata'
import {CachedEntity, CacheMap} from './cacheMap'
import {UpdatesTracker, UpdateType} from './changeTracker'
import {DeferQueue} from './deferQueue'
import {getCommitOrder} from './relationGraph'
import {copy, splitIntoBatches} from './utils'

export {Entity, EntityClass, FindManyOptions, FindOneOptions}

export type ChangeSet<E extends Entity> = {
    metadata: EntityMetadata
    inserts: E[]
    upserts: E[]
    removes: E[]
    extraUpserts: E[]
}

export interface GetOptions<Entity = any> {
    id: string
    relations?: FindOptionsRelations<Entity>
}

// @ts-ignore
export class StoreWithCache extends Store {
    private updates: UpdatesTracker
    private queue: DeferQueue
    private cache: CacheMap
    private logger: Logger

    constructor(private em: () => EntityManager, changes?: ChangeTracker) {
        super(em, changes)
        this.logger = createLogger('sqd:store')
        this.cache = new CacheMap(em, {logger: this.logger})
        this.updates = new UpdatesTracker(em)
        this.queue = new DeferQueue(em)
    }

    async insert<E extends _Entity>(entity: E): Promise<void>
    async insert<E extends _Entity>(entities: E[]): Promise<void>
    async insert<E extends _Entity>(e: E | E[]): Promise<void> {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        for (const entity of entities) {
            this.updates.insert(entity.constructor, entity.id)
            this.cache.add(entity, true)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        for (const entity of entities) {
            this.updates.upsert(entity.constructor, entity.id)
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
        if (id == null) {
            const entities = Array.isArray(e) ? e : [e as E]
            if (entities.length == 0) return

            for (const entity of entities) {
                const entityClass = entity.constructor
                this.updates.remove(entityClass, entity.id)
                this.cache.delete(entityClass, entity.id)
            }
        } else {
            const ids = Array.isArray(id) ? id : [id]
            if (ids.length == 0) return

            const entityClass = e as EntityTarget<E>
            for (const id of ids) {
                this.updates.remove(entityClass, id)
                this.cache.delete(entityClass, id)
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
        for (const entity of res) {
            this.traverseEntity(entity, options.relations || null, (e) => this.cache.add(e))
        }
        return res
    }

    async findBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        await this.commit()
        const res = await super.findBy(entityClass as EntityClass<E>, where)
        for (const entity of res) {
            this.traverseEntity(entity, null, (e) => this.cache.add(e))
        }
        return res
    }

    async findOne<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E | undefined> {
        await this.commit()
        const res = await super.findOne(entityClass as EntityClass<E>, options)
        if (res != null) {
            this.traverseEntity(res, options.relations || null, (e) => this.cache.add(e))
        }
        return res
    }

    async findOneOrFail<E extends Entity>(entityClass: EntityTarget<E>, options: FindOneOptions<E>): Promise<E> {
        await this.commit()
        const res = await super.findOneOrFail(entityClass as EntityClass<E>, options)
        this.traverseEntity(res, options.relations || null, (e) => this.cache.add(e))
        return res
    }

    async findOneBy<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        await this.commit()
        const res = await super.findOneBy(entityClass as EntityClass<E>, where)
        if (res != null) {
            this.traverseEntity(res, null, (e) => this.cache.add(e))
        }
        return res
    }

    async findOneByOrFail<E extends Entity>(
        entityClass: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        await this.commit()
        const res = await super.findOneByOrFail(entityClass as EntityClass<E>, where)
        this.traverseEntity(res, null, (e) => this.cache.add(e))
        return res
    }

    async get<E extends Entity>(entityClass: EntityTarget<E>, id: string): Promise<E | undefined>
    async get<E extends Entity>(entityClass: EntityTarget<E>, options: GetOptions<E>): Promise<E | undefined>
    async get<E extends Entity>(
        entityClass: EntityTarget<E>,
        idOrOptions: string | GetOptions<E>
    ): Promise<E | undefined> {
        const {id, ...options} = parseGetOptions(idOrOptions)

        let entity = this.getCached(entityClass, id, options.relations)
        if (entity !== undefined) return entity ?? undefined

        await this.load()

        entity = this.getCached(entityClass, id, options.relations)
        if (entity !== undefined) return entity ?? undefined

        return await this.findOne(entityClass, {where: {id} as any, relations: options.relations})
    }

    async getOrFail<E extends Entity>(entityClass: EntityTarget<E>, id: string): Promise<E>
    async getOrFail<E extends Entity>(entityClass: EntityTarget<E>, options: GetOptions<E>): Promise<E>
    async getOrFail<E extends Entity>(entityClass: EntityTarget<E>, idOrOptions: string | GetOptions<E>): Promise<E> {
        const options = parseGetOptions(idOrOptions)
        let e = await this.get(entityClass, options)

        if (e == null) {
            const metadata = this.getEntityMetadata(entityClass)
            throw new Error(`Missing entity ${metadata.name} with id "${options.id}"`)
        }

        return e
    }

    async getOrInsert<E extends Entity>(
        entityClass: EntityTarget<E>,
        id: string,
        create: (id: string) => E | Promise<E>
    ): Promise<E>
    async getOrInsert<E extends Entity>(
        entityClass: EntityTarget<E>,
        options: GetOptions<E>,
        create: (id: string) => E | Promise<E>
    ): Promise<E>
    async getOrInsert<E extends Entity>(
        entityClass: EntityTarget<E>,
        idOrOptions: string | GetOptions<E>,
        create: (id: string) => E | Promise<E>
    ): Promise<E> {
        const options = parseGetOptions(idOrOptions)
        let e = await this.get(entityClass, options)

        if (e == null) {
            e = await create(options.id)
            await this.insert(e)
        }

        return e
    }

    /**
     * @deprecated use {@link getOrInsert} instead
     */
    async getOrCreate<E extends Entity>(
        entityClass: EntityTarget<E>,
        idOrOptions: string | GetOptions<E>,
        create: (id: string) => E | Promise<E>
    ) {
        return this.getOrInsert(entityClass, idOrOptions as any, create)
    }

    defer<E extends Entity>(entityClass: EntityTarget<E>, id: string): DeferredEntity<E>
    defer<E extends Entity>(entityClass: EntityTarget<E>, options: GetOptions<E>): DeferredEntity<E>
    defer<E extends Entity>(entityClass: EntityTarget<E>, idOrOptions: string | GetOptions<E>): DeferredEntity<E> {
        const options = parseGetOptions(idOrOptions)
        this.queue.add(entityClass, options.id, options.relations)

        return new DeferredEntity({
            get: async () => this.get(entityClass, options),
            getOrFail: async () => this.getOrFail(entityClass, options),
            getOrInsert: async (create) => this.getOrInsert(entityClass, options, create),
        })
    }

    async commit(): Promise<void> {
        const log = this.logger.child('commit')

        const entityOrder = this.getCommitOrder()

        const changeSets: ChangeSet<any>[] = []
        for (const metadata of entityOrder) {
            const changeSet = this.getChangeSet(metadata.target)
            changeSets.push(changeSet)
        }

        for (const {metadata, inserts, upserts} of changeSets) {
            if (upserts.length > 0) {
                log.debug(`commit upserts for ${metadata.name} (${upserts.length})`)
                await super.upsert(upserts)
            }

            if (inserts.length) {
                log.debug(`commit inserts for ${metadata.name} (${inserts.length})`)
                await super.insert(inserts)
            }
        }

        const changeSetsReversed = [...changeSets].reverse()
        for (const {metadata, removes} of changeSetsReversed) {
            if (removes.length > 0) {
                log.debug(`commit removes for ${metadata.name} (${removes.length})`)
                await super.remove(removes)
            }
        }

        for (const {metadata, extraUpserts} of changeSets) {
            if (extraUpserts.length) {
                log.debug(`commit extra upserts for ${metadata.name} (${extraUpserts.length})`)
                await super.upsert(extraUpserts)
            }
        }

        this.updates.clear()
    }

    clear(): void {
        this.cache.clear()
    }

    async flush(): Promise<void> {
        await this.commit()
        this.clear()
    }

    private getChangeSet<E extends Entity>(target: EntityTarget<E>): ChangeSet<E> {
        const metadata = this.getEntityMetadata(target)

        const inserts: E[] = []
        const upserts: E[] = []
        const removes: E[] = []
        const extraUpserts: E[] = []

        const updates = this.updates.getUpdates(metadata.target)
        for (const [id, type] of updates) {
            const cached = this.cache.get(metadata.target, id) as CachedEntity<E>

            switch (type) {
                case UpdateType.Insert: {
                    assert(cached?.value != null)

                    inserts.push(cached.value)

                    const extraUpsert = this.extractExtraUpsert(cached.value)
                    if (extraUpsert != null) {
                        extraUpserts.push(extraUpsert)
                    }

                    break
                }
                case UpdateType.Upsert: {
                    assert(cached?.value != null)

                    upserts.push(cached.value)

                    const extraUpsert = this.extractExtraUpsert(cached.value)
                    if (extraUpsert != null) {
                        extraUpserts.push(extraUpsert)
                    }

                    break
                }
                case UpdateType.Remove: {
                    const e = metadata.create() as E
                    e.id = id
                    removes.push(e)
                    break
                }
            }
        }

        return {metadata, inserts, upserts, extraUpserts, removes}
    }

    private async load(): Promise<void> {
        for (const {target, data} of this.queue.values()) {
            const metadata = this.getEntityMetadata(target)

            for (const id of data.ids) {
                this.cache.ensure(metadata.target, id)
            }

            for (let batch of splitIntoBatches([...data.ids], 30000)) {
                if (batch.length == 0) continue
                await this.find<any>(metadata.target, {where: {id: In(batch)}, relations: data.relations})
            }
        }

        this.queue.clear()
    }

    private getCached<E extends Entity>(entityClass: EntityTarget<E>, id: string, mask?: FindOptionsRelations<any>) {
        const cached = this.cache.get(entityClass, id)

        if (cached == null) {
            return undefined
        } else if (cached.value == null) {
            return null
        } else {
            return this.cloneEntity(cached.value, mask || null)
        }
    }

    private extractExtraUpsert<E extends Entity>(entity: E) {
        const metadata = this.getEntityMetadata(entity.constructor)

        const commitOrderIndex = this.getCommitOrderIndex(metadata)

        let extraUpsert: E | undefined
        for (const relation of metadata.relations) {
            if (relation.foreignKeys.length == 0) continue

            const inverseEntity = relation.getEntityValue(entity)
            if (inverseEntity == null) continue

            const inverseMetadata = relation.inverseEntityMetadata
            if (metadata === inverseMetadata && inverseEntity.id === entity.id) continue

            const invCommitOrderIndex = this.getCommitOrderIndex(inverseMetadata)
            if (invCommitOrderIndex < commitOrderIndex) continue

            assert(relation.isNullable)

            const invUpdateType = this.updates.get(inverseMetadata.target, inverseEntity.id)
            if (invUpdateType === UpdateType.Insert) {
                if (extraUpsert == null) {
                    extraUpsert = metadata.create() as E
                    extraUpsert.id = entity.id
                    Object.assign(extraUpsert, entity)
                }

                relation.setEntityValue(entity, undefined)
            }
        }

        return extraUpsert
    }

    @def
    private getCommitOrder() {
        const em = this.em()
        return getCommitOrder(em.connection.entityMetadatas)
    }

    private commitOrderIndexes: Map<EntityMetadata, number> | undefined
    private getCommitOrderIndex(metadata: EntityMetadata) {
        if (this.commitOrderIndexes == null) {
            const order = this.getCommitOrder()
            this.commitOrderIndexes = new Map(order.map((m, i) => [m, i]))
        }

        const index = this.commitOrderIndexes.get(metadata)
        assert(index != null)

        return index
    }

    private cloneEntity<E extends Entity>(entity: E, mask: FindOptionsRelations<any> | null): E | undefined {
        const metadata = this.getEntityMetadata(entity.constructor)

        const clonedEntity = metadata.create()

        for (const column of metadata.nonVirtualColumns) {
            const objectColumnValue = column.getEntityValue(entity)
            if (objectColumnValue !== undefined) {
                column.setEntityValue(clonedEntity, copy(objectColumnValue))
            }
        }

        if (mask != null) {
            for (const relation of metadata.relations) {
                const inverseMask = mask[relation.propertyName]
                if (!inverseMask) continue

                const inverseEntity = relation.getEntityValue(entity)
                if (inverseEntity === undefined) {
                    return undefined // relation is missing, but required
                } else if (inverseEntity === null) {
                    relation.setEntityValue(clonedEntity, null)
                } else {
                    const cachedInverseEntity = this.cloneEntity(
                        inverseEntity,
                        typeof inverseMask === 'boolean' ? null : inverseMask
                    )
                    if (cachedInverseEntity == null) {
                        return undefined
                    } else {
                        relation.setEntityValue(clonedEntity, cachedInverseEntity)
                    }
                }
            }
        }

        return clonedEntity
    }

    private traverseEntity(entity: Entity, mask: FindOptionsRelations<any> | null, fn: (e: Entity) => void) {
        const metadata = this.getEntityMetadata(entity.constructor)

        if (mask != null) {
            for (const relation of metadata.relations) {
                const inverseMask = mask[relation.propertyName]
                if (!inverseMask) continue

                const inverseEntity = relation.getEntityValue(entity)
                if (relation.isOneToMany || relation.isManyToMany) {
                    if (!Array.isArray(inverseEntity)) continue
                    for (const entity of inverseEntity) {
                        this.traverseEntity(entity, inverseMask === true ? null : inverseMask, fn)
                    }
                } else if (inverseEntity != null) {
                    this.traverseEntity(inverseEntity, inverseMask === true ? null : inverseMask, fn)
                }
            }
        }

        fn(entity)
    }

    private getEntityMetadata(entityClass: EntityTarget<any>) {
        const em = this.em()
        return em.connection.getMetadata(entityClass)
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

export class DeferredEntity<E extends Entity> {
    constructor(
        private opts: {
            get: () => Promise<E | undefined>
            getOrFail: () => Promise<E>
            getOrInsert: (create: (id: string) => E | Promise<E>) => Promise<E>
        }
    ) {}

    async get(): Promise<E | undefined> {
        return this.opts.get()
    }

    async getOrFail(): Promise<E> {
        return this.opts.getOrFail()
    }

    async getOrInsert(create: (id: string) => E | Promise<E>): Promise<E> {
        return this.opts.getOrInsert(create)
    }

    /**
     * @deprecated use {@link getOrInsert} instead
     */
    async getOrCreate(create: (id: string) => E | Promise<E>): Promise<E> {
        return this.getOrInsert(create)
    }
}

function parseGetOptions<E>(idOrOptions: string | GetOptions<E>): GetOptions<E> {
    if (typeof idOrOptions === 'string') {
        return {id: idOrOptions}
    } else {
        return idOrOptions
    }
}
