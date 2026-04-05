import {createLogger} from '@subsquid/logger'
import {
    TypeormDatabaseOptions as TypeormDatabaseOptions_,
    IsolationLevel,
    DatabaseTransactResult,
} from '@subsquid/typeorm-store'
import {ChangeTracker, rollbackBlock} from '@subsquid/typeorm-store/lib/hot'
import {DatabaseState, FinalTxInfo, HashAndHeight, HotBlock, HotTxInfo} from '@subsquid/typeorm-store/lib/interfaces'
import {TemplateMutation, TemplateRegistryTracker} from '@subsquid/typeorm-store/lib/templates'
import {assertNotNull, def, maybeLast} from '@subsquid/util-internal'
import assert from 'assert'
import {DataSource, EntityManager} from 'typeorm'
import {Store} from './store'
import {StateManager} from './utils/stateManager'
import {createOrmConfig} from '@subsquid/typeorm-config'

export {DatabaseTransactResult, TemplateMutation}

export {IsolationLevel}

export interface TypeormDatabaseOptions extends TypeormDatabaseOptions_ {
    /**
     * If true, will batch write operations
     * @default true
     */
    postponeWriteOperations?: boolean

    /**
     * If true, will cache entities on request
     * @default true
     */
    cacheEntities?: boolean

    /**
     * If true, will reset the state on commit
     * @default true
     */
    resetOnCommit?: boolean
}

const StateManagerSymbol = Symbol('StateManager')

export class TypeormDatabase {
    protected statusSchema: string
    protected isolationLevel: IsolationLevel
    protected postponeWriteOperations: boolean
    protected cacheEntities: boolean
    protected resetOnCommit: boolean
    protected con?: DataSource & {
        [StateManagerSymbol]?: StateManager
    }
    protected projectDir: string

    public readonly supportsHotBlocks: boolean
    public readonly supportsTemplates = true as const

    constructor(options?: TypeormDatabaseOptions) {
        this.statusSchema = options?.stateSchema || 'squid_processor'
        this.isolationLevel = options?.isolationLevel || 'SERIALIZABLE'
        this.postponeWriteOperations = options?.postponeWriteOperations ?? true
        this.cacheEntities = options?.cacheEntities ?? true
        this.resetOnCommit = options?.resetOnCommit ?? true
        this.supportsHotBlocks = options?.supportHotBlocks ?? true
        this.projectDir = options?.projectDir || process.cwd()
    }

    async connect(): Promise<DatabaseState> {
        assert(this.con == null, 'already connected')

        this.con = await this.initializeDataSource()

        try {
            return await this.con.transaction('SERIALIZABLE', (em) => this.initTransaction(em))
        } catch (e: any) {
            await this.con.destroy().catch(() => {}) // ignore error
            this.con = undefined
            throw e
        }
    }

    protected async initializeDataSource(): Promise<DataSource> {
        const cfg = createOrmConfig({projectDir: this.projectDir})
        const connection = new DataSource(cfg)
        return connection.initialize()
    }

    async disconnect(): Promise<void> {
        await this.con?.destroy().finally(() => this.con = undefined)
    }

    private async initTransaction(em: EntityManager): Promise<DatabaseState> {
        let schema = this.escapedSchema()

        await em.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`)
        await em.query(
            `CREATE TABLE IF NOT EXISTS ${schema}.status (` +
                `id int4 primary key, ` +
                `height int4 not null, ` +
                `hash text DEFAULT '0x', ` +
                `nonce int4 DEFAULT 0` +
                `)`
        )
        await em.query(
            // for databases created by prev version of typeorm store
            `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS hash text DEFAULT '0x'`
        )
        await em.query(
            // for databases created by prev version of typeorm store
            `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS nonce int DEFAULT 0`
        )
        await em.query(`CREATE TABLE IF NOT EXISTS ${schema}.hot_block (height int4 primary key, hash text not null)`)
        await em.query(
            `CREATE TABLE IF NOT EXISTS ${schema}.hot_change_log (` +
                `block_height int4 not null references ${schema}.hot_block on delete cascade, ` +
                `index int4 not null, ` +
                `change jsonb not null, ` +
                `PRIMARY KEY (block_height, index)` +
                `)`
        )
        await em.query(
            `CREATE TABLE IF NOT EXISTS ${schema}.template_registry (` +
                `key text not null, ` +
                `value text not null, ` +
                `type boolean not null, ` +
                `block_number int not null, ` +
                `height int not null, ` +
                `PRIMARY KEY (key, value, type, block_number)` +
                `)`
        )

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
        )
        if (status.length == 0) {
            await em.query(`INSERT INTO ${schema}.status (id, height, hash) VALUES (0, -1, '0x')`)
            return assertStateInvariants({height: -1, hash: '0x', nonce: 0, top: [], templates: []})
        }

        let rawTemplates: any[] = await em.query(
            `SELECT key, value, type, block_number FROM ${schema}.template_registry ` +
                `WHERE height <= $1 ORDER BY height, block_number, type, key, value`,
            [status[0].height]
        )
        let templates: TemplateMutation[] = rawTemplates.map(mapTemplateMutation)

        let hotBlocks: HashAndHeight[] = await em.query(`SELECT height, hash FROM ${schema}.hot_block ORDER BY height`)
        let top: HotBlock[] = []
        for (let block of hotBlocks) {
            let rawBlockTemplates: any[] = await em.query(
                `SELECT key, value, type, block_number FROM ${schema}.template_registry ` +
                    `WHERE height = $1 ORDER BY block_number, type, key, value`,
                [block.height]
            )
            top.push({...block, templates: rawBlockTemplates.map(mapTemplateMutation)})
        }

        return assertStateInvariants({...status[0], top, templates})
    }

    private async getState(em: EntityManager): Promise<DatabaseState> {
        let schema = this.escapedSchema()

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
        )

        assert(status.length == 1)

        let rawTop: HashAndHeight[] = await em.query(`SELECT hash, height FROM ${schema}.hot_block ORDER BY height`)
        let top: HotBlock[] = rawTop.map(b => ({...b, templates: []}))

        return assertStateInvariants({...status[0], top, templates: []})
    }

    transact(info: FinalTxInfo, cb: (store: Store) => Promise<DatabaseTransactResult | void>): Promise<void> {
        return this.submit(async (em) => {
            let state = await this.getState(em)
            let {prevHead: prev, nextHead: next} = info

            assert(state.hash === info.prevHead.hash, RACE_MSG)
            assert(state.height === prev.height)
            assert(prev.height < next.height)
            assert(prev.hash != next.hash)

            for (let i = state.top.length - 1; i >= 0; i--) {
                let block = state.top[i]
                await rollbackBlock(this.statusSchema, em, block.height)
            }

            await this.performUpdates(cb, em, new TemplateRegistryTracker(em, this.statusSchema, next.height))

            await this.updateStatus(em, state.nonce, next)
        })
    }

    transactHot(
        info: HotTxInfo,
        cb: (store: Store, block: HashAndHeight) => Promise<void>
    ): Promise<void> {
        return this.transactHot2(info, async (store, sliceBeg, sliceEnd) => {
            for (let i = sliceBeg; i < sliceEnd; i++) {
                await cb(store, info.newBlocks[i])
            }
        })
    }

    transactHot2(
        info: HotTxInfo,
        cb: (store: Store, sliceBeg: number, sliceEnd: number) => Promise<DatabaseTransactResult | void>
    ): Promise<void> {
        return this.submit(async (em) => {
            let state = await this.getState(em)
            let chain: HashAndHeight[] = [state, ...state.top]

            assertChainContinuity(info.baseHead, info.newBlocks)
            assert(info.finalizedHead.height <= (maybeLast(info.newBlocks) ?? info.baseHead).height)

            let baseHeadPos = chain.findIndex((b) => b.hash === info.baseHead.hash)
            assert(baseHeadPos >= 0, RACE_MSG)
            if (info.newBlocks.length == 0) {
                assert(baseHeadPos === chain.length - 1, RACE_MSG)
            }
            assert(chain[0].height <= info.finalizedHead.height, RACE_MSG)

            let rollbackPos = baseHeadPos + 1

            for (let i = chain.length - 1; i >= rollbackPos; i--) {
                await rollbackBlock(this.statusSchema, em, chain[i].height)
            }

            if (info.newBlocks.length) {
                let finalizedEnd = info.newBlocks.findIndex((b) => b.height > info.finalizedHead.height)
                if (finalizedEnd < 0) {
                    finalizedEnd = info.newBlocks.length
                }
                if (finalizedEnd > 0) {
                    await this.performUpdates(
                        (store) => cb(store, 0, finalizedEnd),
                        em,
                        new TemplateRegistryTracker(em, this.statusSchema, info.finalizedHead.height)
                    )
                }
                for (let i = finalizedEnd; i < info.newBlocks.length; i++) {
                    let b = info.newBlocks[i]
                    await this.insertHotBlock(em, b)
                    await this.performUpdates(
                        (store) => cb(store, i, i + 1),
                        em,
                        new TemplateRegistryTracker(em, this.statusSchema, b.height),
                        new ChangeTracker(em, this.statusSchema, b.height)
                    )
                }
            }

            chain = chain.slice(0, rollbackPos).concat(info.newBlocks)

            await this.deleteHotBlocks(em, info.finalizedHead.height)

            await this.updateStatus(em, state.nonce, info.finalizedHead)
        })
    }

    private deleteHotBlocks(em: EntityManager, finalizedHeight: number): Promise<void> {
        return em.query(`DELETE FROM ${this.escapedSchema()}.hot_block WHERE height <= $1`, [finalizedHeight])
    }

    private insertHotBlock(em: EntityManager, block: HashAndHeight): Promise<void> {
        return em.query(`INSERT INTO ${this.escapedSchema()}.hot_block (height, hash) VALUES ($1, $2)`, [
            block.height,
            block.hash,
        ])
    }

    private async updateStatus(em: EntityManager, nonce: number, next: HashAndHeight): Promise<void> {
        let schema = this.escapedSchema()

        let result: [data: any[], rowsChanged: number] = await em.query(
            `UPDATE ${schema}.status SET height = $1, hash = $2, nonce = nonce + 1 WHERE id = 0 AND nonce = $3`,
            [next.height, next.hash, nonce]
        )

        let rowsChanged = result[1]

        // Will never happen if isolation level is SERIALIZABLE or REPEATABLE_READ,
        // but occasionally people use multiprocessor setups and READ_COMMITTED.
        assert.strictEqual(rowsChanged, 1, RACE_MSG)
    }

    private async performUpdates(
        cb: (store: Store) => Promise<DatabaseTransactResult | void>,
        em: EntityManager,
        templateRegistry: TemplateRegistryTracker,
        changeWriter?: ChangeTracker
    ): Promise<void> {
        let store = new Store({
            em,
            state: this.getStateManager(),
            logger: this.getLogger().child('store'),
            changes: changeWriter,
            postponeWriteOperations: this.postponeWriteOperations,
            cacheEntities: this.cacheEntities,
        })

        try {
            let result = await cb(store)

            if (this.resetOnCommit) {
                await store.flush()
            } else {
                await store.sync()
            }

            if (result?.templates) {
                await templateRegistry.persist(result.templates)
            }
        } finally {
            store['isClosed'] = true
        }
    }

    private async submit(tx: (em: EntityManager) => Promise<void>): Promise<void> {
        let retries = 3
        while (true) {
            try {
                let con = this.con
                assert(con != null, 'not connected')
                return await con.transaction(this.isolationLevel, tx)
            } catch (e: any) {
                if (e.code == '40001' && retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    private escapedSchema(): string {
        let con = assertNotNull(this.con)
        return con.driver.escape(this.statusSchema)
    }

    @def
    private getLogger() {
        return createLogger('sqd:db')
    }

    private getStateManager() {
        let connection = assertNotNull(this.con)
        let stateManager = connection[StateManagerSymbol]
        if (stateManager == null) {
            stateManager = new StateManager({
                connection,
                logger: this.getLogger().child('state'),
            })
            connection[StateManagerSymbol] = stateManager
        }

        return stateManager
    }
}

const RACE_MSG = 'status table was updated by foreign process, make sure no other processor is running'

function assertStateInvariants(state: DatabaseState): DatabaseState {
    let height = state.height

    // Sanity check. Who knows what driver will return?
    assert(Number.isSafeInteger(height))

    assertChainContinuity(state, state.top)

    return state
}

function assertChainContinuity(base: HashAndHeight, chain: HashAndHeight[]) {
    let prev = base
    for (let b of chain) {
        assert(b.height > prev.height, 'blocks must form a continues chain')
        prev = b
    }
}

function mapTemplateMutation(r: any): TemplateMutation {
    return {
        type: r.type ? 'add' : 'delete',
        key: r.key,
        value: r.value,
        blockNumber: r.block_number,
    }
}
