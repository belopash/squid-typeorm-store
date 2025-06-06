import {createLogger} from '@subsquid/logger'
import {
    TypeormDatabase as TypeormDatabase_,
    TypeormDatabaseOptions as TypeormDatabaseOptions_,
    IsolationLevel,
} from '@subsquid/typeorm-store'
import {ChangeTracker, rollbackBlock} from '@subsquid/typeorm-store/lib/hot'
import {DatabaseState, FinalTxInfo, HashAndHeight, HotTxInfo} from '@subsquid/typeorm-store/lib/interfaces'
import {assertNotNull, def, last, maybeLast} from '@subsquid/util-internal'
import assert from 'assert'
import {DataSource, EntityManager} from 'typeorm'
import {Store} from './store'
import {StateManager} from './utils/stateManager'
import {createOrmConfig} from '@subsquid/typeorm-config'

export {IsolationLevel}

export interface TypeormDatabaseOptions extends TypeormDatabaseOptions_ {
    /**
     * If true, will batch write operations
     * @default true
     */
    batchWriteOperations?: boolean

    /**
     * If true, will cache entities on request
     * @default true
     */
    cacheEntities?: boolean

    /**
     * If true, will sync entities on request
     * @default true
     */
    syncEntities?: boolean

    /**
     * If true, will reset the state on commit
     * @default true
     */
    resetOnCommit?: boolean

    /**
     * Batch size for database operations
     * @default 1000
     */
    batchSize?: number

    /**
     * Enable relation processing optimizations
     * @default true
     */
    optimizeRelations?: boolean
}

const StateManagerSymbol = Symbol('StateManager')

export class TypeormDatabase {
    protected statusSchema: string
    protected isolationLevel: IsolationLevel
    protected batchWriteOperations: boolean
    protected cacheEntities: boolean
    protected syncEntities: boolean
    protected resetOnCommit: boolean
    protected con?: DataSource & {
        [StateManagerSymbol]?: StateManager
    }
    protected projectDir: string

    public readonly supportsHotBlocks: boolean

    constructor(options?: TypeormDatabaseOptions) {
        this.statusSchema = options?.stateSchema || 'squid_processor'
        this.isolationLevel = options?.isolationLevel || 'SERIALIZABLE'
        this.batchWriteOperations = options?.batchWriteOperations ?? true
        this.cacheEntities = options?.cacheEntities ?? true
        this.syncEntities = options?.syncEntities ?? true
        this.resetOnCommit = options?.resetOnCommit ?? true
        this.supportsHotBlocks = options?.supportHotBlocks ?? true
        this.projectDir = options?.projectDir || process.cwd()
    }

    async connect(): Promise<DatabaseState> {
        assert(this.con == null, 'already connected')

        let cfg = createOrmConfig({projectDir: this.projectDir})
        this.con = new DataSource(cfg)

        await this.con.initialize()

        try {
            return await this.con.transaction('SERIALIZABLE', (em) => this.initTransaction(em))
        } catch (e: any) {
            await this.con.destroy().catch(() => {}) // ignore error
            this.con = undefined
            throw e
        }
    }

    async disconnect(): Promise<void> {
        await this.con?.destroy().catch(() => {}) // ignore error
        this.con = undefined
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

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
        )
        if (status.length == 0) {
            await em.query(`INSERT INTO ${schema}.status (id, height, hash) VALUES (0, -1, '0x')`)
            status.push({height: -1, hash: '0x', nonce: 0})
        }

        let top: HashAndHeight[] = await em.query(`SELECT height, hash FROM ${schema}.hot_block ORDER BY height`)

        return assertStateInvariants({...status[0], top})
    }

    private async getState(em: EntityManager): Promise<DatabaseState> {
        let schema = this.escapedSchema()

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
        )

        assert(status.length == 1)

        let top: HashAndHeight[] = await em.query(`SELECT hash, height FROM ${schema}.hot_block ORDER BY height`)

        return assertStateInvariants({...status[0], top})
    }

    transact(info: FinalTxInfo, cb: (store: Store) => Promise<void>): Promise<void> {
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

            await this.performUpdates(cb, em)

            await this.updateStatus(em, state.nonce, next)
        })
    }

    transactHot(info: HotTxInfo, cb: (store: Store, block: HashAndHeight) => Promise<void>): Promise<void> {
        return this.transactHot2(info, async (store, sliceBeg, sliceEnd) => {
            for (let i = sliceBeg; i < sliceEnd; i++) {
                await cb(store, info.newBlocks[i])
            }
        })
    }

    transactHot2(
        info: HotTxInfo,
        cb: (store: Store, sliceBeg: number, sliceEnd: number) => Promise<void>
    ): Promise<void> {
        return this.submit(async (em) => {
            let state = await this.getState(em)
            let chain = [state, ...state.top]

            assertChainContinuity(info.baseHead, info.newBlocks)
            assert(info.finalizedHead.height <= (maybeLast(info.newBlocks) ?? info.baseHead).height)

            assert(
                chain.find((b) => b.hash === info.baseHead.hash),
                RACE_MSG
            )
            if (info.newBlocks.length == 0) {
                assert(last(chain).hash === info.baseHead.hash, RACE_MSG)
            }
            assert(chain[0].height <= info.finalizedHead.height, RACE_MSG)

            let rollbackPos = info.baseHead.height + 1 - chain[0].height

            for (let i = chain.length - 1; i >= rollbackPos; i--) {
                await rollbackBlock(this.statusSchema, em, chain[i].height)
            }

            if (info.newBlocks.length) {
                let finalizedEnd = info.finalizedHead.height - info.newBlocks[0].height + 1
                if (finalizedEnd > 0) {
                    await this.performUpdates((store) => cb(store, 0, finalizedEnd), em)
                } else {
                    finalizedEnd = 0
                }
                for (let i = finalizedEnd; i < info.newBlocks.length; i++) {
                    let b = info.newBlocks[i]
                    await this.insertHotBlock(em, b)
                    await this.performUpdates(
                        (store) => cb(store, i, i + 1),
                        em,
                        new ChangeTracker(em, this.statusSchema, b.height)
                    )
                }
            }

            chain = chain.slice(0, rollbackPos).concat(info.newBlocks)

            let finalizedHeadPos = info.finalizedHead.height - chain[0].height
            assert(chain[finalizedHeadPos].hash === info.finalizedHead.hash)
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
        cb: (store: Store) => Promise<void>,
        em: EntityManager,
        changeWriter?: ChangeTracker
    ): Promise<void> {
        let store = new Store({
            em,
            state: this.getStateManager(),
            logger: this.getLogger().child('store'),
            changes: changeWriter,
            batchWriteOperations: this.batchWriteOperations,
            cacheEntities: this.cacheEntities,
            syncEntities: this.syncEntities,
        })

        try {
            await cb(store)

            if (this.resetOnCommit) {
                await store.flush()
            } else {
                await store.sync()
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
        assert(b.height === prev.height + 1, 'blocks must form a continues chain')
        prev = b
    }
}
