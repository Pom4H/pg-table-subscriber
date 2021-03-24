const { Client } = require('pg');
const createSubscriber = require('pg-listen');

const dropTriggerFunctionSql = (trigger) => `DROP FUNCTION IF EXISTS ${trigger}()`;

const dropTriggerSql = (trigger, table) => `DROP TRIGGER IF EXISTS ${trigger} ON ${table}`;

const createTriggerFunctionSql = (trigger) => `CREATE OR REPLACE FUNCTION ${trigger}()
    RETURNS trigger AS $$
    BEGIN
    PERFORM pg_notify(
        '${trigger}',
        json_build_object(
        ${trigger.split('_')[2] === 'update' ? `'previous', row_to_json(OLD),` : ''}
        'record', row_to_json(NEW)
        )::text
    );
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    `;

const createTriggerSql = ({ trigger, when, operation, table }) => `CREATE TRIGGER ${trigger}
    ${when} ${operation}
    ON ${table}
    FOR EACH ROW
    EXECUTE PROCEDURE ${trigger}()`;


class PgTableSubscriber {
    #config;
    #logger;
    #listeners = new Map();

    start = async ({ logger, ...config } = {}) => {
        this.#logger = logger || console;
        this.#config = config;
        this.subscriber = await createSubscriber(config);
        await this.subscriber.connect();

        for (const [trigger, listeners] of this.#listeners.entries()) {
            for (const listener of listeners) {
                this.subscriber.notifications.on(trigger, listener);
                await this.#createTrigger(trigger);
                await this.subscriber.listenTo(trigger);
                this.#logger.info(`Subscribed to: ${trigger}`);
            }
        }
    }

    end = async () => {
        await this.subscriber.unlistenAll();
        await this.subscriber.close();
        if (!this.#config.keepTriggers) {
            for (const trigger of this.#listeners.keys()) {
                await this.#dropTrigger(trigger);
                this.#logger.info(`Cleanup trigger: ${trigger}`);
            }
        }
    }

    beforeInsert = (table, cb) => this.#addListener(`before_${table}_insert`, cb);
    beforeUpdate = (table, cb) => this.#addListener(`before_${table}_update`, cb);
    beforeDelete = (table, cb) => this.#addListener(`before_${table}_delete`, cb);
    afterInsert = (table, cb) => this.#addListener(`after_${table}_insert`, cb);
    afterUpdate = (table, cb) => this.#addListener(`after_${table}_update`, cb);
    afterDelete = (table, cb) => this.#addListener(`after_${table}_delete`, cb);

    #addListener = (trigger, cb) => {
        if (!trigger || !cb) return;
        const callbacks = this.#listeners.get(trigger);
        if (callbacks) {
            callbacks.add(cb);
        } else {
            this.#listeners.set(trigger, new Set([cb]));
        }
    }

    #createTrigger = async (trigger) => {
        const [when, table, operation] = trigger.split('_');
        await this.#runTransaction([
            createTriggerFunctionSql(trigger),
            createTriggerSql({ trigger, when, operation, table })
        ]);
    }

    #dropTrigger = async (trigger) => {
        const [, table,] = trigger.split('_');
        await this.#runTransaction([
            dropTriggerSql(trigger, table),
            dropTriggerFunctionSql(trigger, table),
        ]);
    }

    #runTransaction = async (queries) => {
        const client = new Client(this.#config)
        await client.connect();
        try {
            await client.query('BEGIN');
            for (const query of queries) {
                await client.query(query);
            }
            await client.query('COMMIT');
        } catch (e) {
            await client.query('ROLLBACK');
            throw e;
        } finally {
            await client.end();
        }
    }

    static #instance;
    static get Singleton() {
        if (!PgTableSubscriber.#instance) {
            PgTableSubscriber.#instance = new PgTableSubscriber();
        }
        return PgTableSubscriber.#instance;
    }
};

module.exports = PgTableSubscriber.Singleton;
