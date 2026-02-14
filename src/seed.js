const { Pool } = require('pg');

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

(async () => {
    const client = await pool.connect();
    try {
        console.log('🌱 Seeding Database...');
        await client.query('DROP TABLE IF EXISTS records');
        await client.query(`
            CREATE TABLE records (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INTEGER,
                metadata JSONB
            )
        `);

        console.log('Inserting 100,000 rows...');
        const BATCH = 5000;
        for (let i = 0; i < 100000; i += BATCH) {
            const values = [];
            for (let j = 0; j < BATCH; j++) {
                const meta = JSON.stringify({ tag: 'test', index: i+j });
                values.push(`('Record_${i+j}', ${Math.floor(Math.random()*1000)}, '${meta}'::jsonb)`);
            }
            await client.query(`INSERT INTO records (name, value, metadata) VALUES ${values.join(',')}`);
        }
        console.log('✅ Done!');
    } catch (e) { console.error(e); } 
    finally { client.release(); pool.end(); }
})();