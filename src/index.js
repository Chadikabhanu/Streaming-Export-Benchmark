const { Writable } = require('stream');
const { pipeline } = require('stream/promises');
const { Pool } = require('pg');
const QueryStream = require('pg-query-stream');
// Adjust path if your formatters are in lib/
const { JSONStreamFormatter, CSVStreamFormatter, XMLStreamFormatter, ParquetStreamFormatter } = require('./lib/formatters'); 

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

const forceGC = () => { if (global.gc) global.gc(); };

async function measureExport(format, queryText, columns) {
    const client = await pool.connect();
    let bytesWritten = 0, peakMem = 0, chunkCount = 0;

    const getFormatter = (fmt) => {
        switch (fmt) {
            case 'json': return new JSONStreamFormatter(columns);
            case 'csv': return new CSVStreamFormatter(columns);
            case 'xml': return new XMLStreamFormatter(columns);
            case 'parquet': return new ParquetStreamFormatter(columns);
            default: throw new Error(`Unknown format: ${fmt}`);
        }
    };

    const nullStream = new Writable({
        write(chunk, encoding, callback) {
            bytesWritten += chunk.length;
            chunkCount++;
            if (chunkCount % 500 === 0) {
                const used = process.memoryUsage().heapUsed / 1024 / 1024;
                if (used > peakMem) peakMem = used;
            }
            callback();
        }
    });

    try {
        const start = process.hrtime();
        const dbStream = client.query(new QueryStream(queryText));
        await pipeline(dbStream, getFormatter(format), nullStream);
        const diff = process.hrtime(start);
        
        return {
            format,
            duration: (diff[0] + diff[1] / 1e9).toFixed(4) + 's',
            size: (bytesWritten / 1024 / 1024).toFixed(2) + ' MB',
            memory: (Math.round(peakMem * 100) / 100) + ' MB'
        };
    } finally { client.release(); }
}

(async () => {
    console.log('🚀 Starting Benchmark...');
    try {
        await pool.query('SELECT 1'); // Wait for DB
        const results = [];
        const query = 'SELECT id, name, value, metadata FROM records';
        const columns = [
            { source: 'id', target: 'id' }, { source: 'name', target: 'name' },
            { source: 'value', target: 'val' }, { source: 'metadata', target: 'meta' }
        ];

        for (const fmt of ['csv', 'json', 'xml', 'parquet']) {
            console.log(`Benchmarking ${fmt}...`);
            forceGC();
            await new Promise(r => setTimeout(r, 200));
            try { results.push(await measureExport(fmt, query, columns)); } 
            catch (e) { console.error(`Failed ${fmt}:`, e.message); }
        }
        console.table(results);
    } catch (err) { console.error('Fatal:', err); } 
    finally { await pool.end(); }
})();