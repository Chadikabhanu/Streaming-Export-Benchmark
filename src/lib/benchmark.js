const { Writable } = require('stream');
const { pipeline } = require('stream/promises'); // Node.js 15+ standard for safe streams
const QueryStream = require('pg-query-stream');
const { JSONStreamFormatter, CSVStreamFormatter, XMLStreamFormatter, ParquetStreamFormatter } = require('./formatters');

/**
 * Helper to force Garbage Collection if exposed.
 * Run node with --expose-gc for this to work (e.g., node --expose-gc index.js)
 */
const forceGC = () => {
    if (global.gc) {
        global.gc();
    }
};

async function measureExport(pool, format, queryText, columns) {
    const client = await pool.connect();
    let bytesWritten = 0;
    let peakMem = 0;
    let chunkCount = 0;

    // 1. Factory for formatters to keep logic clean
    const getFormatter = (fmt) => {
        switch (fmt) {
            case 'json': return new JSONStreamFormatter(columns);
            case 'csv': return new CSVStreamFormatter(columns);
            case 'xml': return new XMLStreamFormatter(columns);
            case 'parquet': return new ParquetStreamFormatter(columns);
            default: throw new Error(`Unknown format: ${fmt}`);
        }
    };

    const formatter = getFormatter(format);
    const start = process.hrtime();

    // 2. Optimized Writable: Only checks memory every 500 chunks
    const nullStream = new Writable({
        write(chunk, encoding, callback) {
            bytesWritten += chunk.length;
            chunkCount++;
            
            // Checking process.memoryUsage() is expensive. 
            // We sample it every 500 chunks to prevent the benchmark logic 
            // from slowing down the actual export.
            if (chunkCount % 500 === 0) {
                const used = process.memoryUsage().heapUsed / 1024 / 1024;
                if (used > peakMem) peakMem = used;
            }
            callback();
        }
    });

    try {
        const query = new QueryStream(queryText);
        const dbStream = client.query(query);

        // 3. Use pipeline() instead of pipe(). 
        // This automatically handles error propagation and cleanup 
        // if the DB connection fails or formatter crashes.
        await pipeline(
            dbStream,
            formatter,
            nullStream
        );

        const diff = process.hrtime(start);
        
        return {
            format,
            durationSeconds: parseFloat((diff[0] + diff[1] / 1e9).toFixed(4)),
            fileSizeBytes: bytesWritten,
            fileSizeMB: (bytesWritten / 1024 / 1024).toFixed(2),
            peakMemoryMB: Math.round(peakMem * 100) / 100
        };
    } catch (err) {
        console.error(`Error benchmarking ${format}:`, err);
        throw err;
    } finally { 
        client.release(); 
    }
}

async function runBenchmark(pool) {
    const results = [];
    
    // Defined outside to easily change for all formats
    const query = 'SELECT id, name, value, metadata FROM records';
    const columns = [
        { source: 'id', target: 'id' }, { source: 'name', target: 'name' },
        { source: 'value', target: 'val' }, { source: 'metadata', target: 'meta' }
    ];

    const formats = ['csv', 'json', 'xml', 'parquet'];

    for (const fmt of formats) {
        console.log(`Benchmarking ${fmt}...`);
        
        // 4. Reset environment
        // Force GC to prevent previous run's memory from polluting this run
        forceGC(); 
        // distinct pause to let event loop settle
        await new Promise(resolve => setTimeout(resolve, 200)); 

        const result = await measureExport(pool, fmt, query, columns);
        results.push(result);
    }
    return results;
}

module.exports = { runBenchmark };