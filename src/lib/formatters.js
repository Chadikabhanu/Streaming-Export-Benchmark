const { Transform } = require('stream');
const parquet = require('parquetjs');

/**
 * Helper to normalize values before formatting.
 * Handles Dates, NULLs, and JSON objects (JSONB).
 * Returns a raw string representation.
 */
function prepareValue(val) {
    if (val === null || val === undefined) {
        return '';
    }
    if (val instanceof Date) {
        return val.toISOString();
    }
    if (typeof val === 'object') {
        return JSON.stringify(val);
    }
    return String(val);
}

class JSONStreamFormatter extends Transform {
    constructor(columns) {
        super({ objectMode: true });
        this.columns = columns;
        this.isFirst = true;
        this.push('[');
    }

    _transform(chunk, encoding, callback) {
        const output = {};
        this.columns.forEach(col => {
            // Keep native types for JSON output
            output[col.target] = chunk[col.source];
        });
        
        try {
            const prefix = this.isFirst ? '' : ',';
            this.push(prefix + JSON.stringify(output));
            this.isFirst = false;
            callback();
        } catch (err) {
            callback(err);
        }
    }

    _flush(callback) {
        this.push(']');
        callback();
    }
}

class CSVStreamFormatter extends Transform {
    constructor(columns) {
        super({ objectMode: true });
        this.columns = columns;
        // Header Row
        this.push(columns.map(c => c.target).join(',') + '\n');
    }

    _transform(chunk, encoding, callback) {
        const row = this.columns.map(col => {
            let val = prepareValue(chunk[col.source]);

            // CSV Rule: Escape double quotes by doubling them (" -> "")
            // This applies to ALL strings, not just JSON objects
            if (val.includes('"')) {
                val = val.replace(/"/g, '""');
            }

            // CSV Rule: Wrap in quotes if it contains delimiter, newline, or quotes
            // For safety, we wrap everything in quotes to handle edge cases easily
            return `"${val}"`;
        }).join(',');

        this.push(row + '\n');
        callback();
    }
}

class XMLStreamFormatter extends Transform {
    constructor(columns) {
        super({ objectMode: true });
        this.columns = columns;
        this.push('<?xml version="1.0" encoding="UTF-8"?>\n<records>\n');
    }

    _transform(chunk, encoding, callback) {
        let rowXml = '  <record>\n';
        
        this.columns.forEach(col => {
            let val = prepareValue(chunk[col.source]);

            // XML Rule: Escape special entities to prevent invalid XML
            // e.g. "AT&T" becomes "AT&amp;T"
            val = val.replace(/&/g, '&amp;')
                     .replace(/</g, '&lt;')
                     .replace(/>/g, '&gt;')
                     .replace(/"/g, '&quot;')
                     .replace(/'/g, '&apos;');

            rowXml += `    <${col.target}>${val}</${col.target}>\n`;
        });

        this.push(rowXml + '  </record>\n');
        callback();
    }

    _flush(callback) {
        this.push('</records>');
        callback();
    }
}

class ParquetStreamFormatter extends Transform {
    constructor(columns) {
        super({ objectMode: true });
        this.columns = columns;
        
        // Define Schema: All fields as UTF8 Strings for maximum compatibility
        // (Parquet requires a strict schema, so we normalize everything to string)
        const schemaDefs = {};
        columns.forEach(col => {
            schemaDefs[col.target] = { type: 'UTF8', optional: true };
        });
        
        this.schema = new parquet.ParquetSchema(schemaDefs);
        this.rowCount = 0;
    }

    // Node.js v15+ API for async initialization
    async _construct(callback) {
        try {
            // Create a specialized writer that pipes data back to this stream
            this.writer = await parquet.ParquetWriter.openStream(this.schema, {
                write: (buffer) => {
                    this.push(buffer);
                    return Promise.resolve();
                },
                close: () => Promise.resolve()
            });
            callback();
        } catch (err) {
            callback(err);
        }
    }

    async _transform(chunk, encoding, callback) {
        try {
            const row = {};
            this.columns.forEach(col => {
                // Parquet writer expects string values for UTF8 columns
                row[col.target] = prepareValue(chunk[col.source]);
            });

            await this.writer.appendRow(row);

            // Memory Management: Yield to event loop every 1000 rows
            this.rowCount++;
            if (this.rowCount % 1000 === 0) {
                setImmediate(callback);
            } else {
                callback();
            }
        } catch (err) {
            callback(err);
        }
    }

    async _flush(callback) {
        try {
            if (this.writer) {
                await this.writer.close(); // Writes the Parquet footer
            }
            callback();
        } catch (err) {
            callback(err);
        }
    }
}

module.exports = { 
    JSONStreamFormatter, 
    CSVStreamFormatter, 
    XMLStreamFormatter, 
    ParquetStreamFormatter 
};