duckdb = require('../../duckdb/tools/nodejs/lib')
nodearrow = require('../')
arrow = require('apache-arrow')
assert = require('assert');
const vector_size = 1024

describe('DuckDB to Arrow IPC demo', async () => {
	let db;
	let conn;
	const batch_size = vector_size * 3;
	const total = vector_size * 10;

	before((done) => {
		db = new duckdb.Database(':memory:', () => {
			conn = new duckdb.Connection(db, done);
		});
	});

	it('streaming arrow', async () => {
		let got_batches = 0;
		const arrowStreamResult = await conn.arrowStream('SELECT * FROM range(0, ?)', total);
		while (true) {
			let RecordBatch = await arrowStreamResult.nextRecordBatch(batch_size);
			if (!RecordBatch) break;

			got_batches++;
			cDataPointers = RecordBatch.GetCDataPointers();
			ipc = nodearrow.arraytoipc(cDataPointers.data, cDataPointers.schema)
			reader = arrow.RecordBatchReader.from(ipc);
			batch = reader.readAll()[0];

			if (got_batches < Math.ceil(total / batch_size)) {
				assert.equal(batch.toArray().length, batch_size);
			}
		}

		assert.equal(got_batches, Math.ceil(total / batch_size));
	});
});

describe('[Benchmark] Single INT column', async () => {
	// Config
	const batch_size = vector_size*100;
	const column_size = 10*1000*1000;

	let db;
	let conn;

	before((done) => {
		db = new duckdb.Database(':memory:', () => {
			db.run("CREATE TABLE test AS select * FROM range(0,?) tbl(i);", column_size, (err) => {
				conn = new duckdb.Connection(db, done);
			});
		});
	});

	it('DuckDB + IPC conversion (batch_size=' + batch_size + ')', async () => {
		let got_batches = 0;
		let got_rows = 0;
		const batches = [];

		const queryResult = await conn.arrowStream('SELECT * FROM test;');

		while(true) {
			let RecordBatch = await queryResult.nextRecordBatch(batch_size);
			if (!RecordBatch) {
				break;
			}
			cDataPointers = RecordBatch.GetCDataPointers();
			ipc = nodearrow.arraytoipc(cDataPointers.data, cDataPointers.schema)
			reader = arrow.RecordBatchReader.from(ipc);
			batch = reader.readAll()[0];
			batches.push(batch)
			got_rows += batch.data.length;
			got_batches++;
		}
		assert.equal(got_rows, column_size);
		assert.equal(got_batches, Math.ceil(column_size/batch_size));
	});

	it('DuckDB materialize full table in JS', (done) => {
		conn.all('SELECT * FROM test;', (err, res) => {
			assert.equal(res.length, column_size);
			done()
		});
	});
});

describe('[Benchmark] TPCH SF1 lineitem into arrow IPC format', async () => {
	// Config
	const batch_size = vector_size*100;
	const expected_rows = 60175;
	const parquet_file_path = "/tmp/lineitem_sf0_01.parquet";
	// const expected_rows = 6001215;
	// const parquet_file_path = "/tmp/lineitem_sf1.parquet";

	let db;
	let conn;

	before((done) => {
		db = new duckdb.Database(':memory:', () => {
			conn = new duckdb.Connection(db, done);
		});
	});

	it('DuckDB + IPC conversion (batch_size=' + batch_size + ')', async () => {
		const batches = [];
		let got_rows = 0;

		const queryResult = await conn.arrowStream('SELECT * FROM "' + parquet_file_path + '";');

		while(true) {
			let arrowArrayCData = await queryResult.nextRecordBatch(batch_size);
			if (!arrowArrayCData) {
				break;
			}
			cDataPointers = arrowArrayCData.GetCDataPointers();
			ipc = nodearrow.arraytoipc(cDataPointers.data, cDataPointers.schema)
			reader = arrow.RecordBatchReader.from(ipc);
			batch = reader.readAll()[0];
			batches.push(batch)
			got_rows += batch.data.length;
		}
		assert.equal(got_rows, expected_rows);
	});

	it('Arrow Parquet reader + IPC conversion', async () => {
		const batches = [];
		let total_size = 0;
		ipc = nodearrow.parquettoipc('file://' + parquet_file_path);
		reader = arrow.RecordBatchReader.from(ipc);
		for await (const batch of reader) {
			batches.push(batch);
			total_size += batch.data.length;
		}
		assert.equal(total_size, expected_rows);
	});

});
