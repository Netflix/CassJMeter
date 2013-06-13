package com.matriks.jmeter.connections.datastaxclient;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;

public class DataStaxClientOperation implements Operation {
	private AbstractSerializer valueSerializer;
	private ColumnFamily<Object, Object> cfs;
	private AbstractSerializer columnSerializer;
	private final String cfName;
	private final boolean isCounter;

	public class DataStaxClientResponseData extends ResponseData {
		public DataStaxClientResponseData(String response, int size, String host, long latency, Object key, Object cn, Object value) {
			super(response, size, host, latency, key, cn, value);
		}

		public DataStaxClientResponseData(String response, int size, OperationResult<?> result, Object key, Object cn, Object value) {
			super(response, size, EXECUTED_ON + (result != null ? result.getHost().getHostName() : ""), (result != null ? result
					.getLatency(TimeUnit.MILLISECONDS) : 0), key, cn, value);
		}

		public DataStaxClientResponseData(String response, int size, OperationResult<?> result, Object key, Map<?, ?> kv) {
			super(response, size, (result == null) ? "" : result.getHost().getHostName(), result != null ? result.getLatency(TimeUnit.MILLISECONDS)
					: 0, key, kv);
		}
	}

	public DataStaxClientOperation(String cfName, boolean isCounter) {
		this.cfName = cfName;
		this.isCounter = isCounter;
	}

	@Override
	public void serlizers(AbstractSerializer<?> keySerializer, AbstractSerializer<?> columnSerializer, AbstractSerializer<?> valueSerializer) {
		this.cfs = new ColumnFamily(cfName, keySerializer, columnSerializer);
		this.columnSerializer = columnSerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public ResponseData put(Object key, Object colName, Object value) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseData get(Object rkey, Object colName) throws OperationException {
		Session session = DataStaxClientConnection.instance.session();
		TableMetadata tm = DataStaxClientConnection.instance.getKeyspaceMetadata().getTable(cfName);
		String partitionKey = tm.getPartitionKey().get(0).getName();
		
		Query query = QueryBuilder.select(colName.toString()).from(cfName).where(QueryBuilder.eq(partitionKey, rkey)).limit(1000000)
				.setConsistencyLevel(ConsistencyLevel.valueOf(com.netflix.jmeter.properties.Properties.instance.cassandra.getReadConsistency()));
		
		ResultSetFuture rs = session.executeAsync(query);

		int size = 0;
		try {
			Row row = rs.getUninterruptibly(1000000, TimeUnit.MILLISECONDS).one();
			size = row != null ? row.getBytesUnsafe(colName.toString()).capacity() : 0;
		}
		catch (TimeoutException e) {
			e.printStackTrace();
			throw new OperationException(e);
		}

		return new DataStaxClientResponseData("", size, "", 0, rkey, colName, null);
	}

	@Override
	public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseData putComposite(String key, String colName, ByteBuffer vbb) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseData batchCompositeMutate(String key, Map<String, ByteBuffer> nv) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseData getComposite(String key, String compositeColName) throws OperationException {
		Session session = DataStaxClientConnection.instance.session();

		TableMetadata tm = DataStaxClientConnection.instance.getKeyspaceMetadata().getTable(cfName);
		String partitionKey = tm.getPartitionKey().get(0).getName();
		Object partitionValue = key;

		String[] colList = compositeColName.split(":");
		String clusteredKey = colList[0];
		String clusteredValue = colList[1];
		String colName = colList[2];
		
		Long start = System.nanoTime();
		
		ResultSetFuture rs = session.executeAsync(QueryBuilder.select(colName).from(cfName).where(QueryBuilder.eq(partitionKey, partitionValue))
				.and(QueryBuilder.eq(clusteredKey, clusteredValue)).limit(1000000));
		
		int size = 0;
		
		try {
			Row row = rs.getUninterruptibly(1000000, TimeUnit.MILLISECONDS).one();
			size = row != null ? row.getBytesUnsafe(colName.toString()).capacity() : 0;
		}

		catch (TimeoutException e) {
			e.printStackTrace();
			throw new OperationException(e);
		}
		
		Long duration = System.nanoTime() - start;
		
		return new DataStaxClientResponseData("", size, "", TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS), key, compositeColName, null);
	}

	@Override
	public ResponseData delete(Object rkey, Object colName) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

}
