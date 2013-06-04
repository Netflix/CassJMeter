package com.matriks.jmeter.connections.datastaxclient;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.SystemUtils;

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
			super(response, size, EXECUTED_ON + (result != null ? result.getHost().getHostName() : ""), (result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0), key, cn, value);
		}

		public DataStaxClientResponseData(String response, int size, OperationResult<?> result, Object key, Map<?, ?> kv) {
			super(response, size, (result == null) ? "" : result.getHost().getHostName(), result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0, key, kv);
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
		// TODO Auto-generated method stub
		return null;
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
		StringBuffer response = new StringBuffer();
		Session session = DataStaxClientConnection.instance.session();

		String[] partitionKeyList = key.split(":");
		String partitionKey = partitionKeyList[0];
		String partitionValue = partitionKeyList[1];

		String[] colList = compositeColName.split(":");
		String clusteredKey = colList[0];
		String clusteredKeyValue = colList[1];
		String colName = colList[2];

		ResultSet rs = session.execute(QueryBuilder.select(colName).from(cfName).where(QueryBuilder.eq(partitionKey, partitionValue)).limit(1000000).enableTracing());
		// .and(QueryBuilder.lte(clusteredKey, clusteredKeyValue)).and(QueryBuilder.gt(clusteredKey, clusteredKeyValue))

		for (Row row : rs) {
			String value = row != null ? SystemUtils.convertToString(valueSerializer, row.getBytesUnsafe(colName)) : null;
			response.append(value + "\n");
		}
		return new DataStaxClientResponseData(response.toString(), 0, "", TimeUnit.MILLISECONDS.convert(rs.getExecutionInfo().getQueryTrace().getDurationMicros(), TimeUnit.MICROSECONDS), key,
				compositeColName, null);
	}

	@Override
	public ResponseData delete(Object rkey, Object colName) throws OperationException {
		// TODO Auto-generated method stub
		return null;
	}

}
