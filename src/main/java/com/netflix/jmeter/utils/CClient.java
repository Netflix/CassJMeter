package com.netflix.jmeter.utils;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CClient extends Cassandra.Client
{
    public String host;
    public TSocket socket;

    public CClient(TSocket socket, TBinaryProtocol tBinaryProtocol, String h)
    {
        super(tBinaryProtocol);
        this.host = h;
        this.socket = socket;
    }

    public static CClient getClient(String currentNode, int port)
    {
        try
        {
            TSocket socket = new TSocket(currentNode, port);
            TTransport transport = new TFramedTransport(socket);
            CClient client = new CClient(socket, new TBinaryProtocol(transport), currentNode);
            transport.open();
            return client;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
