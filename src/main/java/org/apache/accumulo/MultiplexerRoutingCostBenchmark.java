package org.apache.accumulo;

import org.apache.accumulo.core.rpc.RpcService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 8, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 5, warmups = 2)
public class MultiplexerRoutingCostBenchmark {

    @Param({"CLIENT", "TABLET_INGEST", "TABLET_MANAGEMENT"})
    public String rpcServiceName;

    private byte[] multiplexedFrame;
    private byte[] accumuloFrame;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        RpcService service = RpcService.valueOf(rpcServiceName);
        multiplexedFrame = MultiplexerBenchmark.buildMultiplexedFrame(service);
        accumuloFrame = MultiplexerBenchmark.buildAccumuloFrame(service);
    }

    @Benchmark
    public void standard_routingCostOnly(Blackhole bh) throws TException {
        TMemoryInputTransport transport = new TMemoryInputTransport(multiplexedFrame);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        TMessage msg = protocol.readMessageBegin();
        int separatorIndex = msg.name.indexOf(":");
        String serviceName = msg.name.substring(0, separatorIndex);
        String remainingMessage = msg.name.substring(serviceName.length() + ":".length());
        bh.consume(serviceName);
        bh.consume(separatorIndex);
        bh.consume(remainingMessage);
    }

    @Benchmark
    public void accumulo_routingCostOnly(Blackhole bh) throws TException {
        TMemoryInputTransport transport = new TMemoryInputTransport(accumuloFrame);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        protocol.readMessageBegin();
        byte serviceId = protocol.readByte();
        bh.consume(serviceId);
    }
}
