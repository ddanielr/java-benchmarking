package org.apache.accumulo;

import static org.apache.accumulo.server.rpc.ThriftProcessorTypes.CLIENT;
import static org.apache.accumulo.server.rpc.ThriftProcessorTypes.TABLET_INGEST;
import static org.apache.accumulo.server.rpc.ThriftProcessorTypes.TABLET_MGMT;

import org.apache.accumulo.core.rpc.RpcService;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


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
    private final Map<String, Consumer<String>> SERVICE_PROCESSOR_MAP = new HashMap<>();
    private final TProcessor[] PROCESSORS = new TProcessor[256];

    @Setup(Level.Trial)
    public void setup() throws Exception {
        RpcService service = RpcService.valueOf(rpcServiceName);
        multiplexedFrame = MultiplexerBenchmark.buildMultiplexedFrame(service);
        accumuloFrame = MultiplexerBenchmark.buildAccumuloFrame(service);
        SERVICE_PROCESSOR_MAP.put(rpcServiceName, System.out::println);
        PROCESSORS[Byte.toUnsignedInt(CLIENT.getService().getShortId())] = (in, out) -> {int x =1;};
        PROCESSORS[Byte.toUnsignedInt(TABLET_INGEST.getService().getShortId())] = (in, out) -> {int y =2;};
        PROCESSORS[Byte.toUnsignedInt(TABLET_MGMT.getService().getShortId())] = (in, out) -> {int z =3;};
    }

    @Benchmark
    public void standard_routingCostOnly(Blackhole bh) throws TException {
        TMemoryInputTransport transport = new TMemoryInputTransport(multiplexedFrame);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        TMessage msg = protocol.readMessageBegin();
        int separatorIndex = msg.name.indexOf(":");
        String serviceName = msg.name.substring(0, separatorIndex);
        Consumer<String> consumer = SERVICE_PROCESSOR_MAP.get(serviceName);
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
        int serviceId = Byte.toUnsignedInt(protocol.readByte());
        TProcessor processor = PROCESSORS[serviceId];
        bh.consume(serviceId);
    }
}
