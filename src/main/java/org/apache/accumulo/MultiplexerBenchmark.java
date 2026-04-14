/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.accumulo;

import org.apache.accumulo.core.rpc.AccumuloTMultiplexedProcessor;
import org.apache.accumulo.core.rpc.RpcService;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TMemoryBuffer;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 8, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 5, warmups = 2)
//@Fork(value = 2, jvmArgsAppend = {
      //  "-XX:+UnlockDiagnosticVMOptions",
        //"-XX:+PrintInlining", // confirm routing is not inlined away
    //    "-XX:+PrintCompilation", // shows when methods get compiled/deoptimized
  //      "-XX:-TieredCompilation"  // forces C2 only, no tier confusion in results
//})
public class MultiplexerBenchmark {

    @Param({"CLIENT", "TABLET_INGEST", "TABLET_MANAGEMENT"})
    public String rpcServiceName;

    private byte[] multiplexedFrame;
    private byte[] accumuloFrame;

    private TMultiplexedProcessor standardMultiplexer;
    private AccumuloTMultiplexedProcessor accumuloMultiplexer;

    private LongAdder[] callCounters;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        callCounters = new LongAdder[RpcService.values().length];
        for (int i = 0; i < callCounters.length; i++) {
            callCounters[i] = new LongAdder();
        }

        setupStandardMultiplexer();
        setupAccumuloMultiplexer();

        RpcService service = RpcService.valueOf(rpcServiceName);
        multiplexedFrame = buildMultiplexedFrame(service);
        accumuloFrame = buildAccumuloFrame(service);
    }

    @TearDown(Level.Trial)
    public void verify() {
        long total = 0;
        StringBuilder report = new StringBuilder("Call counts per RpcService:\n");
        for (RpcService service: RpcService.values()) {
            long count = callCounters[service.getShortId()].sum();
            total += count;
            report.append(String.format(" %-20s : %,d%n", service.name(), count));
        }
        System.out.println(report);

        if (total == 0) {
            throw new IllegalStateException("No calls were counted. JIT eliminated the routing entirely" + report);
        }
    }


    private void setupStandardMultiplexer() {
        standardMultiplexer = new TMultiplexedProcessor();
        // Standard Multiplexer routes on "ServiceName:methodName"
        for (RpcService service : RpcService.values()) {
            final LongAdder counter = callCounters[service.getShortId()];
            standardMultiplexer.registerProcessor(service.name(), (in, out) -> {
                counter.increment(); // observable side effect - prevents dead code elimination
                in.readMessageEnd();

            });
        }
    }

    private void setupAccumuloMultiplexer() {
        accumuloMultiplexer = new AccumuloTMultiplexedProcessor();
        for (RpcService service : RpcService.values()) {
            final LongAdder counter = callCounters[service.getShortId()];
            accumuloMultiplexer.registerProcessor(service, (in, out) -> {
                counter.increment();
                in.readMessageEnd();
            });
        }
    }

    @Benchmark
    public void standard_TMultiplexedProcessor(Blackhole bh) throws Exception {
        TMemoryInputTransport transport = new TMemoryInputTransport(multiplexedFrame);
        TProtocol protocol = new TCompactProtocol(transport);
        standardMultiplexer.process(protocol, protocol);
        bh.consume(callCounters[RpcService.valueOf(rpcServiceName).getShortId()].sum());
    }

    @Benchmark
    public void accumulo_byteIndexed_processor(Blackhole bh) throws Exception {
        TMemoryInputTransport transport = new TMemoryInputTransport(accumuloFrame);
        TProtocol protocol = new TCompactProtocol(transport);
        accumuloMultiplexer.process(protocol, protocol);
        bh.consume(callCounters[RpcService.valueOf(rpcServiceName).getShortId()].sum());
    }

    static byte[] buildMultiplexedFrame(RpcService service) throws TException {
        String methodName = service.name() + ":" + "compact";
        TMemoryBuffer buffer = new TMemoryBuffer(256);
        TProtocol protocol = new TCompactProtocol(buffer);
        protocol.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, 1));
        protocol.writeStructBegin(new TStruct("args"));
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        protocol.writeMessageEnd();
        protocol.getTransport().flush();
        byte[] bytes = new byte[buffer.length()];
        System.arraycopy(buffer.getArray(), 0, bytes, 0, buffer.length());
        return bytes;
    }

    static byte[] buildAccumuloFrame(RpcService service) throws TException {
        TMemoryBuffer buffer = new TMemoryBuffer(256);
        TProtocol protocol = new TCompactProtocol(buffer);
        protocol.writeMessageBegin(new TMessage("compact", TMessageType.CALL, 1));
        protocol.writeByte(service.getShortId());
        protocol.writeStructBegin(new TStruct("args"));
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        protocol.writeMessageEnd();
        protocol.getTransport().flush();
        byte[] bytes = new byte[buffer.length()];
        System.arraycopy(buffer.getArray(), 0, bytes, 0, buffer.length());
        return bytes;
    }
}