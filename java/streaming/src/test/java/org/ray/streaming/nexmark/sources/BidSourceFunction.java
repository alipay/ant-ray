/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ray.streaming.nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.ray.streaming.api.function.impl.SourceFunction;
import java.util.Random;

public class BidSourceFunction implements SourceFunction<Bid> {

  private volatile boolean running = true;
  private final GeneratorConfig config =
      new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
  private long eventsCountSoFar = 0;
  private final int rate;

  public BidSourceFunction(int srcRate) {
    this.rate = srcRate;
  }

  private long nextId() {
    return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
  }

  @Override
  public void init(int parallel, int index) {}

  @Override
  public void fetch(long batchId, SourceContext<Bid> ctx) throws Exception {
    while (running && eventsCountSoFar < 20_000_000) {
      long emitStartTime = System.currentTimeMillis();

      for (int i = 0; i < rate; i++) {

        long nextId = nextId();
        Random rnd = new Random(nextId);

        // When, in event time, we should generate the event. Monotonic.
        long eventTimestamp =
            config
                .timestampAndInterEventDelayUsForEvent(config.nextEventNumber(eventsCountSoFar))
                .getKey();

        ctx.collect(BidGenerator.nextBid(nextId, rnd, eventTimestamp, config));
        eventsCountSoFar++;
      }

      // Sleep for the rest of timeslice if needed
      long emitTime = System.currentTimeMillis() - emitStartTime;
      if (emitTime < 1000) {
        Thread.sleep(1000 - emitTime);
      }
    }
  }

  @Override
  public void close() {}
}
