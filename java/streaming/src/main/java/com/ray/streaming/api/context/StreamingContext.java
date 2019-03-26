package com.ray.streaming.api.context;

import com.ray.streaming.api.stream.StreamSink;
import com.ray.streaming.plan.Plan;
import com.ray.streaming.plan.PlanBuilder;
import com.ray.streaming.schedule.IJobSchedule;
import com.ray.streaming.schedule.impl.JobScheduleImpl;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.Ray;

/**
 * Encapsulate the context information of a streaming Job.
 */
public class StreamingContext implements Serializable {

  private transient AtomicInteger idGenerator;
  private List<StreamSink> streamSinks;
  /**
   * The logic plan.
   */
  private Plan plan;

  private StreamingContext() {
    this.idGenerator = new AtomicInteger(0);
    this.streamSinks = new ArrayList<>();
  }

  public static StreamingContext buildContext() {
    Ray.init();
    return new StreamingContext();
  }

  /**
   * construct streaming job dag && run streaming job
   */
  public void execute() {
    PlanBuilder planBuilder = new PlanBuilder(this.streamSinks);
    this.plan = planBuilder.buildPlan();
    plan.printPlan();

    IJobSchedule jobSchedule = new JobScheduleImpl();
    jobSchedule.schedule(plan);
  }

  public int generateId() {
    return this.idGenerator.incrementAndGet();
  }

  public void addSink(StreamSink streamSink) {
    streamSinks.add(streamSink);
  }
}
