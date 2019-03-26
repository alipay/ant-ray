package com.ray.streaming.schedule.impl;

import com.ray.streaming.api.partition.impl.BroadcastPartition;
import com.ray.streaming.cluster.ResourceManager;
import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.graph.ExecutionNode;
import com.ray.streaming.core.graph.ExecutionNode.NodeType;
import com.ray.streaming.core.graph.ExecutionTask;
import com.ray.streaming.core.runtime.StreamWorker;
import com.ray.streaming.core.runtime.context.WorkerContext;
import com.ray.streaming.operator.impl.MasterOperator;
import com.ray.streaming.plan.Plan;
import com.ray.streaming.plan.PlanEdge;
import com.ray.streaming.plan.PlanVertex;
import com.ray.streaming.plan.VertexType;
import com.ray.streaming.schedule.IJobSchedule;
import com.ray.streaming.schedule.ITaskAssign;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;


public class JobScheduleImpl implements IJobSchedule {

  private Plan plan;
  private ResourceManager resourceManager;
  private ITaskAssign taskAssign;

  public JobScheduleImpl(Plan plan) {
    this.plan = plan;
    this.resourceManager = new ResourceManager();
    this.taskAssign = new TaskAssignImpl();
  }

  /**
   * schedule physical plan to execution graph.
   * and call streaming worker to init and run.
   */
  @Override
  public void schedule() {
    addJobMaster(plan);
    List<RayActor<StreamWorker>> workers = this.resourceManager.createWorker(getPlanWorker());
    ExecutionGraph executionGraph = this.taskAssign.assign(this.plan, workers);

    List<ExecutionNode> executionNodes = executionGraph.getExecutionNodeList();
    List<RayObject<Boolean>> waits = new ArrayList<>();
    ExecutionTask masterTask = null;
    for (ExecutionNode executionNode : executionNodes) {
      List<ExecutionTask> executionTasks = executionNode.getExecutionTaskList();
      for (ExecutionTask executionTask : executionTasks) {
        if (executionNode.getNodeType() != NodeType.MASTER) {
          Integer taskId = executionTask.getTaskId();
          RayActor<StreamWorker> streamWorker = executionTask.getWorker();
          waits.add(Ray.call(StreamWorker::init, streamWorker,
              new WorkerContext(taskId, executionGraph)));
        } else {
          masterTask = executionTask;
        }
      }
    }
    Ray.wait(waits);

    Integer masterId = masterTask.getTaskId();
    RayActor<StreamWorker> masterWorker = masterTask.getWorker();
    Ray.call(StreamWorker::init, masterWorker,
        new WorkerContext(masterId, executionGraph)).get();


  }

  private void addJobMaster(Plan plan) {
    int masterVertexId = 0;
    int masterParallelism = 1;
    PlanVertex masterVertex = new PlanVertex(masterVertexId, masterParallelism, VertexType.MASTER,
        new MasterOperator());
    plan.getPlanVertexList().add(masterVertex);
    List<PlanVertex> planVertices = plan.getPlanVertexList();
    for (PlanVertex planVertex : planVertices) {
      if (planVertex.getVertexType() == VertexType.SOURCE) {
        PlanEdge planEdge = new PlanEdge(masterVertexId, planVertex.getVertexId(),
            new BroadcastPartition());
        plan.getPlanEdgeList().add(planEdge);
      }
    }
  }

  private int getPlanWorker() {
    List<PlanVertex> planVertexList = plan.getPlanVertexList();
    return planVertexList.stream().map(vertex -> vertex.getParallelism()).reduce(0, Integer::sum);
  }
}
