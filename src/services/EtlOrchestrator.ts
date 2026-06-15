/**
 * Grain Silo Processing Coordinator
 * Agriculture Siphon Smart Sensors Orchestrator
 *
 * Distributed systems concepts covered:
 * - Task Graphs (DAGs)
 * - MapReduce-style batch processing
 * - Sharded validation
 * - Saga orchestration with compensating transactions
 * - Automatic rollback on cloud/storage failure
 * - Latency tracking and bottleneck detection
 *
 * Run with:
 *   npm install
 *   npm run start
 */

type StepStatus = 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'ROLLED_BACK' | 'SKIPPED';
type StepName = 'extract' | 'transform' | 'load' | 'aggregate';

type SensorRecord = {
  sensorId: string;
  siloId: string;
  crop: 'wheat' | 'corn' | 'barley';
  moisture: number;
  temperature: number;
  timestamp: number;
};

type TransformedRecord = SensorRecord & {
  shardId: string;
  riskLevel: 'LOW' | 'MEDIUM' | 'HIGH';
  recommendedAction: 'STORE' | 'VENTILATE' | 'DRY' | 'REJECT';
};

type AggregateResult = {
  siloId: string;
  count: number;
  averageMoisture: number;
  highRiskCount: number;
};

type PipelineContext = {
  runId: string;
  raw: SensorRecord[];
  transformed: TransformedRecord[];
  loadedKeys: string[];
  aggregateKeys: string[];
  statusFeed: Record<StepName, StepStatus>;
  latenciesMs: Record<StepName, number>;
  logs: string[];
};

type PipelineStep = {
  name: StepName;
  dependsOn: StepName[];
  execute: (ctx: PipelineContext) => Promise<void>;
  compensate: (ctx: PipelineContext) => Promise<void>;
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

class SimulatedCloudStorage<T> {
  private data = new Map<string, T>();

  constructor(private readonly name: string, private failOnWrite = false) {}

  async write(key: string, value: T): Promise<void> {
    await sleep(30 + Math.floor(Math.random() * 60));
    if (this.failOnWrite) {
      throw new Error(`${this.name} write failed for key=${key}`);
    }
    this.data.set(key, value);
  }

  async delete(key: string): Promise<void> {
    await sleep(10);
    this.data.delete(key);
  }

  entries(): [string, T][] {
    return [...this.data.entries()];
  }
}

class AgricultureSensorCluster {
  async readBatch(): Promise<SensorRecord[]> {
    await sleep(80);
    return [
      { sensorId: 'S-1', siloId: 'SILO-A', crop: 'wheat', moisture: 11.8, temperature: 24, timestamp: Date.now() },
      { sensorId: 'S-2', siloId: 'SILO-A', crop: 'wheat', moisture: 15.4, temperature: 29, timestamp: Date.now() },
      { sensorId: 'S-3', siloId: 'SILO-B', crop: 'corn', moisture: 18.9, temperature: 32, timestamp: Date.now() },
      { sensorId: 'S-4', siloId: 'SILO-C', crop: 'barley', moisture: 9.6, temperature: 21, timestamp: Date.now() }
    ];
  }
}

class EtlOrchestrator {
  private readonly rawStorage = new SimulatedCloudStorage<SensorRecord>('raw-storage');
  private readonly processedStorage = new SimulatedCloudStorage<TransformedRecord>('processed-storage');
  private readonly aggregateStorage: SimulatedCloudStorage<AggregateResult>;
  private readonly sensorCluster = new AgricultureSensorCluster();
  private readonly steps: PipelineStep[];

  constructor(options?: { simulateAggregateFailure?: boolean }) {
    this.aggregateStorage = new SimulatedCloudStorage<AggregateResult>(
      'aggregate-storage',
      options?.simulateAggregateFailure ?? false
    );

    this.steps = [
      {
        name: 'extract',
        dependsOn: [],
        execute: async ctx => {
          ctx.raw = await this.sensorCluster.readBatch();
          for (const record of ctx.raw) {
            const key = `${ctx.runId}:raw:${record.sensorId}`;
            await this.rawStorage.write(key, record);
          }
        },
        compensate: async ctx => {
          for (const record of ctx.raw) {
            await this.rawStorage.delete(`${ctx.runId}:raw:${record.sensorId}`);
          }
        }
      },
      {
        name: 'transform',
        dependsOn: ['extract'],
        execute: async ctx => {
          ctx.transformed = ctx.raw.map(record => this.validateAndTransform(record));
        },
        compensate: async ctx => {
          ctx.transformed = [];
        }
      },
      {
        name: 'load',
        dependsOn: ['transform'],
        execute: async ctx => {
          for (const record of ctx.transformed) {
            const key = `${ctx.runId}:processed:${record.sensorId}`;
            await this.processedStorage.write(key, record);
            ctx.loadedKeys.push(key);
          }
        },
        compensate: async ctx => {
          for (const key of ctx.loadedKeys.reverse()) {
            await this.processedStorage.delete(key);
          }
          ctx.loadedKeys = [];
        }
      },
      {
        name: 'aggregate',
        dependsOn: ['load'],
        execute: async ctx => {
          const aggregates = this.mapReduceAggregate(ctx.transformed);
          for (const aggregate of aggregates) {
            const key = `${ctx.runId}:aggregate:${aggregate.siloId}`;
            await this.aggregateStorage.write(key, aggregate);
            ctx.aggregateKeys.push(key);
          }
        },
        compensate: async ctx => {
          for (const key of ctx.aggregateKeys.reverse()) {
            await this.aggregateStorage.delete(key);
          }
          ctx.aggregateKeys = [];
        }
      }
    ];
  }

  async run(): Promise<PipelineContext> {
    const ctx: PipelineContext = {
      runId: `RUN-${Date.now()}`,
      raw: [],
      transformed: [],
      loadedKeys: [],
      aggregateKeys: [],
      statusFeed: { extract: 'PENDING', transform: 'PENDING', load: 'PENDING', aggregate: 'PENDING' },
      latenciesMs: { extract: 0, transform: 0, load: 0, aggregate: 0 },
      logs: []
    };

    const completed: PipelineStep[] = [];

    try {
      for (const step of this.steps) {
        this.assertDependenciesSucceeded(step, ctx);
        await this.executeStep(step, ctx);
        completed.push(step);
      }
      ctx.logs.push('PIPELINE_SUCCESS: all DAG states completed.');
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      ctx.logs.push(`PIPELINE_FAILED: ${reason}`);
      await this.rollback(completed, ctx);
    }

    ctx.logs.push(`BOTTLENECK: ${this.findBottleneck(ctx)}`);
    return ctx;
  }

  private async executeStep(step: PipelineStep, ctx: PipelineContext): Promise<void> {
    ctx.statusFeed[step.name] = 'RUNNING';
    ctx.logs.push(`START ${step.name}`);
    const start = performance.now();

    try {
      await step.execute(ctx);
      ctx.statusFeed[step.name] = 'SUCCESS';
      ctx.logs.push(`SUCCESS ${step.name}`);
    } catch (error) {
      ctx.statusFeed[step.name] = 'FAILED';
      throw error;
    } finally {
      ctx.latenciesMs[step.name] = Math.round(performance.now() - start);
    }
  }

  private async rollback(completed: PipelineStep[], ctx: PipelineContext): Promise<void> {
    ctx.logs.push('ROLLBACK_STARTED: executing Saga compensations in reverse order.');
    for (const step of completed.reverse()) {
      await step.compensate(ctx);
      ctx.statusFeed[step.name] = 'ROLLED_BACK';
      ctx.logs.push(`ROLLED_BACK ${step.name}`);
    }
    for (const step of this.steps) {
      if (ctx.statusFeed[step.name] === 'PENDING') ctx.statusFeed[step.name] = 'SKIPPED';
    }
    ctx.logs.push('ROLLBACK_DONE: persistent writes cleaned.');
  }

  private assertDependenciesSucceeded(step: PipelineStep, ctx: PipelineContext): void {
    for (const dependency of step.dependsOn) {
      if (ctx.statusFeed[dependency] !== 'SUCCESS') {
        throw new Error(`${step.name} cannot run because ${dependency} is not successful`);
      }
    }
  }

  private validateAndTransform(record: SensorRecord): TransformedRecord {
    if (record.moisture < 0 || record.moisture > 100) {
      throw new Error(`invalid moisture from ${record.sensorId}`);
    }
    if (record.temperature < -20 || record.temperature > 70) {
      throw new Error(`invalid temperature from ${record.sensorId}`);
    }

    const shardId = `shard-${record.siloId.slice(-1)}`;
    const riskLevel = record.moisture >= 18 || record.temperature >= 32 ? 'HIGH'
      : record.moisture >= 14 || record.temperature >= 28 ? 'MEDIUM'
      : 'LOW';
    const recommendedAction = riskLevel === 'HIGH' ? 'DRY'
      : riskLevel === 'MEDIUM' ? 'VENTILATE'
      : 'STORE';

    return { ...record, shardId, riskLevel, recommendedAction };
  }

  private mapReduceAggregate(records: TransformedRecord[]): AggregateResult[] {
    const grouped = records.reduce<Record<string, TransformedRecord[]>>((acc, record) => {
      acc[record.siloId] ??= [];
      acc[record.siloId].push(record);
      return acc;
    }, {});

    return Object.entries(grouped).map(([siloId, items]) => ({
      siloId,
      count: items.length,
      averageMoisture: Number((items.reduce((sum, item) => sum + item.moisture, 0) / items.length).toFixed(2)),
      highRiskCount: items.filter(item => item.riskLevel === 'HIGH').length
    }));
  }

  private findBottleneck(ctx: PipelineContext): string {
    return Object.entries(ctx.latenciesMs)
      .sort((a, b) => b[1] - a[1])
      .map(([step, latency]) => `${step}=${latency}ms`)[0];
  }
}

async function main() {
  console.log('\n=== Successful run ===');
  const ok = await new EtlOrchestrator().run();
  console.table(ok.statusFeed);
  console.table(ok.latenciesMs);
  console.log(ok.logs.join('\n'));

  console.log('\n=== Failure run with automatic rollback ===');
  const failed = await new EtlOrchestrator({ simulateAggregateFailure: true }).run();
  console.table(failed.statusFeed);
  console.table(failed.latenciesMs);
  console.log(failed.logs.join('\n'));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});

export { EtlOrchestrator, SimulatedCloudStorage, AgricultureSensorCluster };
