import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(
  executor: IExecutor,
  queue: AsyncIterable<ITask>,
  maxThreads = 0,
) {
  maxThreads = Math.max(0, maxThreads);

  const running = new Map<ITask['targetId'], Promise<void>>();
  const pending = new Set<ITask>();

  while (true) {
    if (maxThreads && running.size == maxThreads) {
      await Promise.race(running.values());

      continue;
    }

    const queueIterator = queue[Symbol.asyncIterator]();
    const { done, value: task } = await queueIterator.next();

    if (done) {
      if (!running.size && !pending.size) break;

      await Promise.race(running.values());

      continue;
    }

    if (running.has(task.targetId)) {
      pending.add(task);

      continue;
    }

    const runningTask = executor.executeTask(task).then(onComplete(task));

    running.set(task.targetId, runningTask);
  }

  function onComplete(completedTask: ITask) {
    return () => {
      running.delete(completedTask.targetId);

      if (!pending.size) return;

      for (const task of pending) {
        if (maxThreads && running.size == maxThreads) return;
        if (running.has(task.targetId)) continue;

        const runningTask = executor.executeTask(task).then(onComplete(task));

        running.set(task.targetId, runningTask);
        pending.delete(task);
      }
    };
  }
}
