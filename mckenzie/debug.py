import logging

from .arguments import argparsable, description
from .base import Manager
from .database import IsolationLevel
from .task import TaskReason, TaskState
from .worker import WorkerReason, WorkerState


logger = logging.getLogger(__name__)


@argparsable('debugging')
class DebugManager(Manager, name='debug'):
    def summary(self, args):
        logger.info('No action specified.')

    @description('verify denormalized database values')
    def verify_denormalization(self, args):
        @self.db.tx
        def F(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            TaskReason.validate('task_reason', tx)

        @self.db.tx
        def F(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            TaskState.validate('task_state', tx)

        @self.db.tx
        def F(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            WorkerReason.validate('worker_reason', tx)

        @self.db.tx
        def F(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            WorkerState.validate('worker_state', tx)

        @self.db.tx
        def invalid_claims(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            return tx.execute('''
                    SELECT name
                    FROM task
                    WHERE (claimed_by IS NULL) != (claimed_since IS NULL)
                    ''')

        for name, in invalid_claims:
            print(f'Task "{name}" has invalid claim.')

        @self.db.tx
        def task_dependencies(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            return tx.execute('''
                    SELECT t.name, t.num_dependencies, COUNT(td.dependency_id)
                    FROM task t
                    LEFT JOIN task_dependency td ON td.task_id = t.id
                    GROUP BY t.name, t.num_dependencies
                    HAVING t.num_dependencies != COUNT(td.dependency_id)
                    ''')

        for name, expected, actual in task_dependencies:
            if actual == 1:
                print(f'Task "{name}" has 1 dependency, not {expected}.')
            else:
                print(f'Task "{name}" has {actual} dependencies, not '
                      f'{expected}.')

        @self.db.tx
        def task_dependencies_satisfied(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            # There's no need for a LEFT JOIN to include tasks that have no
            # dependencies, thanks to the dependencies_bound constraint and the
            # preceding check.
            return tx.execute('''
                    SELECT t.name, t.expected, t.actual
                    FROM (
                        SELECT t1.name,
                               t1.num_dependencies_satisfied AS expected,
                               COUNT(*) FILTER
                                   (WHERE ts2.satisfies_dependency
                                    OR (td.soft
                                        AND ts2.satisfies_soft_dependency))
                                   AS actual
                        FROM task t1
                        JOIN task_dependency td ON td.task_id = t1.id
                        JOIN task t2 ON t2.id = td.dependency_id
                        JOIN task_state ts2 ON ts2.id = t2.state_id
                        GROUP BY t1.name, t1.num_dependencies_satisfied
                    ) t
                    WHERE t.expected != t.actual
                    ''')

        for name, expected, actual in task_dependencies_satisfied:
            if actual == 1:
                print(f'Task "{name}" has 1 satisfied dependency, not '
                      f'{expected}.')
            else:
                print(f'Task "{name}" has {actual} satisfied dependencies, '
                      f'not {expected}.')

        @self.db.tx
        def worker_tasks(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            return tx.execute('''
                    SELECT w.id, w.num_tasks, COUNT(wt.task_id)
                    FROM worker w
                    LEFT JOIN worker_task wt ON wt.worker_id = w.id
                    GROUP BY w.id, w.num_tasks
                    HAVING w.num_tasks != COUNT(wt.task_id)
                    ''')

        for slurm_job_id, expected, actual in worker_tasks:
            if actual == 1:
                print(f'Worker {slurm_job_id} has 1 task, not {expected}.')
            else:
                print(f'Worker {slurm_job_id} has {actual} tasks, not '
                      f'{expected}.')

        @self.db.tx
        def worker_tasks_active(tx):
            tx.isolate(IsolationLevel.SERIALIZABLE, deferrable=True)

            # There's no need for a LEFT JOIN to include workers that have no
            # tasks, thanks to the tasks_bound constraint and the preceding
            # check.
            return tx.execute('''
                    SELECT w.id, w.expected, w.actual
                    FROM (
                        SELECT w.id, w.num_tasks_active AS expected,
                               COUNT(*) FILTER (WHERE wt.active) AS actual
                        FROM worker w
                        JOIN worker_task wt ON wt.worker_id = w.id
                        GROUP BY w.id, w.num_tasks_active
                    ) w
                    WHERE w.expected != w.actual
                    ''')

        for slurm_job_id, expected, actual in worker_tasks_active:
            if actual == 1:
                print(f'Worker {slurm_job_id} has 1 active task, not '
                      f'{expected}.')
            else:
                print(f'Worker {slurm_job_id} has {actual} active tasks, not '
                      f'{expected}.')
