from .worker import WorkerReason


descriptions = {
    WorkerReason.wr_worker_spawn: 'Minted by "worker spawn".',
    WorkerReason.wr_worker_quit_cancelled: 'Cancelled by "worker quit".',
    WorkerReason.wr_start: 'Started running.',
    WorkerReason.wr_quit: 'Started quitting.',
    WorkerReason.wr_unquit: 'Resumed running.',
    WorkerReason.wr_success: 'Exited normally.',
    WorkerReason.wr_success_idle: 'Ran out of tasks.',
    WorkerReason.wr_success_abort: 'Aborted.',
    WorkerReason.wr_failure: 'Exited unexpectedly.',
    WorkerReason.wr_worker_ack_failed: 'Acknowledged by "worker ack-failed".',
    WorkerReason.wr_worker_clean_queued: 'Found "queued" by "worker clean".',
    WorkerReason.wr_worker_clean_running: 'Found in a running state by "worker clean".',
    }

for reason in sorted(WorkerReason):
    tx.execute('''
        INSERT INTO worker_reason (id, name, description)
        VALUES (%s, %s, %s);
        ''', (reason.value, reason.name, descriptions[reason]))
