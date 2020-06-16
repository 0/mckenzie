from .task import TaskReason


descriptions = {
    TaskReason.tr_task_add: 'Minted by "task add".',
    TaskReason.tr_task_cancel: 'Cancelled by "task cancel".',
    TaskReason.tr_task_uncancel: 'Uncancelled by "task uncancel".',
    TaskReason.tr_task_hold: 'Held by "task hold".',
    TaskReason.tr_task_release: 'Released by "task release".',
    TaskReason.tr_ready: 'No unsatisfied dependencies.',
    TaskReason.tr_waiting_dep: 'New unsatisfied dependencies.',
    TaskReason.tr_running: 'Picked up by worker.',
    TaskReason.tr_failure_exit_code: 'Exited with non-zero status.',
    TaskReason.tr_failure_string: 'Ended with invalid string.',
    TaskReason.tr_failure_abort: 'Worker exited before task finished.',
    TaskReason.tr_failure_worker_clean: 'Found by "worker clean".',
    TaskReason.tr_failure_run: 'Failed to run task.',
    TaskReason.tr_failure_memory: 'Exceeded memory limit.',
    TaskReason.tr_failure_scrapped: 'Scrapped by "worker scrap-task".',
    TaskReason.tr_limit_retry: 'Automatically reset with extended limit.',
    TaskReason.tr_scrapped_reset: 'Automatically reset after scrapping.',
    TaskReason.tr_scrapped_moved: 'Automatically moved after scrapping.',
    TaskReason.tr_task_reset_failed: 'Reset by "task reset-failed".',
    TaskReason.tr_success: 'Successfully finished running.',
    TaskReason.tr_task_synthesize: 'Synthesized by "task synthesize".',
    TaskReason.tr_task_cleanablize: 'Marked as cleanable by "task cleanablize".',
    TaskReason.tr_task_uncleanablize: 'Unmarked as cleanable by "task uncleanablize".',
    TaskReason.tr_task_clean_cleaning: 'Prepared for cleaning by "task clean".',
    TaskReason.tr_task_clean_cleaned: 'Cleaned by "task clean".',
    TaskReason.tr_task_rerun_synthesize: 'Synthesized by "task rerun".',
    TaskReason.tr_task_rerun_cleanablize: 'Marked as cleanable by "task rerun".',
    TaskReason.tr_task_rerun_cleaning: 'Prepared for cleaning by "task rerun".',
    TaskReason.tr_task_rerun_cleaned: 'Cleaned by "task rerun".',
    TaskReason.tr_task_rerun_reset: 'Reset by "task rerun".',
    }

for reason in sorted(TaskReason):
    tx.execute('''
        INSERT INTO task_reason (id, name, description)
        VALUES (%s, %s, %s);
        ''', (reason.value, reason.name, descriptions[reason]))
