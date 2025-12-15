"""Application runtime - shared thread pool for async operations."""

from concurrent.futures import ThreadPoolExecutor

_executor: ThreadPoolExecutor | None = None


def get_executor() -> ThreadPoolExecutor:
    """Get the shared thread pool executor."""
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="kafka-sim")
    return _executor


def shutdown_executor() -> None:
    """Shutdown the thread pool (call on app exit)."""
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=True)
        _executor = None
