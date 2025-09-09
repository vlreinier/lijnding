import asyncio
import time
from lijnding import Pipeline, stage, from_iterable

# --- Define Stages ---

@stage(backend="process", workers=2)
def process_stage_1(x):
    # Simulate CPU work
    for i in range(1000):
        _ = i * i
    return x * 2

@stage(backend="process", workers=2)
def process_stage_2(x):
    # Simulate CPU work
    for i in range(500):
        _ = i * i
    return x + 1

@stage
def sync_stage(x):
    return x - 10

async def main():
    """
    Runs multiple pipelines, including several with the 'process' backend,
    to validate that the framework does not hang in a regular Python script.
    """
    print("--- Starting validation script ---")

    # --- Test Case 1: A simple process pipeline ---
    print("\n[RUNNING] Test Case 1: Simple process pipeline")
    start_time = time.time()
    p1 = from_iterable(range(5)) | process_stage_1
    results, _ = await p1.run()
    results_list = [item async for item in results]
    duration = time.time() - start_time
    print(f"[PASSED] Test Case 1 finished in {duration:.2f}s. Results: {results_list}")
    assert sorted(results_list) == [0, 2, 4, 6, 8]

    # --- Test Case 2: A second process pipeline ---
    print("\n[RUNNING] Test Case 2: Another process pipeline")
    start_time = time.time()
    p2 = from_iterable(range(3)) | process_stage_2
    results, _ = await p2.run()
    results_list = [item async for item in results]
    duration = time.time() - start_time
    print(f"[PASSED] Test Case 2 finished in {duration:.2f}s. Results: {results_list}")
    assert sorted(results_list) == [1, 2, 3]

    # --- Test Case 3: A mixed pipeline ---
    print("\n[RUNNING] Test Case 3: Mixed pipeline (process -> serial)")
    start_time = time.time()
    p3 = from_iterable(range(4)) | process_stage_1 | sync_stage
    results, _ = await p3.run()
    results_list = [item async for item in results]
    duration = time.time() - start_time
    print(f"[PASSED] Test Case 3 finished in {duration:.2f}s. Results: {results_list}")
    assert sorted(results_list) == [-10, -8, -6, -4]

    print("\n--- Validation script finished successfully! ---")


if __name__ == "__main__":
    # This is required for multiprocessing to work correctly on all platforms
    import multiprocessing
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    asyncio.run(main())
