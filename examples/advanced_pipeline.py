"""
An advanced example demonstrating a more complex data processing pipeline
using LijnDing.

This pipeline simulates a workflow that:
1.  Generates a list of user IDs from a source stage.
2.  For each user ID, it branches out to perform several actions in parallel:
    - Fetches user data (simulated I/O-bound task).
    - Calculates a user-specific metric (simulated CPU-bound task).
    - Checks if the user ID is a "special" user.
3.  Uses different backends ('thread' and 'process') for appropriate tasks.
4.  Includes a stage that can fail to demonstrate error handling.
5.  Merges the results from the branches into a single data structure.
6.  Uses an aggregator to generate a final summary report.
"""
import time
import random
from typing import Dict, Any, Iterable, List

from lijnding.core import stage, aggregator_stage, Pipeline
from lijnding.core.errors import ErrorPolicy
from lijnding.components import branch

# --- Stage Definitions ---

@stage
def generate_user_ids() -> Iterable[int]:
    """A source stage that generates a list of user IDs to process."""
    print("--- Generating User IDs ---")
    yield from range(5)
    # This user ID will cause an error later
    yield 99
    yield from range(5, 10)

@stage(backend="thread", workers=4, name="fetch_user_data")
def fetch_user_data(user_id: int) -> Dict[str, Any]:
    """Simulates a slow, I/O-bound API call to get user data."""
    print(f"Fetching data for user {user_id}...")
    # Simulate a user that causes an error
    if user_id == 99:
        raise ConnectionError(f"Failed to fetch data for user {user_id}")

    time.sleep(random.uniform(0.01, 0.05))
    return {"user_id": user_id, "name": f"User {user_id}", "email": f"user{user_id}@example.com"}

@stage(backend="process", workers=2, name="calculate_metrics")
def calculate_metrics(data: Dict[str, Any]) -> float:
    """Simulates a CPU-bound task to calculate a score for a user."""
    print(f"Calculating metrics for {data['name']}...")
    # Simulate heavy computation
    _ = [i * i for i in range(10_000)]
    score = data["user_id"] * random.uniform(0.5, 1.5)
    return round(score, 2)

@stage
def check_if_special(data: Dict[str, Any]) -> bool:
    """A simple, fast stage to check a user property."""
    return data["user_id"] % 3 == 0

@aggregator_stage
def generate_report(results: Iterable[List[Any]]) -> Dict[str, Any]:
    """
    An aggregator stage that consumes all the branched results and creates
    a final summary report.
    """
    print("\n--- Generating Final Report ---")
    processed_count = 0
    total_score = 0
    special_users = []

    for item in results:
        # The item is a list: [user_data, score, is_special]
        user_data, score, is_special = item
        processed_count += 1
        total_score += score
        if is_special:
            special_users.append(user_data["name"])

    return {
        "users_processed": processed_count,
        "average_score": round(total_score / processed_count, 2) if processed_count else 0,
        "special_users": special_users,
    }

# --- Pipeline Definition ---

def create_advanced_pipeline() -> Pipeline:
    """Creates and configures the advanced processing pipeline."""

    # This branch processes each user ID in parallel
    processing_branch = branch(
        fetch_user_data,
        calculate_metrics,
        check_if_special,
        merge="zip"  # Zip results into a tuple: (user_data, score, is_special)
    )

    # Configure the 'fetch_user_data' stage within the branch to skip errors
    # Note: We access the stage via the branch's internal `branch_pipelines`
    fetch_stage = processing_branch.branch_pipelines[0].stages[0]
    fetch_stage.error_policy = ErrorPolicy(mode="skip")

    # The main pipeline
    main_pipeline = (
        generate_user_ids
        | processing_branch
        | generate_report
    )

    main_pipeline.name = "Advanced_ETL"
    return main_pipeline

# --- Execution ---

if __name__ == "__main__":
    print("Starting advanced pipeline run...")
    pipeline = create_advanced_pipeline()

    # You can uncomment the following line to see a visual representation
    # of the pipeline. Requires graphviz to be installed.
    # print(pipeline.visualize())

    start_time = time.time()
    # We use .collect() because the final stage is an aggregator
    final_report, context = pipeline.collect()
    end_time = time.time()

    print("\n--- Pipeline Run Complete ---")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    print("\n--- Final Report ---")
    import json
    print(json.dumps(final_report[0], indent=2))

    print("\n--- Context Metrics ---")
    print(f"Errors encountered in 'fetch_user_data': {context.get('errors:fetch_user_data', 0)}")
    print(f"Items processed by 'calculate_metrics': {context.get('items_in:calculate_metrics', 0)}")