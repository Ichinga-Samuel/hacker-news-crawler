# Hacker News API Crawler: A Study in Python's Structured Concurrency

This project demonstrates and compares three different structured concurrency models in Python's `asyncio` library for concurrently crawling the Hacker News API. The goal is to fetch and store data about stories, comments, jobs, and users into a local SQLite database.

The three concurrency models explored are:

1.  **`asyncio.gather`**: For concurrently running a known set of awaitable objects.
2.  **`asyncio.TaskGroup`** (Python 3.11+): A modern, context-manager-based approach for managing a dynamic group of tasks.
3.  **Producer-Consumer Queue**: A custom implementation using `asyncio.Queue` to manage a large, dynamically generated workload with a fixed number of consumers (workers).

## Features ✨

  * **Multiple Concurrency Strategies**: Implements API crawling using `gather`, `TaskGroup`, and `Queue` for direct comparison.
  * **Two Crawling Modes**:
      * `traverse`: Starts from the latest story lists (top, new, best, etc.) and recursively fetches related items (comments, users, parents).
      * `walk_back`: Starts from the max item ID on Hacker News and works backward, fetching a specified number of items.
  * **Data Persistence**: Saves all fetched data into a structured SQLite database (`db.sqlite3`).
  * **Modular Design**: The code is separated into logical modules for the API client, database models, database interaction, and each concurrency strategy.

## Project Structure 📁

The project is organized into several files, each with a specific responsibility:

| File              | Description                                                                                                                                                |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `api.py`          | A simple, asynchronous client for interacting with the official Hacker News Firebase API.                                                                  |
| `db.py`           | A singleton class to manage the connection to the SQLite database.                                                                                         |
| `models.py`       | Contains `dataclass` definitions for different item types (Story, Comment, Job, User, etc.) and methods for creating tables.                               |
| `save_to_db.py`   | Handles the logic for saving parsed API data into the correct database tables based on item type.                                                          |
| `async_gather.py` | Implements the crawler using `asyncio.gather`. It collects all tasks in a list and runs them. New tasks discovered during execution are added to the list. |
| `async_group.py`  | Implements the crawler using `asyncio.TaskGroup`. Tasks are spawned within the task group's context, ensuring all are awaited.                             |
| `async_queue.py`  | Implements the crawler using a producer-consumer pattern. A central queue holds tasks, and a pool of worker coroutines executes them.                      |
| `task_queue.py`   | A custom, reusable `TaskQueue` class built on top of `asyncio.PriorityQueue` that manages worker tasks, timeouts, and graceful shutdowns.                  |

## Getting Started 🚀

Follow these instructions to get the project set up and running on your local machine.

### Prerequisites

  * Python 3.11 or newer (required for `asyncio.TaskGroup`).
  * A stable internet connection to access the Hacker News API.

### Installation & Setup

1.  **Clone the repository** (or download and extract the files into a single directory).

2.  **Navigate to the project directory**:

    ```bash
    cd path/to/your/project
    ```

3.  It's highly recommended to use a **virtual environment**:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

4.  No external libraries are needed\! The project only uses Python's standard libraries.

## Usage 🧑‍💻

You can run any of the three crawlers (`async_gather.py`, `async_group.py`, or `async_queue.py`). Each script can be run in either `traverse` or `walk_back` mode.

To run a specific implementation, you can modify the `main` function within the desired file to set the mode, or simply run it from the command line.

**Example Commands:**

  * **To run the `asyncio.gather` implementation (default `traverse` mode):**

    ```bash
    python async_gather.py
    ```

  * **To run the `asyncio.TaskGroup` implementation in `walk_back` mode:**

      * First, open `async_group.py` and change the `main` function call at the bottom:
        ```python
        if __name__ == "__main__":
            basicConfig(level="INFO")
            # Change mode here
            asyncio.run(main(mode="walk_back"))
        ```
      * Then, run the script:
        ```bash
        python async_group.py
        ```

  * **To run the `asyncio.Queue` implementation:**

    ```bash
    python async_queue.py
    ```

After a run is completed or stopped, the script will print a summary of the total items saved in each database table.

## Concurrency Models Explained 🤓

This project serves as a practical guide to understanding different ways of handling concurrent I/O-bound tasks in `asyncio`.

-----

### 1\. `asyncio.gather` (`async_gather.py`)

`asyncio.gather` is used to run a *known sequence* of awaitable objects concurrently.

  * **How it works**: It takes one or more awaitables (like coroutines or tasks) and returns an aggregate list of their results once all have completed.
  * **Our Implementation**: We start by creating tasks for an initial set of stories. As new items (like comments or users) are discovered, new tasks are created and appended to a central `self.tasks` list. This is a simple approach but can become difficult to manage, especially with error handling and task cancellation. It can also lead to very high memory usage if the number of tasks grows uncontrollably.

-----

### 2\. `asyncio.TaskGroup` (`async_group.py`)

Introduced in Python 3.11, `asyncio.TaskGroup` provides a more robust and modern API for structured concurrency.

  * **How it works**: It's a context manager (`async with`) that waits for all tasks created within its block to finish before proceeding. If any task raises an unhandled exception, all other tasks in the group are automatically cancelled.
  * **Our Implementation**: Tasks are created using `task_group.create_task()`. The structure is much cleaner than manually managing a list of tasks. The lifecycle of the tasks is tied directly to the `async with` block, preventing "leaked" tasks that run in the background forgotten. This is now the recommended approach for managing a dynamic group of related tasks.

-----

### 3\. Producer-Consumer with `asyncio.Queue` (`async_queue.py` & `task_queue.py`)

This pattern decouples the work production from the work consumption. It's excellent for situations where the total amount of work is unknown upfront and can grow dynamically.

  * **How it works**:
      * **Producers**: The `get_item` and `get_user` methods act as producers. When they discover a new item ID to fetch, they don't create a task immediately. Instead, they put a `QueueItem` onto a central `asyncio.Queue`.
      * **Consumers**: A fixed number of "worker" coroutines run in the background. Their only job is to pull items from the queue and execute them.
  * **Our Implementation**: The `TaskQueue` class manages the queue and the lifecycle of the worker tasks. This approach gives us fine-grained control over the level of concurrency (by limiting the number of workers), which prevents us from sending an overwhelming number of requests at once. It's the most complex of the three but also the most scalable and resilient for very large, unpredictable workloads.

Happy coding\! If you have any questions, feel free to ask.