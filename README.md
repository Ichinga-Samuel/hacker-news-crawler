# Data Scraping with Structural Concurrency  🚀

A Python-based asynchronous data crawler utilizing `asyncio.Queue` to efficiently collect, manage, and prioritize
tasks while exploring data from the Hacker News API. This project demonstrates how to use `asyncio` to handle
concurrent API requests and manage task flow through different types of queues, including `PriorityQueue`,
FIFO, and LIFO queues.

## Features

- **Asynchronous Task Management:** Uses `asyncio.Queue` for efficient, non-blocking task scheduling.
- **Priority Task Handling:** Ensures that critical tasks (such as saving data) are completed through the use of `asyncio.PriorityQueue`.
- **Concurrent Data Collection:** Leverages multiple asynchronous workers to concurrently crawl and collect data from Hacker News endpoints.
- **Flexible Queue Types:** Supports multiple queue types (`PriorityQueue`, FIFO, LIFO) to demonstrate flexibility in task management.

## Installation and Usage

This project does not have any external dependencies or specific requirements. You can run it with the default Python environment. Simply clone the repository and start exploring!

### Clone the repository:

```bash
git clone https://github.com/Ichinga-Samuel/hacker-news-crawler.git
cd hacker-news-crawler
```

### Usage

There are no specific command line options required to run this project. Simply execute the main script:

```bash
python async_queue.py
```

## First Part: Asyncio with gather

In the initial version of this project, we used `asyncio.gather` to collect and process data concurrently from various
Hacker News endpoints. This approach efficiently handled multiple API requests simultaneously, enabling fast data
retrieval across the `/beststories`, `/topstories`, `/newstories`, `/askstories`, `/showstories`, and `/jobstories` endpoints.

This initial implementation offered a simple and effective way to run tasks concurrently, but lacked the flexibility
and control needed to handle task prioritization or manage large-scale asynchronous workflows efficiently.

## Current Approach: Asyncio Queues
In the current iteration, we replaced `asyncio.gather` with `asyncio.Queue` to better manage task flow.
This version introduces task prioritization using `asyncio.PriorityQueue` while allowing the use of other queue types
like FIFO and LIFO. We implemented a task queue system (`TaskQueue`) that allows tasks to be scheduled and processed
asynchronously with built-in priority handling, graceful shutdowns, and timeout control.

### `AsyncQueue` Class

This class orchestrates the data collection and task management. It leverages `TaskQueue` to handle concurrent task
execution and prioritization.

- **`get_user`**: Fetches and processes user data, including their submitted items.
- **`get_item`**: Retrieves stories, comments, and jobs, while handling related data like the author, parent items, and child items (kids).
- **`traverse_api`**: Collects a broad range of stories (e.g., top, new, best) from various Hacker News endpoints and adds them to the task queue for processing.
- **`walk_back`**: Starts from the largest item ID in the Hacker News API and walks back through a specified number of items.

### TaskQueue Class

Manages task queues and worker coroutines. It supports prioritization, timeout handling, and graceful shutdown upon receiving SIGINT signals.

### QueueItem Class

A wrapper for tasks added to the queue, encapsulating the coroutine to be executed and its arguments.
It ensures tasks are hashable and sortable, which is critical for prioritization in `asyncio.PriorityQueue`.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to the [Hacker News API](https://github.com/HackerNews/API) for providing the data source for this project.
- Inspired by the potential of asynchronous programming in Python for scalable, high-performance data processing.

---
