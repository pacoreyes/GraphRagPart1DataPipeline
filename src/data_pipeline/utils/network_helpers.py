# -----------------------------------------------------------
# Network Request Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
from typing import Any, Callable, Coroutine, Optional, TypeVar, AsyncIterable

import httpx
from dagster import AssetExecutionContext
from tqdm.asyncio import tqdm_asyncio
from tqdm.asyncio import tqdm

T = TypeVar("T")
R = TypeVar("R")


async def make_async_request_with_retries(
    context: AssetExecutionContext,
    url: str,
    method: str = "POST",
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    max_retries: int = 5,
    initial_backoff: int = 2,
    timeout: int = 60,
    client: Optional[httpx.AsyncClient] = None,
) -> httpx.Response:
    """
    Executes an asynchronous HTTP request with an exponential backoff retry strategy.

    Args:
        context (AssetExecutionContext): Dagster execution context.
        url (str): The target URL.
        method (str): HTTP method.
        params (Optional[dict[str, Any]]): Query parameters or data.
        headers (Optional[dict[str, str]]): HTTP headers.
        max_retries (int): Maximum retries.
        initial_backoff (int): Initial backoff seconds.
        timeout (int): Request timeout seconds.
        client (Optional[httpx.AsyncClient]): Optional existing client to reuse.

    Returns:
        httpx.Response: The successful response.
    """
    should_close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        should_close_client = True

    try:
        for attempt in range(max_retries):
            try:
                request_args: dict[str, Any] = {"headers": headers}
                if method.upper() == "GET":
                    request_args["params"] = params
                elif method.upper() == "POST":
                    request_args["data"] = params
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response = await client.request(method, url, **request_args)
                response.raise_for_status()
                return response

            except httpx.HTTPError as error:
                wait_time = initial_backoff * (2**attempt)
                context.log.warning(
                    f"Async Attempt {attempt + 1}/{max_retries} for {method} {url} failed: {error}. "
                    f"Retrying in {wait_time}s."
                )
                if attempt == max_retries - 1:
                    raise

                await asyncio.sleep(wait_time)
    finally:
        if should_close_client:
            await client.aclose()

    raise httpx.HTTPError(
        f"Failed to fetch data using {method} for {url} after {max_retries} retries."
    )


async def run_tasks_concurrently(
    items: list[T],
    processor: Callable[[T], Coroutine[Any, Any, R]],
    concurrency_limit: int = 5,
    description: str = "Processing",
) -> list[R]:
    """
    Runs a list of async tasks concurrently with a semaphore limit and a progress bar.

    Args:
        items (list[T]): List of items to process.
        processor (Callable[[T], Coroutine[Any, Any, R]]): Async function to process each item.
        concurrency_limit (int): Max concurrent tasks.
        description (str): Description for the progress bar.

    Returns:
        list[R]: List of results in the order of the input items.
    """
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def sem_task(item: T) -> R:
        async with semaphore:
            return await processor(item)

    tasks = [sem_task(item) for item in items]
    return await tqdm_asyncio.gather(*tasks, desc=description)


async def yield_batches_concurrently(
    items: list[T],
    batch_size: int,
    processor_fn: Callable[[list[T], httpx.AsyncClient], Coroutine[Any, Any, list[R]]],
    concurrency_limit: int,
    description: str = "Processing Batches",
    timeout: int = 60,
    client: Optional[httpx.AsyncClient] = None,
) -> AsyncIterable[R]:
    """
    Processes a list of items in batches concurrently and yields results as they complete.
    Args:
        items: List of items to process.
        batch_size: Number of items per batch.
        processor_fn: Async function that takes a batch and a client, returning a list of results.
        concurrency_limit: Max number of concurrent batches.
        description: Progress bar description.
        timeout: Timeout for the httpx client.
        client: Optional existing client to reuse.
    Yields:
        Individual result items from the processed batches.
    """
    chunks = [items[i: i + batch_size] for i in range(0, len(items), batch_size)]
    semaphore = asyncio.Semaphore(concurrency_limit)
    should_close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        should_close_client = True
    try:
        async def worker(batch: list[T]) -> list[R]:
            async with semaphore:
                return await processor_fn(batch, client)
        tasks = [worker(chunk) for chunk in chunks]

        # Use tqdm to wrap the as_completed iterator
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=description):
            batch_results = await future
            for res in batch_results:
                yield res
    finally:
        if should_close_client:
            await client.aclose()
