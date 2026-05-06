import asyncio
from datetime import timedelta
from typing import Coroutine, cast

import aiofiles
import httpx
from prefect.tasks import task_input_hash
from prefect import task, flow, get_run_logger
from config import config
from schema import BatchApplyResponse, ExtractResult
from pydantic import FilePath
from config import config


def mineru_task_cache_key(context, parameters):
    params = parameters.copy()
    params.pop("client", None)
    return task_input_hash(context, params)


@task(
    cache_key_fn=mineru_task_cache_key,
    persist_result=True,
    retries=3
)
async def apply_batch_urls(client: httpx.AsyncClient, files: list[FilePath]) -> BatchApplyResponse:
    """Step 1. Apply Batch Urls"""

    url = config.apply_batch_url
    headers = {"Authorization": f"Bearer {config.api_token.get_secret_value()}"}
    data = {"files": [{"name": f.name, "data_id": str(i)} for i, f in enumerate(files)],
            "model_version": config.model_version}

    resp = await client.post(url, json=data, headers=headers)
    resp.raise_for_status()
    result = resp.json()

    if result["code"] != 0:
        raise Exception(f"API Error: {result['msg']}")

    return BatchApplyResponse(
        batch_id=result["data"]["batch_id"],
        file_urls=result["data"]["file_urls"]
    )


@task(
    cache_key_fn=mineru_task_cache_key,
    retries=3,
    retry_delay_seconds=5
)
async def upload_file_async(client: httpx.AsyncClient, file_path: FilePath, upload_url: str):
    """Step 2. Upload Files Asynchronously"""

    logger = get_run_logger()

    logger.info(f"Starting upload for {file_path.name} ({file_path.stat().st_size / 1024 / 1024:.2f} MB)")

    async with aiofiles.open(file_path, mode='rb') as f:
        async def file_generator():
            while chunk := await f.read(64 * 1024):
                yield chunk

        resp = await client.put(upload_url, content=file_generator())
        resp.raise_for_status()

    logger.info(f"Successfully uploaded: {file_path.name}")


@task(
    cache_key_fn=mineru_task_cache_key,
    cache_expiration=timedelta(seconds=0),
    retries=3
)
async def wait_for_results(client: httpx.AsyncClient, batch_id: str, timeout_mins: int = 40) -> list[ExtractResult]:
    """Step 3. Wait For Results"""

    logger = get_run_logger()

    url = f"{config.extract_results_url}/{batch_id}"
    headers = {"Authorization": f"Bearer {config.api_token.get_secret_value()}"}

    import time
    start_time = time.time()
    poll_interval = 2.0

    while True:
        if (time.time() - start_time) > timeout_mins * 60:
            raise TimeoutError(f"MinerU extraction timed out after {timeout_mins} minutes.")

        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        full_res = resp.json()

        results_list = full_res.get("data", {}).get("extract_result", [])

        if not results_list:
            logger.warning("No results found in API response yet...")
        else:
            is_all_done = all(item.get("state") in ["done", "failed"] for item in results_list)

            if is_all_done:
                logger.info("Batch extraction complete.")
                return [
                    ExtractResult(
                        file_id=item.get("data_id", "unknown"),
                        status=item.get("state", "unknown"),
                        full_zip_url=item.get("full_zip_url"),
                        extract_result=item
                    ) for item in results_list
                ]

        if results_list:
            current_state = results_list[0].get("state")
            logger.info(f"Current state: {current_state} (Elapsed: {int(time.time() - start_time)}s)")

        sleep_time = min(poll_interval, max(0, timeout_mins * 60 - (time.time() - start_time)))
        await asyncio.sleep(sleep_time)
        poll_interval = min(poll_interval * 2, 30.0)


@flow(name="MinerU Multimodal Processing Pipeline")
async def mineru_batch_flow(file_paths: list[FilePath]) -> list[ExtractResult]:
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Step 1. Apply Batch URLs
        apply_info = await cast(Coroutine, apply_batch_urls(client, file_paths))

        # Step 2. Upload files concurrently
        upload_tasks = [
            upload_file_async(client, path, url)
            for path, url in zip(file_paths, apply_info.file_urls)
        ]
        await asyncio.gather(*upload_tasks)

        # Step 3. Wait for results
        results = await cast(Coroutine, wait_for_results(client, apply_info.batch_id))
        return results
