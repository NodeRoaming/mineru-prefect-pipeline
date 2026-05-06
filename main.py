import asyncio
from pathlib import Path

from tasks_and_flow import mineru_batch_flow

if __name__ == "__main__":
    my_files = [Path('resources/example.pdf')]

    result = asyncio.run(mineru_batch_flow(my_files))

    for i, ret in enumerate(result):
        print(f"Zip {i + 1}: {ret.full_zip_url}")
