import logging
from pathlib import Path

from huggingface_hub import HfApi

logger = logging.getLogger(__name__)


def push_dataset_file_to_hf(
    data_path: Path,
    repo_id: str,
    revision: str = "main",
    commit_message: str = "Database updated",
) -> None:
    """Push a Dataset file (Parquet, CSV) to Hugging Face Hub.

    Args:
        data_path (Path): The path to the dataset file to push. The name of the
            file will be used as the path in the repository.
        repo_id (str, optional): The repository ID on Hugging Face Hub.
        revision (str, optional): The revision to push the data to. Defaults to
            "main".
        commit_message (str, optional): The commit message. Defaults to
            "Database updated".
    """
    logger.info("Start pushing data to Hugging Face at %s", repo_id)
    if not data_path.exists():
        raise FileNotFoundError(f"Data is missing: {data_path}")
    if data_path.suffix not in [".parquet", ".csv"]:
        raise ValueError(f"A parquet file is expected. Got {data_path.suffix} instead.")
    # We use the HF_Hub api since it gives us way more flexibility than
    # push_to_hub()
    HfApi().upload_file(
        path_or_fileobj=data_path,
        repo_id=repo_id,
        revision=revision,
        repo_type="dataset",
        path_in_repo=data_path.name,
        commit_message=commit_message,
    )
    logger.info("Data succesfully pushed to Hugging Face at %s", repo_id)
