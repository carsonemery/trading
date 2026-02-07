"""Polygon S3 flatfiles client for downloading historical market data.

This module provides a class-based client for asynchronously downloading
Polygon's daily aggregate data from their S3 bucket.
"""

import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import aioboto3
import boto3
import pandas as pd
from botocore.config import Config

from bearplanes.utils.config import get_aws_credentials


@dataclass(frozen=True, slots=True)
class DownloadJob:
    """Represents a single day's worth of data to download."""
    object_name: str  # S3 key (full path)
    path: str  # Local file path
    date_str: str  # Date in YYYY-MM-DD format


class PolygonS3Client:
    """Client for downloading Polygon flatfiles from S3.
    
    This client handles asynchronous downloads of daily aggregate stock data
    from Polygon's S3 bucket with configurable concurrency, retries, and caching.
    
    Examples:
        >>> client = PolygonS3Client()
        >>> df = client.download_daily_bars(
        ...     start_date='2024-01-01',
        ...     end_date='2024-01-31',
        ...     output_dir='data/polygon/daily'
        ... )
    """
    
    # Polygon S3 configuration
    BUCKET_NAME = 'flatfiles'
    ENDPOINT_URL = 'https://files.polygon.io'
    SIGNATURE_VERSION = 's3v4'
    DAILY_BARS_PREFIX = 'us_stocks_sip/day_aggs_v1/'
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None
    ):
        """Initialize Polygon S3 client.
        
        Args:
            aws_access_key_id: AWS access key. If None, reads from environment.
            aws_secret_access_key: AWS secret key. If None, reads from environment.
        """
        # Get credentials
        creds = get_aws_credentials()
        self.aws_access_key_id = aws_access_key_id or creds["access_key_id"]
        self.aws_secret_access_key = aws_secret_access_key or creds["secret_access_key"]
        
        # Initialize async session
        self.session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
    
    def download_daily_bars(
        self,
        start_date: str,
        end_date: str,
        output_dir: Path,
        max_concurrency: int = 20,
        skip_existing: bool = True,
        retries: int = 3,
        backoff_seconds: float = 0.5,
        return_dataframe: bool = False
    ) -> Optional[pd.DataFrame]:
        """Download daily aggregate bars for date range.
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format (inclusive).
            end_date: End date in 'YYYY-MM-DD' format (exclusive).
            output_dir: Directory to save downloaded files.
            max_concurrency: Maximum concurrent downloads.
            skip_existing: If True, skip files that already exist locally.
            retries: Number of retry attempts per file.
            backoff_seconds: Initial backoff time for retries (exponential).
            return_dataframe: If True, load all files into a DataFrame and return it.
            
        Returns:
            DataFrame if return_dataframe=True, otherwise None.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Build download plan
        jobs = self._build_download_list(
            prefix=self.DAILY_BARS_PREFIX,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir
        )
        
        print(f"\nFound {len(jobs)} files to download")
        
        # Execute downloads
        asyncio.run(self._download_files_async(
            jobs=jobs,
            max_concurrency=max_concurrency,
            skip_existing=skip_existing,
            retries=retries,
            backoff_seconds=backoff_seconds
        ))
        
        # Optionally load into DataFrame
        if return_dataframe:
            return self._read_files_into_df(output_dir)
        
        return None
    
    def _build_download_list(
        self,
        prefix: str,
        start_date: str,
        end_date: str,
        output_dir: Path
    ) -> List[DownloadJob]:
        """Build list of files to download from S3.
        
        Args:
            prefix: S3 prefix to list objects under.
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            output_dir: Local directory for downloads.
            
        Returns:
            List of DownloadJob objects.
        """
        print("\nBuilding download list...")
        print("=" * 40)
        
        jobs: List[DownloadJob] = []
        
        # Create sync S3 client for pagination
        s3_sync = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.ENDPOINT_URL,
            config=Config(signature_version=self.SIGNATURE_VERSION)
        )
        
        # Calculate start_after key (day before start_date)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        day_before = start_dt - timedelta(days=1)
        year = day_before.strftime('%Y')
        month = day_before.strftime('%m')
        date_str = day_before.strftime('%Y-%m-%d')
        start_after_key = f'{prefix}{year}/{month}/{date_str}.csv.gz'
        
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Paginate through S3 objects
        paginator = s3_sync.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(
            Bucket=self.BUCKET_NAME,
            Prefix=prefix,
            StartAfter=start_after_key
        ):
            for obj in page.get('Contents', []):
                object_name = str(obj['Key'])
                
                # Extract date from S3 key
                match = re.search(r'(\d{4}-\d{2}-\d{2})', object_name)
                if not match:
                    print(f"Warning: Unexpected key format: {object_name}")
                    continue
                
                date_str = match.group(1)
                date = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Check if past end date
                if date >= end_dt:
                    return jobs
                
                # Build local file path
                filename = f"{date_str}.csv.gz"
                path = str(output_dir / filename)
                
                # Add to job list
                jobs.append(DownloadJob(
                    object_name=object_name,
                    path=path,
                    date_str=date_str
                ))
                
                print(f"  {date_str}")
        
        return jobs
    
    async def _download_files_async(
        self,
        jobs: List[DownloadJob],
        max_concurrency: int,
        skip_existing: bool,
        retries: int,
        backoff_seconds: float
    ) -> None:
        """Orchestrate concurrent downloads with bounded concurrency.
        
        Args:
            jobs: List of download jobs.
            max_concurrency: Maximum concurrent downloads.
            skip_existing: Skip files that already exist.
            retries: Number of retry attempts.
            backoff_seconds: Initial backoff time for retries.
        """
        print(f"\nDownloading {len(jobs)} files (max concurrency: {max_concurrency})...")
        print("=" * 40)
        
        # Create semaphore for bounded concurrency
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async with self.session.client(
            's3',
            endpoint_url=self.ENDPOINT_URL,
            config=Config(signature_version=self.SIGNATURE_VERSION),
        ) as s3_async:
            async with asyncio.TaskGroup() as tg:
                for job in jobs:
                    tg.create_task(self._guarded_download(
                        s3=s3_async,
                        semaphore=semaphore,
                        job=job,
                        skip_existing=skip_existing,
                        retries=retries,
                        backoff_seconds=backoff_seconds,
                    ))
        
        print("\nDownload complete!")
    
    async def _guarded_download(
        self,
        s3,
        semaphore: asyncio.Semaphore,
        job: DownloadJob,
        skip_existing: bool,
        retries: int,
        backoff_seconds: float
    ) -> None:
        """Download with bounded concurrency, skip-if-exists, and retries.
        
        Args:
            s3: Async S3 client.
            semaphore: Semaphore for concurrency control.
            job: Download job.
            skip_existing: Skip if file exists.
            retries: Number of retry attempts.
            backoff_seconds: Initial backoff time.
        """
        # Skip if file exists
        if skip_existing and os.path.exists(job.path):
            return
        
        async with semaphore:
            attempt = 0
            while True:
                try:
                    await self._download_file(s3, job)
                    return
                except Exception as e:
                    attempt += 1
                    if attempt > retries:
                        print(f"Failed: {job.date_str} after {retries} retries: {e}")
                        return
                    # Exponential backoff
                    await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))
    
    async def _download_file(
        self,
        s3,
        job: DownloadJob
    ) -> None:
        """Download a single file from S3.
        
        Args:
            s3: Async S3 client.
            job: Download job.
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(job.path), exist_ok=True)
        
        # Download file
        await s3.download_file(self.BUCKET_NAME, job.object_name, job.path)
        print(f"{job.date_str}")
    
    def _read_files_into_df(self, directory: Path) -> pd.DataFrame:
        """Read all CSV.GZ files in directory into a single DataFrame.
        
        Args:
            directory: Directory containing CSV.GZ files.
            
        Returns:
            Concatenated DataFrame of all files.
        """
        print(f"\nLoading files from {directory}...")
        
        dataframes = []
        files = sorted([f for f in os.listdir(directory) if f.endswith('csv.gz')])
        
        for file in files:
            filepath = os.path.join(directory, file)
            df = pd.read_csv(filepath, compression='gzip')
            dataframes.append(df)
        
        result = pd.concat(dataframes, ignore_index=True)
        print(f"Loaded {len(result):,} rows from {len(files)} files")
        
        return result
