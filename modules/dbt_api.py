"""
dbt Cloud API Integration Module
================================
Handles all interactions with the dbt Cloud API for creating projects,
jobs, and managing runs.
"""

import requests
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import time


class JobRunStatus(Enum):
    """dbt Cloud job run statuses."""
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


@dataclass
class JobConfig:
    """Configuration for a dbt Cloud job."""
    name: str
    project_id: int
    environment_id: int
    dbt_version: Optional[str] = None
    execute_steps: Optional[List[str]] = None
    triggers: Optional[Dict[str, Any]] = None
    settings: Optional[Dict[str, Any]] = None
    schedule: Optional[Dict[str, Any]] = None
    

class DBTCloudAPI:
    """
    Client for interacting with the dbt Cloud API.
    
    Supports creating projects, environments, jobs, and triggering runs
    for both Core and Fusion engine comparisons.
    """
    
    BASE_URL = "https://cloud.getdbt.com/api/v2"
    
    def __init__(self, account_id: str, api_token: str, base_url: Optional[str] = None):
        """
        Initialize the dbt Cloud API client.
        
        Args:
            account_id: The dbt Cloud account ID
            api_token: The dbt Cloud API token
            base_url: Optional custom base URL (for enterprise deployments)
        """
        self.account_id = account_id
        self.api_token = api_token
        self.base_url = base_url or self.BASE_URL
        
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json",
        })
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make an authenticated request to the dbt Cloud API."""
        url = f"{self.base_url}/accounts/{self.account_id}/{endpoint}"
        
        response = self.session.request(
            method=method,
            url=url,
            json=data,
            params=params
        )
        
        response.raise_for_status()
        return response.json()
    
    # ==================== Projects ====================
    
    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects in the account."""
        result = self._make_request("GET", "projects/")
        return result.get("data", [])
    
    def get_project(self, project_id: int) -> Dict[str, Any]:
        """Get details of a specific project."""
        result = self._make_request("GET", f"projects/{project_id}/")
        return result.get("data", {})
    
    def create_project(
        self,
        name: str,
        connection_id: Optional[int] = None,
        repository_id: Optional[int] = None,
        dbt_project_subdirectory: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new dbt Cloud project.
        
        Args:
            name: Project name
            connection_id: Database connection ID
            repository_id: Git repository ID
            dbt_project_subdirectory: Path to dbt_project.yml in repo
            
        Returns:
            Created project data
        """
        data = {"name": name}
        
        if connection_id:
            data["connection_id"] = connection_id
        if repository_id:
            data["repository_id"] = repository_id
        if dbt_project_subdirectory:
            data["dbt_project_subdirectory"] = dbt_project_subdirectory
        
        result = self._make_request("POST", "projects/", data=data)
        return result.get("data", {})
    
    # ==================== Environments ====================
    
    def list_environments(self, project_id: int) -> List[Dict[str, Any]]:
        """List all environments in a project."""
        result = self._make_request(
            "GET", 
            "environments/",
            params={"project_id": project_id}
        )
        return result.get("data", [])
    
    def create_environment(
        self,
        project_id: int,
        name: str,
        dbt_version: str = "1.7.0",
        environment_type: str = "deployment",
        use_custom_branch: bool = False,
        custom_branch: Optional[str] = None,
        credentials_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a new environment in a project.
        
        Args:
            project_id: The project ID
            name: Environment name
            dbt_version: dbt version to use
            environment_type: 'deployment' or 'development'
            use_custom_branch: Whether to use a custom git branch
            custom_branch: The custom branch name
            credentials_id: Database credentials ID
            
        Returns:
            Created environment data
        """
        data = {
            "project_id": project_id,
            "name": name,
            "dbt_version": dbt_version,
            "type": environment_type,
            "use_custom_branch": use_custom_branch,
        }
        
        if custom_branch:
            data["custom_branch"] = custom_branch
        if credentials_id:
            data["credentials_id"] = credentials_id
        
        result = self._make_request("POST", "environments/", data=data)
        return result.get("data", {})
    
    # ==================== Jobs ====================
    
    def list_jobs(self, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """List all jobs, optionally filtered by project."""
        params = {}
        if project_id:
            params["project_id"] = project_id
        
        result = self._make_request("GET", "jobs/", params=params)
        return result.get("data", [])
    
    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Get details of a specific job."""
        result = self._make_request("GET", f"jobs/{job_id}/")
        return result.get("data", {})
    
    def create_job(self, config: JobConfig) -> Dict[str, Any]:
        """
        Create a new job in dbt Cloud.
        
        Args:
            config: JobConfig with job settings
            
        Returns:
            Created job data
        """
        data = {
            "name": config.name,
            "project_id": config.project_id,
            "environment_id": config.environment_id,
            "execute_steps": config.execute_steps or ["dbt build"],
            "triggers": config.triggers or {
                "github_webhook": False,
                "schedule": False,
                "git_provider_webhook": False,
            },
            "settings": config.settings or {
                "threads": 4,
                "target_name": "default",
            },
        }
        
        if config.dbt_version:
            data["dbt_version"] = config.dbt_version
        
        if config.schedule:
            data["schedule"] = config.schedule
        
        result = self._make_request("POST", "jobs/", data=data)
        return result.get("data", {})
    
    def create_core_vs_fusion_jobs(
        self,
        project_id: int,
        environment_id: int,
        base_name: str = "SAO Comparison"
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Create two comparison jobs: one for Core, one for Fusion.
        
        Args:
            project_id: The project ID
            environment_id: The environment ID
            base_name: Base name for the jobs
            
        Returns:
            Tuple of (core_job, fusion_job) data
        """
        # Create Core job (traditional execution)
        core_config = JobConfig(
            name=f"{base_name} - dbt Core",
            project_id=project_id,
            environment_id=environment_id,
            execute_steps=[
                "dbt deps",
                "dbt build --full-refresh"  # Full refresh for baseline
            ],
            settings={
                "threads": 4,
                "target_name": "core_target",
            }
        )
        core_job = self.create_job(core_config)
        
        # Create Fusion job (with SAO enabled)
        fusion_config = JobConfig(
            name=f"{base_name} - dbt Fusion (SAO)",
            project_id=project_id,
            environment_id=environment_id,
            execute_steps=[
                "dbt deps",
                "dbt build"  # SAO will optimize this
            ],
            settings={
                "threads": 4,
                "target_name": "fusion_target",
            }
        )
        fusion_job = self.create_job(fusion_config)
        
        return core_job, fusion_job
    
    def update_job(self, job_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing job."""
        result = self._make_request("POST", f"jobs/{job_id}/", data=updates)
        return result.get("data", {})
    
    def delete_job(self, job_id: int) -> bool:
        """Delete a job."""
        try:
            self._make_request("DELETE", f"jobs/{job_id}/")
            return True
        except Exception:
            return False
    
    # ==================== Runs ====================
    
    def trigger_run(
        self,
        job_id: int,
        cause: str = "Triggered via API",
        git_sha: Optional[str] = None,
        git_branch: Optional[str] = None,
        schema_override: Optional[str] = None,
        dbt_version_override: Optional[str] = None,
        threads_override: Optional[int] = None,
        target_name_override: Optional[str] = None,
        generate_docs_override: Optional[bool] = None,
        timeout_seconds_override: Optional[int] = None,
        steps_override: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a job run.
        
        Args:
            job_id: The job ID to run
            cause: Description of why the run was triggered
            git_sha: Specific git SHA to run
            git_branch: Specific git branch to run
            schema_override: Override the target schema
            dbt_version_override: Override dbt version
            threads_override: Override thread count
            target_name_override: Override target name
            generate_docs_override: Override doc generation
            timeout_seconds_override: Override timeout
            steps_override: Override execution steps
            
        Returns:
            Run data including run_id
        """
        data = {"cause": cause}
        
        if git_sha:
            data["git_sha"] = git_sha
        if git_branch:
            data["git_branch"] = git_branch
        if schema_override:
            data["schema_override"] = schema_override
        if dbt_version_override:
            data["dbt_version_override"] = dbt_version_override
        if threads_override:
            data["threads_override"] = threads_override
        if target_name_override:
            data["target_name_override"] = target_name_override
        if generate_docs_override is not None:
            data["generate_docs_override"] = generate_docs_override
        if timeout_seconds_override:
            data["timeout_seconds_override"] = timeout_seconds_override
        if steps_override:
            data["steps_override"] = steps_override
        
        result = self._make_request("POST", f"jobs/{job_id}/run/", data=data)
        return result.get("data", {})
    
    def get_run(self, run_id: int) -> Dict[str, Any]:
        """Get details of a specific run."""
        result = self._make_request("GET", f"runs/{run_id}/")
        return result.get("data", {})
    
    def list_runs(
        self,
        job_id: Optional[int] = None,
        project_id: Optional[int] = None,
        status: Optional[JobRunStatus] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List runs with optional filters."""
        params = {"limit": limit, "offset": offset}
        
        if job_id:
            params["job_definition_id"] = job_id
        if project_id:
            params["project_id"] = project_id
        if status:
            params["status"] = status.value
        
        result = self._make_request("GET", "runs/", params=params)
        return result.get("data", [])
    
    def wait_for_run(
        self,
        run_id: int,
        poll_interval: int = 10,
        timeout: int = 3600
    ) -> Dict[str, Any]:
        """
        Wait for a run to complete.
        
        Args:
            run_id: The run ID to wait for
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait
            
        Returns:
            Final run data
            
        Raises:
            TimeoutError: If run doesn't complete within timeout
        """
        start_time = time.time()
        
        while True:
            run = self.get_run(run_id)
            status = run.get("status")
            
            if status in [
                JobRunStatus.SUCCESS.value,
                JobRunStatus.ERROR.value,
                JobRunStatus.CANCELLED.value
            ]:
                return run
            
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Run {run_id} did not complete within {timeout} seconds")
            
            time.sleep(poll_interval)
    
    def cancel_run(self, run_id: int) -> Dict[str, Any]:
        """Cancel a running job."""
        result = self._make_request("POST", f"runs/{run_id}/cancel/")
        return result.get("data", {})
    
    # ==================== Run Artifacts ====================
    
    def get_run_artifact(
        self,
        run_id: int,
        artifact_path: str
    ) -> Any:
        """
        Get an artifact from a run.
        
        Args:
            run_id: The run ID
            artifact_path: Path to the artifact (e.g., 'manifest.json', 'run_results.json')
            
        Returns:
            Artifact contents
        """
        result = self._make_request(
            "GET",
            f"runs/{run_id}/artifacts/{artifact_path}"
        )
        return result
    
    def get_run_results(self, run_id: int) -> Dict[str, Any]:
        """Get run_results.json for a completed run."""
        return self.get_run_artifact(run_id, "run_results.json")
    
    def get_manifest(self, run_id: int) -> Dict[str, Any]:
        """Get manifest.json for a completed run."""
        return self.get_run_artifact(run_id, "manifest.json")
    
    # ==================== Comparison Methods ====================
    
    def run_comparison(
        self,
        core_job_id: int,
        fusion_job_id: int,
        wait_for_completion: bool = True
    ) -> Dict[str, Any]:
        """
        Run both Core and Fusion jobs for comparison.
        
        Args:
            core_job_id: Job ID for Core execution
            fusion_job_id: Job ID for Fusion execution
            wait_for_completion: Whether to wait for runs to complete
            
        Returns:
            Comparison results with metrics
        """
        # Trigger both runs
        core_run = self.trigger_run(
            core_job_id,
            cause="SAO Comparison - Core baseline"
        )
        fusion_run = self.trigger_run(
            fusion_job_id,
            cause="SAO Comparison - Fusion with SAO"
        )
        
        results = {
            "core_run_id": core_run.get("id"),
            "fusion_run_id": fusion_run.get("id"),
        }
        
        if wait_for_completion:
            # Wait for both runs
            core_final = self.wait_for_run(core_run.get("id"))
            fusion_final = self.wait_for_run(fusion_run.get("id"))
            
            # Get run results
            core_results = self.get_run_results(core_run.get("id"))
            fusion_results = self.get_run_results(fusion_run.get("id"))
            
            # Calculate comparison metrics
            results.update({
                "core_run": core_final,
                "fusion_run": fusion_final,
                "core_results": core_results,
                "fusion_results": fusion_results,
                "comparison": self._calculate_comparison_metrics(
                    core_results, fusion_results
                )
            })
        
        return results
    
    def _calculate_comparison_metrics(
        self,
        core_results: Dict[str, Any],
        fusion_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate comparison metrics between Core and Fusion runs."""
        core_models = len(core_results.get("results", []))
        fusion_models = len(fusion_results.get("results", []))
        
        # Count successful executions
        core_success = sum(
            1 for r in core_results.get("results", [])
            if r.get("status") == "success"
        )
        fusion_success = sum(
            1 for r in fusion_results.get("results", [])
            if r.get("status") == "success"
        )
        
        # Calculate timing
        core_timing = sum(
            r.get("timing", [{}])[-1].get("duration", 0)
            for r in core_results.get("results", [])
        )
        fusion_timing = sum(
            r.get("timing", [{}])[-1].get("duration", 0)
            for r in fusion_results.get("results", [])
        )
        
        # Models skipped by SAO
        models_skipped = core_models - fusion_models
        
        return {
            "core_models_executed": core_models,
            "fusion_models_executed": fusion_models,
            "models_skipped_by_sao": models_skipped,
            "core_execution_time": core_timing,
            "fusion_execution_time": fusion_timing,
            "time_saved_seconds": core_timing - fusion_timing,
            "time_saved_percentage": (
                (core_timing - fusion_timing) / core_timing * 100
                if core_timing > 0 else 0
            ),
        }

