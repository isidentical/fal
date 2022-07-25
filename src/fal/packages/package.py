from __future__ import annotations
from contextlib import contextmanager

import hashlib
import threading
import shutil
import subprocess
import json
from collections import defaultdict
from functools import cached_property, partial
from dataclasses import dataclass, field
from typing import (
    Callable,
    Dict,
    Any,
    List,
    Optional,
    ContextManager,
    Iterator,
    ClassVar,
    DefaultDict,
)
from pathlib import Path

import yaml
from platformdirs import user_cache_dir

from faldbt.project import FalDbt

_BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
_BASE_CACHE_DIR.mkdir(exist_ok=True)

_BASE_REPO_DIR = _BASE_CACHE_DIR / "repos"
_BASE_REPO_DIR.mkdir(exist_ok=True)

_BASE_VENV_DIR = _BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(exist_ok=True)

_FAL_HOOKS_FILES = [".fal-hooks.yml", ".fal-hooks.yaml"]

# TODO: use the same version as the host
DEFAULT_REQUIREMENTS = [
    "git+https://github.com/isidentical/fal@repo-fetcher",
    "dbt-core",
    # TODO: use the same adapters as the host
    "dbt-postgres",
]


@dataclass(frozen=True)
class Package:
    url: str

    # TODO: use something better than a revision here,
    # like a commit hash.
    revision: str

    # A mapping of packages to locks, for preventing race
    # conditions when two hooks with the same url+revision
    # gets cloned at the same time.
    _CLONE_LOCKS: ClassVar[DefaultDict[threading.Lock]] = defaultdict(threading.Lock)
    @cached_property
    def _key(self) -> str:
        """
        Each package is uniquely identified by its URL and revision.
        """

        # TODO: maybe in the future consider only using the URL.
        #
        # The reason we encode it right now is that; if there are
        # multiple Fal processes running at the same time, they
        # might end up in a race where they check out different revisions
        # before actually reading the spec.
        #
        # If in the future we find a way to read some data from the Git
        # tree directly (without actually changing the state of the repo),
        # we might remove this restriction.

        crypt = hashlib.sha256()
        crypt.update(self.url.encode())
        crypt.update(self.revision.encode())
        return crypt.hexdigest()

    @cached_property
    def path(self) -> Path:
        return _BASE_REPO_DIR / self._key

    def clone(self) -> None:
        from dulwich import porcelain

        with self._CLONE_LOCKS[self._key]:
            if self.path.exists():
                print(f"Using cache for {self.url}@{self.revision}.")
            else:
                print(f"Cloning {self.url}@{self.revision}.")
                porcelain.clone(self.url, self.path, branch=self.revision.encode())

    def _load_raw_spec(self) -> Dict[str, Any]:
        self.clone()
        fal_hooks_paths = [
            self.path / file_name
            for file_name in _FAL_HOOKS_FILES
            if (self.path / file_name).exists()
        ]
        if len(fal_hooks_paths) == 0:
            raise Exception("not a fal package!!")
        elif len(fal_hooks_paths) > 1:
            raise Exception("only a single hook definition file must exist!!")

        [fal_hooks_path] = fal_hooks_paths
        with open(fal_hooks_path) as stream:
            # TODO: implement schema validation
            return yaml.safe_load(stream)

    def get_hook(self, id: str) -> Optional[RemoteHook]:
        for entry in self._load_raw_spec():
            if entry["id"] == id:
                path = self.path / entry.pop("path")
                environment = create_environment(entry.pop("environment", {}))
                return RemoteHook(**entry, path=path, environment=environment)


HookRunner = Callable[["RemoteHook", Dict[str, Any]], None]


@contextmanager
def _clear_on_fail(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        shutil.rmtree(path)
        raise


class Environment:
    def setup(self) -> ContextManager[HookRunner]:
        raise NotImplementedError


@dataclass(frozen=True)
class VirtualPythonEnvironment(Environment):
    requirements: List[str] = field(default_factory=list)

    _VENV_LOCKS: ClassVar[DefaultDict[threading.Lock]] = defaultdict(threading.Lock)

    def __post_init__(self) -> None:
        self.requirements.extend(DEFAULT_REQUIREMENTS)

    @cached_property
    def _key(self) -> str:
        crypt = hashlib.sha256()
        crypt.update(" ".join(self.requirements).encode())
        return crypt.hexdigest()

    @contextmanager
    def setup(self) -> Iterator[HookRunner]:
        venv_path = self._create_venv()
        yield partial(self._run_in_venv, venv_path)

    def _create_venv(self) -> Path:
        from virtualenv import cli_run

        path = _BASE_VENV_DIR / self._key
        with self._VENV_LOCKS[self._key]:
            with _clear_on_fail(path):
                if path.exists():
                    return path

                print(f"Creating virtual environment at {path}.")
                cli_run([str(path)])

                print(f"Installing the requirements: {', '.join(self.requirements)}")
                pip_path = path / "bin" / "pip"
                subprocess.check_call([pip_path, "install"] + self.requirements)

        return path

    def _run_in_venv(
        self,
        venv_path: Path,
        fal_dbt: FalDbt,
        hook_path: Path,
        arguments: Dict[str, Any],
        bound_model_name: str,
    ) -> None:
        python_path = venv_path / "bin" / "python"
        data = json.dumps(
            {
                "bound_model_name": bound_model_name,
                "fal_dbt_config": {
                    "project_dir": fal_dbt.project_dir,
                    "profiles_dir": fal_dbt.profiles_dir,
                    # TODO: include more information, like profiles-target
                },
                "arguments": arguments,
            }
        )
        subprocess.check_call(
            [python_path, "-m", "fal.packages._run_hook", hook_path, data]
        )


def create_environment(data: Dict[str, Any]) -> Environment:
    environment_type = data.get("environment_type", "venv")
    if environment_type == "venv":
        return VirtualPythonEnvironment(data.get("requirements", []))
    raise ValueError(f"Unknown environment type: {environment_type}")


@dataclass(frozen=True)
class RemoteHook:
    id: str
    path: Path
    environment: Environment
    name: Optional[str] = None
    description: Optional[str] = None

    def run(
        self, fal_dbt: FalDbt, arguments: Dict[str, Any], bound_model_name: str
    ) -> None:
        with self.environment.setup() as runner:
            return runner(fal_dbt, self.path, arguments, bound_model_name)
