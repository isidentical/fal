from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, Dict, Optional, List
import subprocess
import json
import faldbt.lib as lib
from dbt.logger import GLOBAL_LOGGER as logger
import os
import shutil
import traceback
from os.path import exists
import argparse


class DbtCliOutput:
    def __init__(
        self,
        command: str,
        return_code: int,
        raw_output: str,
        logs: List[Dict[str, Any]],
        run_results: Dict[str, Any],
    ):
        self._command = command
        self._return_code = return_code
        self._raw_output = raw_output
        self._logs = logs
        self._run_results = run_results

    @property
    def docs_url(self) -> Optional[str]:
        return None

    @property
    def command(self) -> str:
        return self._command

    @property
    def return_code(self) -> int:
        return self._return_code

    @property
    def raw_output(self) -> str:
        return self._raw_output

    @property
    def logs(self) -> List[Dict[str, Any]]:
        return self._logs

    @property
    def run_results(self) -> Dict[str, Any]:
        return self._run_results


def raise_for_dbt_run_errors(output: DbtCliOutput):
    if output.return_code != 0:
        raise RuntimeError("Error running dbt run")


def get_dbt_command_list(args: argparse.Namespace, models_list: List[str]) -> List[str]:
    command_list = ["dbt", "--log-format", "json"]

    if args.debug:
        command_list += ["--debug"]

    command_list += ["run"]

    if args.project_dir:
        command_list += ["--project-dir", args.project_dir]
    if args.profiles_dir:
        command_list += ["--profiles-dir", args.profiles_dir]

    if args.threads:
        command_list += ["--threads", args.threads]

    if args.defer:
        command_list += ["--defer"]

    if args.state:
        command_list += ["--state", args.state]

    if args.target:
        command_list += ["--target", args.target]

    if args.vars is not None and args.vars != "{}":
        command_list += ["--vars", args.vars]

    if len(models_list) > 0:
        if lib.IS_DBT_V0:
            command_list += ["--models"] + models_list
        else:
            command_list += ["--select"] + models_list

    # Assure all command parts are str
    return list(map(str, command_list))


def dbt_run(
    args: argparse.Namespace, models_list: List[str], target_path: str, run_index: int
):
    "Run the dbt run command in a subprocess"

    command_list = get_dbt_command_list(args, models_list)

    # Execute the dbt CLI command in a subprocess.
    full_command = " ".join(command_list)

    logger.info(f"Executing command: {full_command}")

    return_code = 0
    logs = []
    output = []

    process = subprocess.Popen(
        command_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    for raw_line in process.stdout or []:
        line = raw_line.decode("utf-8")
        output.append(line)
        try:
            json_line = json.loads(line)
        except json.JSONDecodeError:
            logger.error(line.rstrip())
            pass
        else:
            logs.append(json_line)
            logger.info(json_line.get("message", json_line.get("msg", line.rstrip())))

    process.wait()
    return_code = process.returncode

    logger.debug(f"dbt exited with return code {return_code}")

    raw_output = "\n".join(output)

    _create_fal_result_file(target_path, run_index)

    run_results = _get_index_run_results(target_path, run_index)

    # Remove run_result.json files in between dbt runs during the same fal flow run
    os.remove(_get_run_result_file(target_path))

    return DbtCliOutput(
        command=full_command,
        return_code=return_code,
        raw_output=raw_output,
        logs=logs,
        run_results=run_results,
    )


def _get_run_result_file(target_path: str) -> str:
    return os.path.join(target_path, "run_results.json")


def _create_fal_result_file(target_path: str, run_index: int):
    fal_run_result = _get_run_result_file(target_path)
    if exists(fal_run_result):
        shutil.copy(
            fal_run_result, os.path.join(target_path, f"fal_results_{run_index}.json")
        )


def _process_logs(raw_logs: str):
    logs, output = [], []
    for line in raw_logs.splitlines():
        output.append(line)
        try:
            json_line = json.loads(line)
        except json.JSONDecodeError:
            logger.error(line.rstrip())
        else:
            if "message" in json_line:
                try:
                    actual_msg = json.loads(json_line["message"])["msg"]
                except:
                    actual_msg = json_line["message"]
            else:
                actual_msg = line.rstrip()
            logger.info(actual_msg)

    return logs, output


@contextmanager
def _patch_log_manager(log_manager, output):
    # Temporarily patch the log_manager's stdout and stderr with the given
    # 'output' buffer, and revert it back at the exit of the context manager.
    original_stdout, original_stderr = log_manager.stdout, log_manager.stderr
    try:
        log_manager.set_output_stream(output)
        yield
    finally:
        log_manager.reset_handlers()
        log_manager.set_output_stream(original_stdout, original_stderr)


def _dbt_run_through_python(args: List[str], target_path: str, run_index: int):
    import io
    from dbt.main import handle_and_check
    from dbt.logger import log_manager

    # This is the core function where we execute the dbt run command. We'll initially
    # redirect all possible streams into our output (this is very similiar how subprocess)
    # is doing.
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output), _patch_log_manager(
        log_manager, output
    ):
        # Then we'll run the given DBT command, and get the 'run_results'.
        try:
            run_results, success = handle_and_check(args)
        except BaseException as exc:
            run_results = None
            return_code = getattr(exc, "code", 1)
            traceback.print_exc(exc)
        else:
            return_code = 0 if success else 1

    logger.debug(f"dbt exited with return code {return_code}")

    raw_output = output.getvalue()
    logs, _ = _process_logs(raw_output)

    # The 'run_results' object has a 'write()' method which is basically json.dump().
    # We'll dump it directly to the fal results file (instead of first dumping it to
    # run results and then copying it over).
    if run_results is not None:
        run_results.write(os.path.join(target_path, f"fal_results_{run_index}.json"))
    else:
        raise NotImplementedError("TODO: What should happen if dbt run fails and there are no run results file?")

    return return_code, raw_output, logs


def dbt_run_through_python(
    args: argparse.Namespace, models_list: List[str], target_path: str, run_index: int
):
    """Run DBT from the Python entry point in a subprocess."""
    from multiprocessing import Pool

    # It is important to split the first argument, since it contains the
    # name of the executable ('dbt').
    args = get_dbt_command_list(args, models_list)[1:]
    args_as_text = " ".join(args)
    logger.info(f"Running DBT with these options: {args_as_text}")

    # We are going to use multiprocessing module to spawn a new
    # process that will run the sub-function we have here. We
    # don't really need a process pool, since we are only going
    # to spawn a single process but it provides a nice abstraction
    # over retrieving values from the process.

    with Pool(processes=1) as pool:
        return_code, raw_output, logs = pool.apply(
            _dbt_run_through_python,
            (args, target_path, run_index),
        )

    run_results = _get_index_run_results(target_path, run_index)
    return DbtCliOutput(
        command="dbt " + args_as_text,
        return_code=return_code,
        raw_output=raw_output,
        logs=logs,
        run_results=run_results,
    )


def _get_index_run_results(target_path: str, run_index: int) -> Dict[Any, Any]:
    """Get run results for a given run index."""
    with open(
        os.path.join(target_path, f"fal_results_{run_index}.json")
    ) as raw_results:
        return json.load(raw_results)
