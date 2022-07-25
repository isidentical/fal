import json
from argparse import ArgumentParser
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class PackageContext:
    args: Dict[str, Any]


def run_fal_hook(
    path: Path,
    bound_model_name: str,
    fal_dbt_config: Dict[str, Any],
    arguments: Dict[str, Any],
) -> None:
    from faldbt.project import FalDbt
    from fal.node_graph import NodeGraph, DbtModelNode, FalScript

    fal_dbt = FalDbt(**fal_dbt_config)

    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    flow_node = node_graph.get_node(bound_model_name)
    assert isinstance(flow_node, DbtModelNode)

    fal_script = FalScript(fal_dbt, flow_node.model, str(path), True)
    fal_script.exec({"package_context": PackageContext(arguments)})


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("data", type=str)

    options = parser.parse_args()
    run_fal_hook(options.path, **json.loads(options.data))


if __name__ == "__main__":
    main()
