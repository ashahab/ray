import copy
import logging
import os
import sys
from typing import Optional, List

import yaml

# Env variables:

# RAY_REPO          Repo to use for finding the wheel
# RAY_BRANCH        Branch to find the wheel
# RAY_VERSION       Version to find the wheel
# RAY_WHEELS        Direct Ray wheel URL
# RAY_TEST_REPO     Repo to use for test scripts
# RAY_TEST_BRANCH   Branch for test scripts
# FILTER_FILE       File filter
# FILTER_TEST       Test name filter
# RELEASE_TEST_SUITE Release test suite (e.g. manual, nightly)


class ReleaseTest:
    def __init__(self, name: str, smoke_test: bool = False, retry: int = 0):
        self.name = name
        self.smoke_test = smoke_test
        self.retry = retry

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __contains__(self, item):
        return self.name.__contains__(item)

    def __iter__(self):
        return iter(self.name)

    def __len__(self):
        return len(self.name)


class SmokeTest(ReleaseTest):
    def __init__(self, name: str, retry: int = 0):
        super(SmokeTest, self).__init__(
            name=name, smoke_test=True, retry=retry)


class ConnectTest(ReleaseTest):
    """Release Test that requires extra setup on the driver."""

    def __init__(self,
                 *args,
                 setup_commands: Optional[List[str]] = None,
                 requirements_file: Optional[str] = None,
                 **kwargs):

        # Commands to run on the driver before kicking off the test.
        self.setup_commands = setup_commands if setup_commands else []

        # Requirements to install on the driver before kicking off the test.
        self.requirements_file = requirements_file

        super().__init__(*args, **kwargs)


CORE_NIGHTLY_TESTS = {
    "~/ray/release/nightly_tests/nightly_tests.yaml": [
        "shuffle_10gb",
        "shuffle_50gb",
        "shuffle_50gb_large_partition",
        "shuffle_100gb",
        "non_streaming_shuffle_100gb",
        "non_streaming_shuffle_50gb_large_partition",
        "non_streaming_shuffle_50gb",
        "dask_on_ray_10gb_sort",
        "dask_on_ray_100gb_sort",
        SmokeTest("dask_on_ray_large_scale_test_no_spilling"),
        SmokeTest("dask_on_ray_large_scale_test_spilling"),
        "stress_test_placement_group",
        "shuffle_1tb_1000_partition",
        "non_streaming_shuffle_1tb_1000_partition",
        "shuffle_1tb_5000_partitions",
        # TODO(sang): It doesn't even work without spilling
        # as it hits the scalability limit.
        # "non_streaming_shuffle_1tb_5000_partitions",
        "decision_tree_autoscaling",
        "decision_tree_autoscaling_20_runs",
        "grpc_decision_tree_autoscaling_20_runs",
        "autoscaling_shuffle_1tb_1000_partitions",
        SmokeTest("stress_test_many_tasks"),
        SmokeTest("stress_test_dead_actors"),
        "shuffle_data_loader",
        "dask_on_ray_1tb_sort",
        "many_nodes_actor_test",
        "grpc_many_nodes_actor_test",
    ],
    "~/ray/benchmarks/benchmark_tests.yaml": [
        "single_node",
        "object_store",
        "many_actors_smoke_test",
        "many_tasks_smoke_test",
        "many_pgs_smoke_test",
    ],
    "~/ray/release/nightly_tests/dataset/dataset_test.yaml": [
        "inference",
        "shuffle_data_loader",
        "pipelined_training_50_gb",
        "pipelined_ingestion_1500_gb_15_windows",
    ],
}

NIGHTLY_TESTS = {
    # "~/ray/release/horovod_tests/horovod_tests.yaml": [
    #     SmokeTest("horovod_test"),
    # ],  # Should we enable this?
    "~/ray/release/golden_notebook_tests/golden_notebook_tests.yaml": [
        "dask_xgboost_test",
        "modin_xgboost_test",
        "torch_tune_serve_test",
    ],
    "~/ray/release/nightly_tests/nightly_tests.yaml": [
        "dask_on_ray_large_scale_test_no_spilling",
        "dask_on_ray_large_scale_test_spilling",
        "pg_autoscaling_regression_test",
    ],
    "~/ray/release/long_running_tests/long_running_tests.yaml": [
        SmokeTest("actor_deaths"),
        SmokeTest("apex"),
        SmokeTest("impala"),
        SmokeTest("many_actor_tasks"),
        SmokeTest("many_drivers"),
        SmokeTest("many_ppo"),
        SmokeTest("many_tasks"),
        SmokeTest("many_tasks_serialized_ids"),
        SmokeTest("node_failures"),
        SmokeTest("pbt"),
        # SmokeTest("serve"),
        # SmokeTest("serve_failure"),
    ],
    "~/ray/release/microbenchmark/microbenchmark.yaml": [
        "microbenchmark",
    ],
    "~/ray/release/sgd_tests/sgd_tests.yaml": [
        "sgd_gpu",
    ],
    "~/ray/release/tune_tests/scalability_tests/tune_tests.yaml": [
        "bookkeeping_overhead",
        "durable_trainable",
        SmokeTest("long_running_large_checkpoints"),
        SmokeTest("network_overhead"),
        "result_throughput_cluster",
        "result_throughput_single_node",
        "xgboost_sweep",
    ],
    "~/ray/release/xgboost_tests/xgboost_tests.yaml": [
        "train_small",
        "train_moderate",
        "train_gpu",
        "tune_small",
        "tune_4x32",
        "tune_32x4",
        "ft_small_elastic",
        "ft_small_non_elastic",
        "distributed_api_test",
    ],
    "~/ray/release/rllib_tests/rllib_tests.yaml": [
        SmokeTest("learning_tests"),
        SmokeTest("stress_tests"),
        "multi_gpu_learning_tests",
        "multi_gpu_with_lstm_learning_tests",
        "multi_gpu_with_attention_learning_tests",
        # We'll have these as per-PR tests soon.
        # "example_scripts_on_gpu_tests",
    ],
    "~/ray/release/serve_tests/serve_tests.yaml": [
        "single_deployment_1k_noop_replica",
        "multi_deployment_1k_noop_replica",
        "serve_micro_benchmark",
        "serve_cluster_fault_tolerance",
    ],
    "~/ray/release/runtime_env_tests/runtime_env_tests.yaml": [
        "rte_many_tasks_actors",
        "wheel_urls",
    ],
}

WEEKLY_TESTS = {
    "~/ray/benchmarks/benchmark_tests.yaml": [
        "many_actors",
        "many_tasks",
        "many_pgs",
        "many_nodes",
    ],
    "~/ray/release/nightly_tests/nightly_tests.yaml": [
        "stress_test_many_tasks",
        "stress_test_dead_actors",
    ],
    "~/ray/release/horovod_tests/horovod_tests.yaml": [
        "horovod_test",
    ],
    "~/ray/release/long_running_distributed_tests"
    "/long_running_distributed.yaml": [
        "pytorch_pbt_failure",
    ],
    # Full long running tests (1 day runtime)
    "~/ray/release/long_running_tests/long_running_tests.yaml": [
        "actor_deaths",
        "apex",
        "impala",
        "many_actor_tasks",
        "many_drivers",
        "many_ppo",
        "many_tasks",
        "many_tasks_serialized_ids",
        "node_failures",
        "pbt",
        "serve",
        "serve_failure",
    ],
    "~/ray/release/tune_tests/scalability_tests/tune_tests.yaml": [
        "network_overhead",
        "long_running_large_checkpoints",
    ],
    "~/ray/release/rllib_tests/rllib_tests.yaml": [
        "learning_tests",
        "stress_tests",
    ],
}

MANUAL_TESTS = {
    "~/ray/release/long_running_tests/long_running_tests.yaml": [
        SmokeTest("serve"),
        SmokeTest("serve_failure"),
    ]
}

# This test suite holds "user" tests to test important user workflows
# in a particular environment.
# All workloads in this test suite should:
#   1. Be run in a distributed (multi-node) fashion
#   2. Use autoscaling/scale up (no wait_cluster.py)
#   3. Use GPUs if applicable
#   4. Have the `use_connect` flag set.
USER_TESTS = {}

SUITES = {
    "core-nightly": CORE_NIGHTLY_TESTS,
    "nightly": {
        **NIGHTLY_TESTS,
        **USER_TESTS
    },
    "weekly": WEEKLY_TESTS,
    "manual": MANUAL_TESTS,
}

DEFAULT_STEP_TEMPLATE = {
    "env": {
        "ANYSCALE_CLOUD_ID": "cld_4F7k8814aZzGG8TNUGPKnc",
        "ANYSCALE_PROJECT": "prj_2xR6uT6t7jJuu1aCwWMsle",
        "RELEASE_AWS_BUCKET": "ray-release-automation-results",
        "RELEASE_AWS_LOCATION": "dev",
        "RELEASE_AWS_DB_NAME": "ray_ci",
        "RELEASE_AWS_DB_TABLE": "release_test_result",
        "AWS_REGION": "us-west-2"
    },
    "agents": {
        "queue": "runner_queue_branch"
    },
    "plugins": [{
        "docker#v3.8.0": {
            "image": "rayproject/ray",
            "propagate-environment": True,
            "volumes": [
                "/tmp/ray_release_test_artifacts:"
                "/tmp/ray_release_test_artifacts"
            ],
        }
    }],
    "commands": [],
    "artifact_paths": ["/tmp/ray_release_test_artifacts/**/*"],
}


def ask_configuration():
    RAY_BRANCH = os.environ.get("RAY_BRANCH", "master")
    RAY_REPO = os.environ.get("RAY_REPO",
                              "https://github.com/ray-project/ray.git")
    RAY_VERSION = os.environ.get("RAY_VERSION", "")
    RAY_WHEELS = os.environ.get("RAY_WHEELS", "")

    RAY_TEST_BRANCH = os.environ.get("RAY_TEST_BRANCH", RAY_BRANCH)
    RAY_TEST_REPO = os.environ.get("RAY_TEST_REPO", RAY_REPO)

    RELEASE_TEST_SUITE = os.environ.get("RELEASE_TEST_SUITE", "nightly")
    FILTER_FILE = os.environ.get("FILTER_FILE", "")
    FILTER_TEST = os.environ.get("FILTER_TEST", "")

    input_ask_step = {
        "input": "Input required: Please specify tests to run",
        "fields": [
            {
                "text": ("RAY_REPO: Please specify the Ray repository used "
                         "to find the wheel."),
                "hint": ("Repository from which to fetch the latest "
                         "commits to find the Ray wheels. Usually you don't "
                         "need to change this."),
                "default": RAY_REPO,
                "key": "ray_repo"
            },
            {
                "text": ("RAY_BRANCH: Please specify the Ray branch used "
                         "to find the wheel."),
                "hint": "For releases, this will be e.g. `releases/1.x.0`",
                "default": RAY_BRANCH,
                "key": "ray_branch"
            },
            {
                "text": ("RAY_VERSION: Please specify the Ray version used "
                         "to find the wheel."),
                "hint": ("Leave empty for latest master. For releases, "
                         "specify the release version."),
                "required": False,
                "default": RAY_VERSION,
                "key": "ray_version"
            },
            {
                "text": "RAY_WHEELS: Please specify the Ray wheel URL.",
                "hint": ("ATTENTION: If you provide this, RAY_REPO, "
                         "RAY_BRANCH and RAY_VERSION will be ignored! "
                         "Please also make sure to provide the wheels URL "
                         "for Python 3.7 on Linux.\n"
                         "You can also insert a commit hash here instead "
                         "of a full URL.\n"
                         "NOTE: You can specify multiple commits or URLs "
                         "for easy bisection (one per line) - this will "
                         "run each test on each of the specified wheels."),
                "required": False,
                "default": RAY_WHEELS,
                "key": "ray_wheels"
            },
            {
                "text": ("RAY_TEST_REPO: Please specify the Ray repository "
                         "used to find the tests you would like to run."),
                "hint": ("If you're developing a new release test, this "
                         "will most likely be your GitHub fork."),
                "default": RAY_TEST_REPO,
                "key": "ray_test_repo"
            },
            {
                "text": ("RAY_TEST_BRANCH: Please specify the Ray branch used "
                         "to find the tests you would like to run."),
                "hint": ("If you're developing a new release test, this "
                         "will most likely be a branch living on your "
                         "GitHub fork."),
                "default": RAY_TEST_BRANCH,
                "key": "ray_test_branch"
            },
            {
                "select": ("RELEASE_TEST_SUITE: Please specify the release "
                           "test suite containing the tests you would like "
                           "to run."),
                "hint": ("Check in the `build_pipeline.py` if you're "
                         "unsure which suite contains your tests."),
                "required": True,
                "options": sorted(SUITES.keys()),
                "default": RELEASE_TEST_SUITE,
                "key": "release_test_suite"
            },
            {
                "text": ("FILTER_FILE: Please specify a filter for the "
                         "test files that should be included in this build."),
                "hint": ("Only test files (e.g. xgboost_tests.yml) that "
                         "match this string will be included in the test"),
                "default": FILTER_FILE,
                "required": False,
                "key": "filter_file"
            },
            {
                "text": ("FILTER_TEST: Please specify a filter for the "
                         "test names that should be included in this build."),
                "hint": ("Only test names (e.g. tune_4x32) that match "
                         "this string will be included in the test"),
                "default": FILTER_TEST,
                "required": False,
                "key": "filter_test"
            },
        ],
        "key": "input_ask_step",
    }

    run_again_step = {
        "commands": [
            f"export {v}=$(buildkite-agent meta-data get \"{k}\")"
            for k, v in {
                "ray_branch": "RAY_BRANCH",
                "ray_repo": "RAY_REPO",
                "ray_version": "RAY_VERSION",
                "ray_wheels": "RAY_WHEELS",
                "ray_test_branch": "RAY_TEST_BRANCH",
                "ray_test_repo": "RAY_TEST_REPO",
                "release_test_suite": "RELEASE_TEST_SUITE",
                "filter_file": "FILTER_FILE",
                "filter_test": "FILTER_TEST",
            }.items()
        ] + [
            "export AUTOMATIC=1",
            "python3 -m pip install --user pyyaml",
            "rm -rf ~/ray || true",
            "git clone -b $${RAY_TEST_BRANCH} $${RAY_TEST_REPO} ~/ray",
            ("python3 ~/ray/release/.buildkite/build_pipeline.py "
             "| buildkite-agent pipeline upload"),
        ],
        "label": ":pipeline: Again",
        "agents": {
            "queue": "runner_queue_branch"
        },
        "depends_on": "input_ask_step",
        "key": "run_again_step",
    }

    return [
        input_ask_step,
        run_again_step,
    ]


def create_test_step(
        ray_repo: str,
        ray_branch: str,
        ray_version: str,
        ray_wheels: str,
        ray_test_repo: str,
        ray_test_branch: str,
        test_file: str,
        test_name: ReleaseTest,
):
    ray_wheels_str = f" ({ray_wheels}) " if ray_wheels else ""

    logging.info(f"Creating step for {test_file}/{test_name}{ray_wheels_str}")
    cmd = str(f"RAY_REPO=\"{ray_repo}\" "
              f"RAY_BRANCH=\"{ray_branch}\" "
              f"RAY_VERSION=\"{ray_version}\" "
              f"RAY_WHEELS=\"{ray_wheels}\" "
              f"RELEASE_RESULTS_DIR=/tmp/artifacts "
              f"python release/e2e.py "
              f"--category {ray_branch} "
              f"--test-config {test_file} "
              f"--test-name {test_name} "
              f"--keep-results-dir")

    if test_name.smoke_test:
        logging.info("This test will run as a smoke test.")
        cmd += " --smoke-test"

    step_conf = copy.deepcopy(DEFAULT_STEP_TEMPLATE)

    if test_name.retry:
        logging.info(f"This test will be retried up to "
                     f"{test_name.retry} times.")
        step_conf["retry"] = {
            "automatic": [{
                "exit_status": "*",
                "limit": test_name.retry
            }]
        }

    step_conf["commands"] = [
        "pip install -q -r release/requirements.txt",
        "pip install -U boto3 botocore",
        f"git clone -b {ray_test_branch} {ray_test_repo} ~/ray", cmd,
        "sudo cp -rf /tmp/artifacts/* /tmp/ray_release_test_artifacts "
        "|| true"
    ]

    if isinstance(test_name, ConnectTest):
        # Add driver side setup commands to the step.
        pip_requirements_command = [f"pip install -U -r "
                                    f"{test_name.requirements_file}"] if \
            test_name.requirements_file else []
        step_conf["commands"] = test_name.setup_commands \
            + pip_requirements_command \
            + step_conf["commands"]

    step_conf["label"] = f"{ray_wheels_str}{test_name} ({ray_branch}) - " \
                         f"{ray_test_branch}/{ray_test_repo}"
    return step_conf


def build_pipeline(steps):
    all_steps = []

    RAY_BRANCH = os.environ.get("RAY_BRANCH", "master")
    RAY_REPO = os.environ.get("RAY_REPO",
                              "https://github.com/ray-project/ray.git")
    RAY_VERSION = os.environ.get("RAY_VERSION", "")
    RAY_WHEELS = os.environ.get("RAY_WHEELS", "")

    RAY_TEST_BRANCH = os.environ.get("RAY_TEST_BRANCH", RAY_BRANCH)
    RAY_TEST_REPO = os.environ.get("RAY_TEST_REPO", RAY_REPO)

    FILTER_FILE = os.environ.get("FILTER_FILE", "")
    FILTER_TEST = os.environ.get("FILTER_TEST", "")

    ray_wheels_list = [""]
    if RAY_WHEELS:
        ray_wheels_list = RAY_WHEELS.split("\n")

    if len(ray_wheels_list) > 1:
        logging.info(f"This will run a bisec on the following URLs/commits: "
                     f"{ray_wheels_list}")

    logging.info(
        f"Building pipeline \n"
        f"Ray repo/branch to test:\n"
        f" RAY_REPO   = {RAY_REPO}\n"
        f" RAY_BRANCH = {RAY_BRANCH}\n\n"
        f" RAY_VERSION = {RAY_VERSION}\n\n"
        f" RAY_WHEELS = {RAY_WHEELS}\n\n"
        f"Ray repo/branch containing the test configurations and scripts:"
        f" RAY_TEST_REPO   = {RAY_TEST_REPO}\n"
        f" RAY_TEST_BRANCH = {RAY_TEST_BRANCH}\n\n"
        f"Filtering for these tests:\n"
        f" FILTER_FILE = {FILTER_FILE}\n"
        f" FILTER_TEST = {FILTER_TEST}\n\n")

    for test_file, test_names in steps.items():
        if FILTER_FILE and FILTER_FILE not in test_file:
            continue

        test_base = os.path.basename(test_file)
        for test_name in test_names:
            if FILTER_TEST and FILTER_TEST not in test_name:
                continue

            if not isinstance(test_name, ReleaseTest):
                test_name = ReleaseTest(name=test_name)

            logging.info(f"Adding test: {test_base}/{test_name}")

            for ray_wheels in ray_wheels_list:
                step_conf = create_test_step(
                    ray_repo=RAY_REPO,
                    ray_branch=RAY_BRANCH,
                    ray_version=RAY_VERSION,
                    ray_wheels=ray_wheels,
                    ray_test_repo=RAY_TEST_REPO,
                    ray_test_branch=RAY_TEST_BRANCH,
                    test_file=test_file,
                    test_name=test_name)

                all_steps.append(step_conf)

    return all_steps


def alert_pipeline(stats: bool = False):
    step_conf = copy.deepcopy(DEFAULT_STEP_TEMPLATE)

    cmd = "python release/alert.py"
    if stats:
        cmd += " --stats"

    step_conf["commands"] = [
        "pip install -q -r release/requirements.txt",
        "pip install -U boto3 botocore",
        cmd,
    ]
    step_conf["label"] = f"Send periodic alert (stats_only = {stats})"
    return [step_conf]


if __name__ == "__main__":
    alert = os.environ.get("RELEASE_ALERT", "0")

    ask_for_config = not bool(int(os.environ.get("AUTOMATIC", "0")))

    if alert in ["1", "stats"]:
        steps = alert_pipeline(alert == "stats")
    elif ask_for_config:
        steps = ask_configuration()
    else:
        TEST_SUITE = os.environ.get("RELEASE_TEST_SUITE", "nightly")
        PIPELINE_SPEC = SUITES[TEST_SUITE]

        steps = build_pipeline(PIPELINE_SPEC)

    yaml.dump({"steps": steps}, sys.stdout)
