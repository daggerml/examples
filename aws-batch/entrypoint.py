import json
import logging
import os
from dataclasses import dataclass, field
from textwrap import dedent
from time import time
from uuid import uuid4

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
TIMEOUT = 5  # seconds
MAX_ITERS = 10
S3_BUCKET = os.environ["JOB_BUCKET"]
S3_PREFIX = os.environ.get("JOB_PREFIX")
DFLT_PROP = {"vcpus": 1, "memory": 512}
DELETE_DYNAMO_ON_FAIL = os.getenv("DELETE_DYNAMO_ON_FAIL")
PENDING_STATES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
SUCCESS_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"


def get_client(name):
    logger.info("getting %r client", name)
    config = Config(connect_timeout=5, retries={"max_attempts": 0})
    return boto3.client(name, config=config)


def s3_get(key):
    s3_client = get_client("s3")
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return
        raise
    resp = get_client("s3").get_object(Bucket=S3_BUCKET, Key=key)
    return resp["Body"].read().decode()


def s3_put(data, key):
    client = get_client("s3")
    logger.info("putting data to s3://%s/%s", S3_BUCKET, key)
    client.put_object(Bucket=S3_BUCKET, Key=key, Body=data.encode())
    logger.info("returning")
    return f"s3://{S3_BUCKET}/{key}"


def now():
    return int(time())


@dataclass
class Dynamo:
    cache_key: str
    run_id: str = field(default_factory=lambda: uuid4().hex)
    db: "boto3.client" = field(default_factory=lambda: get_client("dynamodb"))
    tb = os.environ["DYNAMODB_TABLE"]

    def _update(self, key=None, **kw):
        try:
            self.db.update_item(
                TableName=self.tb,
                Key={"cache_key": {"S": key or self.cache_key}},
                **kw,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info("could not update %r (invalid lock)", self.cache_key)
                return False
            raise

    def put(self, obj):
        logger.info("putting data for %r", self.cache_key)
        ut = now()
        return self._update(
            UpdateExpression="SET #lk = :lk, #ut = :ut, #obj = :obj",
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
                "#obj": "obj",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":ut": {"N": str(ut)},
                ":to": {"N": str(ut - TIMEOUT)},
                ":obj": {"S": json.dumps(obj)},
            },
        )

    def get(self):
        logger.info("getting data for %r", self.cache_key)
        resp = self.db.get_item(
            TableName=self.tb, Key={"cache_key": {"S": self.cache_key}}
        )
        if "obj" not in resp["Item"]:
            return
        return json.loads(resp["Item"]["obj"]["S"])

    def lock(self, key=None):
        logger.info("acquiring lock for %r", self.cache_key)
        ut = now()
        return self._update(
            key,
            UpdateExpression="SET #lk = :lk, #ut = :ut",
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":ut": {"N": str(ut)},
                ":to": {"N": str(ut - TIMEOUT)},
            },
        )

    def unlock(self, key=None):
        logger.info("releasing lock for %r", self.cache_key)
        return self._update(
            key,
            UpdateExpression="REMOVE #lk SET #ut = :ut",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={"#lk": "lock_key", "#ut": "update_time"},
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":ut": {"N": str(now())},
            },
        )

    def delete(self):
        ut = now()
        return self.db.delete_item(
            TableName=self.tb,
            Key={"cache_key": {"S": self.cache_key}},
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":to": {"N": str(ut - TIMEOUT)},
            },
        )


@dataclass
class Batch:
    cache_key: str
    job_id: str | None = None
    job_def: str | None = None
    status: str | None = None
    iters: int = 0
    client: "boto3.client" = field(default_factory=lambda: get_client("batch"))

    @property
    def prefix(self):
        if S3_PREFIX is None:
            return self.cache_key
        return f"{S3_PREFIX}/{self.cache_key}"

    @property
    def result_key(self):
        return f"{self.prefix}/result.dump"

    def _create_job_def(self, **kw):
        response = self.client.register_job_definition(**kw)
        logger.info(
            "Job definition created: %r with arn: %r",
            response["jobDefinitionName"],
            response["jobDefinitionArn"],
        )
        job_def = response["jobDefinitionArn"]
        logger.info("created job definition with arn: %r", job_def)
        self.job_def = job_def

    def submit(self, dump, script, image, requirements):
        dump_ps = s3_put(dump, f"{self.prefix}/input.data")
        container_props = {**DFLT_PROP, **requirements}
        needs_gpu = any(
            x["type"] == "GPU" for x in container_props.get("resourceRequirements", [])
        )
        logger.info("createing job definition with name: %r", f"fn-{self.cache_key}")
        self._create_job_def(
            jobDefinitionName=f"fn-{self.cache_key}",
            type="container",
            containerProperties={
                "image": image,
                "command": [
                    "python3",
                    "-c",
                    dedent(script).strip(),
                ],
                "environment": [
                    {"name": "DML_INPUT", "value": dump_ps},
                    {
                        "name": "DML_OUTPUT",
                        "value": f"s3://{S3_BUCKET}/{self.result_key}",
                    },
                ],
                "jobRoleArn": os.environ["BATCH_TASK_ROLE_ARN"],
                **container_props,
            },
        )
        response = self.client.submit_job(
            jobName=f"fn-{self.cache_key}",
            jobQueue=os.environ["GPU_QUEUE" if needs_gpu else "CPU_QUEUE"],
            jobDefinition=self.job_def,
        )
        logger.info("Job submitted: %r", response["jobId"])
        self.job_id = response["jobId"]
        self.status = "submitted"

    def update(self):
        response = self.client.describe_jobs(jobs=[self.job_id])
        job = response["jobs"][0]
        self.status = job["status"]
        if self.status == SUCCESS_STATE:
            return f"Job {self.job_id} succeeded."
        if self.status == FAILED_STATE:
            failure_reason = (
                job.get("attempts", [{}])[-1]
                .get("container", {})
                .get("reason", "Unknown failure reason")
            )
            status_reason = job.get(
                "statusReason", "No additional status reason provided."
            )
            msg = json.dumps(
                {
                    "job_id": self.job_id,
                    "cache_key": self.cache_key,
                    "failure_reason": failure_reason,
                    "status_reason": status_reason,
                },
                separators=(",", ":"),
            )
            return msg
        return f"Job {self.job_id} is processing with status: {self.status}"

    def gc(self):
        try:
            self.client.deregister_job_definition(jobDefinition=self.job_def)
            logger.info("Successfully deregistered: %r", self.job_def)
            return
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ClientException":
                raise
            if "DEREGISTERED" not in e.response.get("Error", {}).get("Message"):
                raise

    def state_dict(self):
        return {
            "job_id": self.job_id,
            "job_def": self.job_def,
            "status": self.status,
        }


def success_pipeline(dynamo, batch):
    if batch.iters == 0:
        batch.gc()
    dump = s3_get(batch.result_key)
    if dump is None:
        if batch.iters > MAX_ITERS:
            return {
                "status": 422,
                "message": "task failed to write output",
                "dump": None,
            }
        batch.iters += 1
        dynamo.put(batch.state_dict())
        return {
            "status": 204,
            "message": "waiting for task to write output",
            "dump": None,
        }
    return {
        "status": 200,
        "message": "Job finished",
        "dump": dump,
    }


def handler(event, context):
    dynamo = Dynamo(event["cache_key"])
    if not dynamo.lock():
        return {"status": 204, "message": "Could not acquire job lock"}
    try:
        logger.info("getting dynamo info")
        info = dynamo.get() or {}
        logger.info("initializing batch client")
        batch = Batch(event["cache_key"], **info)
        if batch.status is None:
            logger.info("submitting job now")
            kw = {k: v[-1] for k, v in event["kwargs"].items()}
            if "requirements" not in kw:
                kw["requirements"] = {}
            logger.info("submitting job with kwargs: %s", json.dumps(kw))
            batch.submit(dump=event["dump"], **kw)
            logger.info("putting batch state")
            dynamo.put(batch.state_dict())
            logger.info("returning")
            return {
                "status": 201,
                "message": "job created and submitted",
                "dump": None,
            }
        if batch.status == SUCCESS_STATE:
            return success_pipeline(dynamo, batch)
        msg = batch.update()
        dynamo.put(batch.state_dict())
        if batch.status in PENDING_STATES:
            dynamo.put(batch.state_dict())
            return {"status": 202, "message": msg, "dump": None}
        if batch.status == FAILED_STATE:
            if DELETE_DYNAMO_ON_FAIL:
                dynamo.delete()
            return {"status": 400, "message": msg, "dump": None}
        return success_pipeline(dynamo, batch)
    except Exception as e:
        return {"status": 400, "message": str(e), "dump": None}
    finally:
        dynamo.unlock()
