def main():
    # wrapped in a function so an importing tool won't auto execute it
    try:
        import subprocess

        subprocess.check_call(
            ["pip", "install", "boto3", "daggerml==0.0.23", "daggerml-cli==0.0.13"]
        )
    except Exception as e:
        print("ruh roh! Got error:", e)
        raise
    import os
    from urllib.parse import urlparse

    import boto3
    from daggerml import Dml

    INPUT_LOC = os.environ["DML_INPUT"]
    OUTPUT_LOC = os.environ["DML_OUTPUT"]

    def handler(dump):
        p = urlparse(OUTPUT_LOC)
        (
            boto3.client("s3").put_object(
                Bucket=p.netloc, Key=p.path[1:], Body=dump.encode()
            )
        )

    p = urlparse(INPUT_LOC)
    data = (
        boto3.client("s3")
        .get_object(Bucket=p.netloc, Key=p.path[1:])["Body"]
        .read()
        .decode()
    )
    with Dml(data=data, message_handler=handler) as dml:
        with dml.new("test", "test") as d0:
            d0.n0 = sum(d0.argv[1:].value())
            d0.result = d0.n0


if __name__ == "__main__":
    main()
