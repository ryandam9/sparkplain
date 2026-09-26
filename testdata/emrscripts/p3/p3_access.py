# sparkplain phase 3 test: the driver asks STS to assume a role it may not,
# so CloudTrail records an AccessDenied (S3 object calls would not reach
# LookupEvents). The job does some work first, then fails on the refusal.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("p3_access").getOrCreate()
sc = spark.sparkContext
sc.setJobDescription("access: warm-up")
spark.range(0, 10_000_000, numPartitions=8).selectExpr("sum(id)").collect()
jvm = sc._jvm
sts = jvm.com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder.standard().withRegion("ap-southeast-2").build()
req = jvm.com.amazonaws.services.securitytoken.model.AssumeRoleRequest() \
    .withRoleArn("arn:aws:iam::000000000000:role/fixture-no-such-role").withRoleSessionName("sparkplain-test")
sts.assumeRole(req)  # AccessDenied
spark.stop()
