import boto3

client = boto3.client('logs')


def find_log_groups(prefix):
    paginator = client.get_paginator('describe_log_groups')
    page_iterator = paginator.paginate(logGroupNamePrefix=prefix,
                                       PaginationConfig={'PageSize': 50})
    log_groups = []
    for page in page_iterator:
        log_groups.extend(page['logGroups'])

    return log_groups


groups = find_log_groups("/")
print(len(groups))

for group in groups:
    name = group["logGroupName"]
    if "mystack" in name:
        print("Delete: %s" % name)
        client.delete_log_group(logGroupName=name)
