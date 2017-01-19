import boto3
import json


def get_lambda_storage_and_clean():
    result = []
    lambda_client = boto3.client('lambda')
    paginator_fs = lambda_client.get_paginator('list_functions')
    pfs = paginator_fs.paginate(
        PaginationConfig={
            'MaxItems': 1000,
            'PageSize': 100,
        }
    )
    # fs = lambda_client.list_functions(MaxItems=1000)["Functions"]
    total_code_size = 0
    for pf in pfs:
        fs = pf["Functions"]
        print("Functions: %s" % len(fs))
        for f in fs:
            function_code_size = 0
            fn = f["FunctionName"]
            NextMarker = None
            vlen = 0
            while True:
                args = dict(
                    FunctionName=fn,
                    MaxItems=1000,
                )
                if NextMarker:
                    args["Marker"] = NextMarker
                pv = lambda_client.list_versions_by_function(**args)
                NextMarker = pv.get("NextMarker")
                vs = pv["Versions"]
                for v in vs:
                    vn = v["Version"]
                    code_size = v["CodeSize"]
                    function_code_size += code_size
                    if vn != "$LATEST" and "rmaz" in fn:
                        lambda_client.delete_function(
                            FunctionName=fn,
                            Qualifier=vn
                        )
                vlen += len(vs)
                if "NextMarker" not in pv:
                    break
            total_code_size += function_code_size
            result.append(dict(FunctionName=fn, CodeSize=function_code_size, Versions=vlen))
            print("versions: %s\tsize: %s MB\tname: %s" % (vlen, function_code_size / (1024 * 1024), fn))

    print("Total size: %s" % (total_code_size / (1024 * 1024)))
    return result

storage_before_cleaning = get_lambda_storage_and_clean()
print(json.dumps(storage_before_cleaning))
