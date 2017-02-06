import json
import sys
import time
import traceback

import boto3
from dateutil.parser import parse


def all_execution_history(arn):
    events = []
    client = boto3.client('stepfunctions')
    paginator_es = client.get_paginator('get_execution_history')
    pes = paginator_es.paginate(
        executionArn=arn,
        reverseOrder=False,
        PaginationConfig={
            'MaxItems': 123456,
            'PageSize': 1000,
        }
    )
    for pe in pes:
        es = pe["events"]
        print("Events: %s" % len(es))
        events.extend(es)
    return events


def all_executions(state_machine_arn):
    client = boto3.client('stepfunctions')
    executions = []
    paginator_es = client.get_paginator('list_executions')
    pes = paginator_es.paginate(
        stateMachineArn=state_machine_arn,
        # statusFilter='RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED',
        PaginationConfig={
            'MaxItems': 123456,
            'PageSize': 1000,
        }
    )
    for pe in pes:
        es = pe["executions"]
        print("executions: %s" % len(es))
        executions.extend(es)
    return executions


time_fields = ("timestamp", "startDate", "stopDate")


def dump_output(events, file_name):
    for e in events:
        for tf in time_fields:
            if tf in e:
                e[tf] = e[tf].isoformat()
    with open(file_name, 'w') as f:
        json.dump(events, f, indent=4)


def unpack_ouput(file_name):
    with open(file_name, 'r') as f:
        content = json.load(f)
        for e in content:
            for tf in time_fields:
                if tf in e:
                    e[tf] = parse(e[tf])
        return content


def test():
    events = all_execution_history(
        'arn of execution')
    dump_output(events, "events-ex.txt")
    print(len(events))


def run_once(arn_state_machine, fname):
    all_exs = all_executions(arn_state_machine)
    dump_output(all_exs, fname)
    print(len(all_exs))
    # for execution in all_exs:
    #     print(execution['executionArn'])
    #     print(execution['startDate'])
    #     print(execution['stopDate'])


def run(fname):
    all_exs = unpack_ouput(fname)
    print(len(all_exs))

    count = 0
    for ex in all_exs:
        if "" not in ex['name']:
            continue
        print(ex)
        mult = 1
        while True:
            try:
                events = all_execution_history(ex['executionArn'])
                print(len(events))
                dump_output(events, ex['name'] + '.txt')
                break
            except:
                print("Failed attempt")
                traceback.print_exc(file=sys.stdout)
                time.sleep(5 * mult)
                mult *= 2
        count += 1

    print("Dumped: %s", count)


if __name__ == '__main__':
    # run_once('',
    # 'executions.txt')
    run('executions.txt')
