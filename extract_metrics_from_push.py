import gzip
import urllib2
import os
import sys
import traceback

from argparse import ArgumentParser
from cStringIO import StringIO
from datetime import timedelta

from thclient import TreeherderClient


def main():
    options = parse_args()
    revision = options.revision
    repo_name = options.repo_name

    revision_jobs_map = get_all_jobs(repo_name=repo_name, revision=revision)
    timings, unmatched = process_jobs(
        repo_name=repo_name,
        revision=revision,
        th_jobs=revision_jobs_map[revision],
        beg_string=options.beg_string,
        end_string=options.end_string,
        num_of_jobs=options.num_jobs
    )

    effective_time = 0
    effective_total = 0
    for t in timings:
        effective_time += t[1]
        effective_total += t[2]
        print t

    # Let's print the global timing
    print "Number of jobs - initially: {}".format(len(revision_jobs_map[revision]))
    print "Number of jobs - timed: {}".format(len(timings))
    print "Number of jobs - unmatched: {}".format(len(unmatched))
    print "Number of jobs - no artifacts: {}".format(
        options.num_jobs - len(timings) - len(unmatched)
    )
    print (str(round(float(effective_time)/effective_total, 2) * 100) + " %",
           effective_time,
           effective_total)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-r", "--revision", dest="revision", required=True)
    parser.add_argument("--repo", "--repo-name", dest="repo_name", required=True)
    parser.add_argument("--beg-string", dest="beg_string", required=True)
    parser.add_argument("--end-string", dest="end_string", required=True)
    parser.add_argument("--num-jobs", dest="num_jobs", type=int, default=5)
    return parser.parse_args()


def get_all_jobs(repo_name, revision):
    '''Return dictionary of all jobs for a given revision

    Return: {'<revision_hash>': {'<job_id_1>': <job_id_1_metadata>}}
    '''
    print "Fetching Treeherder jobs for {}/{}".format(repo_name, revision)
    th_client = TreeherderClient()
    results = th_client.get_resultsets(repo_name, revision=revision)
    all_jobs = {}
    if results:
        revision_id = results[0]["id"]
        for job in th_client.get_jobs(repo_name, count=6000, result_set_id=revision_id):
            # Grab job metadata
            all_jobs[job['id']] = job

    return {revision: all_jobs}


def _time_difference(beginning, end):
    '''We expect two strings with the format 09:08:07'''
    h, m, s = beginning.split(':')
    beginning = timedelta(hours=int(h), minutes=int(m), seconds=int(s))
    h, m, s = end.split(':')
    end = timedelta(hours=int(h), minutes=int(m), seconds=int(s))
    return end - beginning


def measure(lines, beg_match_str, end_match_str):
    '''Given a Mozharness log we return a time delta between two lines we're matching

    We assume that the timings are on the first column of the line
    '''
    beg_time = None
    end_time = None

    # e.g. 09:08:44 INFO - Running...
    for line in lines:
        if beg_match_str in line:
            beg_time = line.split(' ')[0]
        if end_match_str in line:
            end_time = line.split(' ')[0]

    if beg_time and end_time:
        return int(_time_difference(beg_time, end_time).total_seconds())
    else:
        return None


def get_job_log(repo_name, job_id):
    '''For a given job id return the URL to the log associated to it.'''
    th_client = TreeherderClient()
    query_params = {'job_id': job_id, 'name': 'text_log_summary'}
    try:
        return str(th_client.get_artifacts(repo_name, **query_params)[0]['blob']['logurl'])
    except IndexError:
        print 'No artifacts for {}'.format(job_id)
    except requests.exceptions.ConnectionError as e:
        print 'Connection failed for {}'.format(job_id)
        traceback.print_exc()


def download_and_uncompress(url):
    request = urllib2.Request(url)
    request.add_header('Accept-encoding', 'gzip')
    response = urllib2.urlopen(request)
    if response.info().get('Content-Encoding') == 'gzip':
        buf = StringIO(response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
        # Let's keep the new lines
        lines = data.splitlines(True)
        # print lines[0][0:200]
        # import pdb; pdb.set_trace()
        return lines
    else:
        raise Exception("The encoding should have been 'gzip' but it is instead {}".format(
            response.info().get('Content-Encoding')
        ))


def process_jobs(repo_name, revision, th_jobs, beg_string, end_string, num_of_jobs=5):
    '''For every job for a revision download the logs'''
    CACHE = os.path.join('cache', revision)
    PATHS = ('cache', CACHE)
    timed = []
    untimed = []

    print 'Process logs for {}'.format(revision)

    # We want to put logs for a revision under a directory under 'cache'
    for path in PATHS:
        if not os.path.exists(path):
            os.mkdir(path)

    i = 0
    for job_id, job in th_jobs.iteritems():
        i = i + 1
        if i > num_of_jobs:
            # We've process enough jobs
            break

        log_url = get_job_log(repo_name=repo_name, job_id=job_id)

        if not log_url:
            continue

        # Assuming that job signature is unique
        filename = str(job_id) + '.log'
        log_path = os.path.join(CACHE, filename)

        if not os.path.exists(log_path):
            print "Downloading {}({})".format(job['job_type_name'], job_id)
            decompressed_file = download_and_uncompress(log_url)
            assert type(decompressed_file) == list
            # Save log uncompressed to cache
            with open(log_path, 'w') as outfile:
                outfile.writelines(decompressed_file)
        else:
            print "Loading {}".format(log_path)
            decompressed_file = open(log_path).readlines()

        try:
            timing = measure(
                lines=decompressed_file,
                beg_match_str=beg_string,
                end_match_str=end_string,
            )
        except Exception as e:
            print "Failing job {}".format(job_id)
            traceback.print_exc()
            break

        if timing is None:
            untimed.append((job['id'], job['job_type_name']))
            print "Nothing to measure for {}:{}".format(job_id, job['job_type_name'])
        elif job['result'] in ('retry'):
            # Automatically retried jobs don't have good values to use
            continue
        else:
            job_total_time = job['end_timestamp'] - job['start_timestamp']
            timed.append((
                str(round(float(timing)/job_total_time, 2) * 100) + " %",
                timing,
                job_total_time,
                job['job_type_name'],
                job_id
            ))

    return timed, untimed


if __name__ == "__main__":
    main()
