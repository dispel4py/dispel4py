from flask import Flask, \
    render_template, send_from_directory, abort, redirect, url_for
app = Flask(__name__)

import json
import os
import traceback
ROOT_DIR = os.path.expanduser('~') + '/.dispel4py/monitoring'


@app.route('/monitoring/<job>/graph')
def show_graph(job):
    try:
        job_dir = os.path.join(ROOT_DIR, job)
        return send_from_directory(job_dir, 'graph.png', mimetype='image/png')
    except:
        abort(404)


@app.route('/monitoring/<job>/summary')
def show_status(job):
    try:
        job_dir = os.path.join(ROOT_DIR, job)
        with open(os.path.join(job_dir, 'stack'), 'r') as f:
            job_status = json.load(f)
        # print(job_status)
        return render_template("job_status.html", job=job_status)
    except:
        print(traceback.format_exc())
        raise


@app.route('/monitoring/<job>/timeline')
def serve_timeline(job):
    try:
        info_path = os.path.join(ROOT_DIR, job + '/info')
        with open(info_path, 'r') as f:
            info = json.load(f)
        timeline_path = os.path.join(
            ROOT_DIR, job + '/timestamps')
        with open(timeline_path, 'r') as f:
            from collections import deque
            result = []
            for line in deque(f, maxlen=100):
                try:
                    result.append(json.loads(line))
                except ValueError:
                    # some JSON error
                    pass
        #     return f.read()
        return render_template(
            "job_timeline.html",
            info=json.dumps(info),
            timeline=json.dumps(result))
    except:
        print(traceback.format_exc())
        raise


@app.route('/monitoring/<job>/info')
def show_job_info(job):
    job_dir = os.path.join(ROOT_DIR, job)
    if os.path.isdir(job_dir):
        with open(os.path.join(job_dir, 'info'), 'r') as f:
            job_info = json.load(f)
        processes = []
        for procs in job_info['processes'].values():
            processes.extend(procs)
        job_info['num_processes'] = len(processes)
        if os.path.isfile(os.path.join(job_dir, 'stack')):
            job_info['has_summary'] = True
        if os.path.isfile(os.path.join(job_dir, 'timestamps')):
            job_info['has_timeline'] = True
        return render_template("job_info.html", job=job_info)


@app.route('/monitoring/<job>/delete', methods=['POST'])
def delete_stats(job):
    import shutil
    print('Deleting %s' % job)
    job_dir = os.path.join(ROOT_DIR, job)
    shutil.rmtree(job_dir, True)
    return redirect(url_for('list_jobs'), code=302)


@app.route('/monitoring')
def list_jobs():
    jobs = []
    try:
        print(os.listdir(ROOT_DIR))
        for fn in os.listdir(ROOT_DIR):
            filename = os.path.join(ROOT_DIR, fn)
            if os.path.isdir(filename):
                with open(os.path.join(filename, 'info'), 'r') as f:
                    job_status = json.load(f)
                    job = {'name': job_status['name'],
                           'start_time': job_status['start_time']}
                    if 'end_time' in job_status:
                        job['end_time'] = job_status['end_time']
                jobs.append(job)
    except:
        print(traceback.format_exc())
        # the directory might not exist yet but that's ok
        pass

    return render_template("index.html", job_list=jobs, link='monitoring')


# *********** MongoDB *************

'''
Store monitoring data in MongoDB.
'''

MONGODB_URL = 'mongodb://localhost:27017/'
MONGODB_DB = 'dispel4py_monitor'

from pymongo import MongoClient
client = MongoClient(MONGODB_URL)

from collections import defaultdict


@app.route('/db')
def list_jobs_db():
    collection = client[MONGODB_DB]['job_info']
    jobs = list(collection.find())
    return render_template("index.html", job_list=jobs, link='db')


def lookup_job(job):
    collection = client[MONGODB_DB]['job_info']
    job_info = collection.find_one({'name': job})
    processes = []
    for procs in job_info['processes'].values():
        processes.extend(procs)
    job_info['num_processes'] = len(processes)
    return job_info


@app.route('/db/<job>/info')
def get_job_info(job):
    job_info = lookup_job(job)
    job_info['has_summary'] = True
    job_info['has_timeline'] = True
    return render_template("job_info.html", job=job_info)


@app.route('/db/<job>/graph')
def show_graph_db(job):
    try:
        job_dir = os.path.join(ROOT_DIR, job)
        return send_from_directory(job_dir, 'graph.png', mimetype='image/png')
    except:
        abort(404)


from bson.son import SON


@app.route('/db/<job>/summary')
def show_status_db(job):
    collection = client[MONGODB_DB]['raw']
    agg = [
        {"$match": {"job": job}},
        {"$group": {"_id": {"pe": "$pe",
                            "name": "$name",
                            "process": "$process",
                            "output": "$data.output"},
                    "time": {"$sum": "$time"},
                    "avg_time": {"$avg": "$time"},
                    "count": {"$sum": 1},
                    "size": {"$sum": "$data.size"},
                    "avg_size": {"$avg": "$data.size"}}}
    ]
    info = {
        'summary': defaultdict(lambda: defaultdict(float)),
        'detail': defaultdict(lambda: defaultdict(dict)),
    }
    detail = info['detail']
    total_time = 0.0
    total_count = 0
    total_size = 0
    total_errors = 0
    for record in collection.aggregate(agg):
        pe_id = record['_id']['pe']
        process = record['_id']['process']
        method = record['_id']['name']
        if method == 'write':
            if method not in detail[pe_id][process]:
                detail[pe_id][process][method] = {}
            detail[pe_id][process][method][record['_id']['output']] = {
                'avg': record['avg_time'],
                'time': record['time'],
                'count': record['count'],
                'size': record['size'],
                'avg_size': record['avg_size']}
        else:
            detail[pe_id][process][method] = {
                'avg': record['avg_time'],
                'time': record['time'],
                'count': record['count']
            }
        if method == 'process':
            total_time += record['time']
            total_count += record['count']
            info['summary'][pe_id]['time'] += record['time']
            info['summary'][pe_id]['count'] += record['count']
            total_size += record['size']

    # collect data on errors
    errors_agg = [
        {"$match": {"job": job, "data.error": {"$exists": "true"}}},
        {"$sort": SON([("_id", 1)])},
        {"$group": {"_id": {"pe": "$pe",
                            "name": "$name",
                            "process": "$process"},
                    "count": {"$sum": 1},
                    "last": {"$last": "$data.error"}}}
    ]
    for record in collection.aggregate(errors_agg):
        pe_id = record['_id']['pe']
        process = record['_id']['process']
        method = record['_id']['name']
        detail[pe_id][process][method]['error_count'] = record['count']
        total_errors += record['count']
        info['summary'][pe_id]['error_count'] += record['count']
        info['summary'][pe_id]['last_error'] = record['last']

    info['total'] = {'count': total_count,
                     'time': total_time,
                     'size': total_size,
                     'error_count': total_errors}

    info['info'] = lookup_job(job)
    # print(info['detail'])
    return render_template('job_summary.html', job=info)
    # return json.dumps(info)


if __name__ == "__main__":
    app.run(debug=True)
